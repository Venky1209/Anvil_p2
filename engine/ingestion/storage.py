"""
Storage Layer — Single Source of Truth for the PCE.

Uses SQLite (stdlib) as the primary backend. API is designed to be
a drop-in replacement for DuckDB when available.

ALL modules read/write through this file. Nobody defines tables elsewhere.
Tables:
  - service_identities  : canonical ID → aliases, temporal name ranges
  - raw_events          : every ingested event, indexed by canonical_id + kind + ts
  - causal_edges        : detected causal relationships between events
  - incident_episodes   : resolved incident records with fingerprint vectors
  - incident_families   : clustered incident patterns
  - metric_baselines    : rolling baselines per service+metric for anomaly detection
"""

import sqlite3
import json
import os
import uuid
from typing import Optional


class DuckDBStorage:
    """
    SQLite-backed storage for the entire PCE.

    Named DuckDBStorage for API compatibility — swap the backend by
    changing the __init__ to use duckdb.connect() when DuckDB is available.
    The SQL is ANSI-compatible and works with both.
    """

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
        self._create_tables()

    def _create_tables(self) -> None:
        """Create all PCE tables. Idempotent."""
        cur = self.conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS service_identities (
                canonical_id    TEXT PRIMARY KEY,
                current_name    TEXT NOT NULL,
                aliases         TEXT DEFAULT '[]',
                name_ranges     TEXT DEFAULT '[]',
                behavioral_role TEXT,
                first_seen_ts   TEXT NOT NULL,
                last_seen_ts    TEXT NOT NULL
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_events (
                event_id        TEXT PRIMARY KEY,
                canonical_id    TEXT,
                kind            TEXT NOT NULL,
                ts              TEXT NOT NULL,
                raw_service     TEXT DEFAULT '',
                metric_name     TEXT,
                metric_value    REAL,
                metric_source   TEXT,
                trace_id        TEXT,
                incident_id     TEXT,
                raw_json        TEXT NOT NULL
            )
        """)

        # Indexes for fast lookups
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_canonical_ts
            ON raw_events (canonical_id, ts)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_kind_ts
            ON raw_events (kind, ts)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_trace
            ON raw_events (trace_id)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_incident
            ON raw_events (incident_id)
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS causal_edges (
                edge_id             TEXT PRIMARY KEY,
                cause_event_id      TEXT NOT NULL,
                effect_event_id     TEXT NOT NULL,
                cause_canonical_id  TEXT,
                effect_canonical_id TEXT,
                edge_type           TEXT NOT NULL,
                confidence          REAL DEFAULT 0.5,
                evidence            TEXT DEFAULT '{}',
                reinforcement_count INTEGER DEFAULT 0,
                created_at          TEXT NOT NULL,
                last_reinforced_at  TEXT NOT NULL
            )
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_causal_cause
            ON causal_edges (cause_canonical_id)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_causal_effect
            ON causal_edges (effect_canonical_id)
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS incident_episodes (
                incident_id         TEXT PRIMARY KEY,
                canonical_ids       TEXT DEFAULT '[]',
                fingerprint_vec     TEXT,
                family_id           TEXT,
                ts_start            TEXT,
                ts_incident_signal  TEXT,
                ts_resolved         TEXT,
                remediation_action  TEXT,
                remediation_target  TEXT,
                remediation_outcome TEXT,
                remediation_confidence REAL DEFAULT 0.5,
                tier                TEXT DEFAULT 'warm',
                last_accessed_ts    TEXT,
                event_ids           TEXT DEFAULT '[]',
                causal_chain_ids    TEXT DEFAULT '[]'
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS incident_families (
                family_id               TEXT PRIMARY KEY,
                member_incident_ids     TEXT DEFAULT '[]',
                centroid_fingerprint    TEXT,
                remediation_success_rate REAL DEFAULT 0.0,
                total_incidents         INTEGER DEFAULT 0,
                created_at              TEXT,
                last_updated_ts         TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS metric_baselines (
                canonical_id    TEXT NOT NULL,
                metric_name     TEXT NOT NULL,
                baseline_value  REAL NOT NULL,
                stddev          REAL DEFAULT 0.0,
                sample_count    INTEGER DEFAULT 0,
                last_updated_ts TEXT NOT NULL,
                PRIMARY KEY (canonical_id, metric_name)
            )
        """)

        self.conn.commit()

    # ------------------------------------------------------------------
    # Service Identity operations
    # ------------------------------------------------------------------

    def upsert_service_identity(
        self,
        canonical_id: str,
        current_name: str,
        aliases: list[str],
        name_ranges: list[dict],
        first_seen_ts: str,
        last_seen_ts: str,
        behavioral_role: Optional[str] = None,
    ) -> None:
        """Insert or update a service identity row."""
        self.conn.execute("""
            INSERT INTO service_identities
                (canonical_id, current_name, aliases, name_ranges,
                 behavioral_role, first_seen_ts, last_seen_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (canonical_id) DO UPDATE SET
                current_name = excluded.current_name,
                aliases = excluded.aliases,
                name_ranges = excluded.name_ranges,
                behavioral_role = COALESCE(excluded.behavioral_role, service_identities.behavioral_role),
                last_seen_ts = excluded.last_seen_ts
        """, (
            canonical_id,
            current_name,
            json.dumps(aliases),
            json.dumps(name_ranges),
            behavioral_role,
            first_seen_ts,
            last_seen_ts,
        ))
        self.conn.commit()

    def get_service_identity(self, canonical_id: str) -> Optional[dict]:
        """Fetch a service identity by canonical_id."""
        cur = self.conn.execute(
            "SELECT * FROM service_identities WHERE canonical_id = ?",
            (canonical_id,)
        )
        row = cur.fetchone()
        if row is None:
            return None
        cols = [d[0] for d in cur.description]
        result = dict(zip(cols, row))
        result['aliases'] = json.loads(result['aliases'])
        result['name_ranges'] = json.loads(result['name_ranges'])
        return result

    def get_all_service_identities(self) -> list[dict]:
        """Fetch all service identities."""
        cur = self.conn.execute("SELECT * FROM service_identities")
        cols = [d[0] for d in cur.description]
        rows = []
        for r in cur.fetchall():
            row = dict(zip(cols, r))
            row['aliases'] = json.loads(row['aliases'])
            row['name_ranges'] = json.loads(row['name_ranges'])
            rows.append(row)
        return rows

    # ------------------------------------------------------------------
    # Event operations
    # ------------------------------------------------------------------

    def insert_event(
        self,
        event_id: str,
        canonical_id: str,
        kind: str,
        ts: str,
        raw_service: str,
        raw_json: dict,
        metric_name: Optional[str] = None,
        metric_value: Optional[float] = None,
        metric_source: Optional[str] = None,
        trace_id: Optional[str] = None,
        incident_id: Optional[str] = None,
    ) -> None:
        """Insert a raw event."""
        self.conn.execute("""
            INSERT OR IGNORE INTO raw_events
                (event_id, canonical_id, kind, ts, raw_service,
                 metric_name, metric_value, metric_source,
                 trace_id, incident_id, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event_id, canonical_id, kind, ts, raw_service,
            metric_name, metric_value, metric_source,
            trace_id, incident_id, json.dumps(raw_json),
        ))
        self.conn.commit()

    def get_event_by_id(self, event_id: str) -> Optional[dict]:
        """Fetch a single event by ID."""
        cur = self.conn.execute(
            "SELECT * FROM raw_events WHERE event_id = ?", (event_id,)
        )
        row = cur.fetchone()
        if row is None:
            return None
        cols = [d[0] for d in cur.description]
        result = dict(zip(cols, row))
        result['raw_json'] = json.loads(result['raw_json'])
        return result

    def query_events(
        self,
        canonical_id: Optional[str] = None,
        kind: Optional[str] = None,
        start_ts: Optional[str] = None,
        end_ts: Optional[str] = None,
        trace_id: Optional[str] = None,
        incident_id: Optional[str] = None,
        limit: int = 1000,
    ) -> list[dict]:
        """Flexible event query with optional filters."""
        conditions = []
        params = []

        if canonical_id is not None:
            conditions.append("canonical_id = ?")
            params.append(canonical_id)
        if kind is not None:
            conditions.append("kind = ?")
            params.append(kind)
        if start_ts is not None:
            conditions.append("ts >= ?")
            params.append(start_ts)
        if end_ts is not None:
            conditions.append("ts <= ?")
            params.append(end_ts)
        if trace_id is not None:
            conditions.append("trace_id = ?")
            params.append(trace_id)
        if incident_id is not None:
            conditions.append("incident_id = ?")
            params.append(incident_id)

        where = " AND ".join(conditions) if conditions else "1=1"
        sql = f"SELECT * FROM raw_events WHERE {where} ORDER BY ts ASC LIMIT ?"
        params.append(limit)

        cur = self.conn.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = []
        for r in cur.fetchall():
            row = dict(zip(cols, r))
            row['raw_json'] = json.loads(row['raw_json'])
            rows.append(row)
        return rows

    def get_recent_deploys(
        self, canonical_id: str, before_ts: str, window_minutes: int = 10
    ) -> list[dict]:
        """Get deploy events for a service before a given timestamp."""
        cur = self.conn.execute("""
            SELECT * FROM raw_events
            WHERE canonical_id = ?
              AND kind = 'deploy'
              AND ts <= ?
            ORDER BY ts DESC
            LIMIT 10
        """, (canonical_id, before_ts))
        cols = [d[0] for d in cur.description]
        rows = []
        for r in cur.fetchall():
            row = dict(zip(cols, r))
            row['raw_json'] = json.loads(row['raw_json'])
            rows.append(row)
        return rows

    def count_events(self) -> int:
        """Total events stored."""
        return self.conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]

    # ------------------------------------------------------------------
    # Causal edge operations
    # ------------------------------------------------------------------

    def insert_causal_edge(
        self,
        edge_id: str,
        cause_event_id: str,
        effect_event_id: str,
        cause_canonical_id: str,
        effect_canonical_id: str,
        edge_type: str,
        confidence: float,
        evidence: dict,
        created_at: str,
    ) -> None:
        """Insert a causal edge. Ignores duplicates."""
        self.conn.execute("""
            INSERT OR IGNORE INTO causal_edges
                (edge_id, cause_event_id, effect_event_id,
                 cause_canonical_id, effect_canonical_id,
                 edge_type, confidence, evidence,
                 reinforcement_count, created_at, last_reinforced_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
        """, (
            edge_id, cause_event_id, effect_event_id,
            cause_canonical_id, effect_canonical_id,
            edge_type, confidence, json.dumps(evidence),
            created_at, created_at,
        ))
        self.conn.commit()

    def reinforce_edge(self, edge_id: str, boost: float = 0.1, ts: Optional[str] = None) -> None:
        """Reinforce a causal edge — increase confidence, bump reinforcement count."""
        self.conn.execute("""
            UPDATE causal_edges
            SET confidence = MIN(confidence + ?, 1.0),
                reinforcement_count = reinforcement_count + 1,
                last_reinforced_at = COALESCE(?, last_reinforced_at)
            WHERE edge_id = ?
        """, (boost, ts, edge_id))
        self.conn.commit()

    def decay_edge(self, edge_id: str, decay: float = 0.05) -> None:
        """Decay a causal edge's confidence."""
        self.conn.execute("""
            UPDATE causal_edges
            SET confidence = MAX(confidence - ?, 0.0)
            WHERE edge_id = ?
        """, (decay, edge_id))
        self.conn.commit()

    def query_causal_edges(
        self,
        cause_canonical_id: Optional[str] = None,
        effect_canonical_id: Optional[str] = None,
        min_confidence: float = 0.0,
        limit: int = 100,
    ) -> list[dict]:
        """Query causal edges with flexible filters."""
        conditions = ["confidence >= ?"]
        params: list = [min_confidence]

        if cause_canonical_id is not None:
            conditions.append("cause_canonical_id = ?")
            params.append(cause_canonical_id)
        if effect_canonical_id is not None:
            conditions.append("effect_canonical_id = ?")
            params.append(effect_canonical_id)

        where = " AND ".join(conditions)
        sql = f"""
            SELECT * FROM causal_edges
            WHERE {where}
            ORDER BY confidence DESC, created_at DESC
            LIMIT ?
        """
        params.append(limit)

        cur = self.conn.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = []
        for r in cur.fetchall():
            row = dict(zip(cols, r))
            row['evidence'] = json.loads(row['evidence'])
            rows.append(row)
        return rows

    def get_causal_edges_for_events(self, event_ids: list[str]) -> list[dict]:
        """Get all causal edges where cause or effect is in the given event IDs."""
        if not event_ids:
            return []
        placeholders = ",".join(["?" for _ in event_ids])
        sql = f"""
            SELECT * FROM causal_edges
            WHERE cause_event_id IN ({placeholders})
               OR effect_event_id IN ({placeholders})
            ORDER BY confidence DESC
        """
        cur = self.conn.execute(sql, event_ids + event_ids)
        cols = [d[0] for d in cur.description]
        rows = []
        for r in cur.fetchall():
            row = dict(zip(cols, r))
            row['evidence'] = json.loads(row['evidence'])
            rows.append(row)
        return rows

    # ------------------------------------------------------------------
    # Incident episode operations
    # ------------------------------------------------------------------

    def upsert_incident_episode(self, episode: dict) -> None:
        """Insert or update an incident episode."""
        self.conn.execute("""
            INSERT INTO incident_episodes
                (incident_id, canonical_ids, fingerprint_vec, family_id,
                 ts_start, ts_incident_signal, ts_resolved,
                 remediation_action, remediation_target, remediation_outcome,
                 remediation_confidence, tier, last_accessed_ts,
                 event_ids, causal_chain_ids)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (incident_id) DO UPDATE SET
                canonical_ids = excluded.canonical_ids,
                fingerprint_vec = excluded.fingerprint_vec,
                family_id = excluded.family_id,
                ts_start = COALESCE(excluded.ts_start, incident_episodes.ts_start),
                ts_incident_signal = COALESCE(excluded.ts_incident_signal, incident_episodes.ts_incident_signal),
                ts_resolved = COALESCE(excluded.ts_resolved, incident_episodes.ts_resolved),
                remediation_action = COALESCE(excluded.remediation_action, incident_episodes.remediation_action),
                remediation_target = COALESCE(excluded.remediation_target, incident_episodes.remediation_target),
                remediation_outcome = COALESCE(excluded.remediation_outcome, incident_episodes.remediation_outcome),
                remediation_confidence = COALESCE(excluded.remediation_confidence, incident_episodes.remediation_confidence),
                tier = excluded.tier,
                last_accessed_ts = excluded.last_accessed_ts,
                event_ids = excluded.event_ids,
                causal_chain_ids = excluded.causal_chain_ids
        """, (
            episode.get('incident_id'),
            json.dumps(episode.get('canonical_ids', [])),
            json.dumps(episode.get('fingerprint_vec')),
            episode.get('family_id'),
            episode.get('ts_start'),
            episode.get('ts_incident_signal'),
            episode.get('ts_resolved'),
            episode.get('remediation_action'),
            episode.get('remediation_target'),
            episode.get('remediation_outcome'),
            episode.get('remediation_confidence', 0.5),
            episode.get('tier', 'warm'),
            episode.get('last_accessed_ts'),
            json.dumps(episode.get('event_ids', [])),
            json.dumps(episode.get('causal_chain_ids', [])),
        ))
        self.conn.commit()

    def get_incident_episode(self, incident_id: str) -> Optional[dict]:
        """Fetch a single incident episode."""
        cur = self.conn.execute(
            "SELECT * FROM incident_episodes WHERE incident_id = ?",
            (incident_id,)
        )
        row = cur.fetchone()
        if row is None:
            return None
        cols = [d[0] for d in cur.description]
        result = dict(zip(cols, row))
        for json_col in ['canonical_ids', 'fingerprint_vec', 'event_ids', 'causal_chain_ids']:
            if result.get(json_col) and isinstance(result[json_col], str):
                result[json_col] = json.loads(result[json_col])
        return result

    def get_all_incident_episodes(self, min_tier: Optional[str] = None) -> list[dict]:
        """Fetch all incident episodes, optionally filtered by tier."""
        if min_tier:
            tier_order = {'hot': 0, 'warm': 1, 'cold': 2}
            tiers = [t for t, o in tier_order.items() if o <= tier_order.get(min_tier, 2)]
            placeholders = ",".join(["?" for _ in tiers])
            sql = f"SELECT * FROM incident_episodes WHERE tier IN ({placeholders}) ORDER BY ts_incident_signal DESC"
            cur = self.conn.execute(sql, tiers)
        else:
            cur = self.conn.execute(
                "SELECT * FROM incident_episodes ORDER BY ts_incident_signal DESC"
            )

        cols = [d[0] for d in cur.description]
        rows = []
        for r in cur.fetchall():
            row = dict(zip(cols, r))
            for json_col in ['canonical_ids', 'fingerprint_vec', 'event_ids', 'causal_chain_ids']:
                if row.get(json_col) and isinstance(row[json_col], str):
                    row[json_col] = json.loads(row[json_col])
            rows.append(row)
        return rows

    # ------------------------------------------------------------------
    # Metric baseline operations
    # ------------------------------------------------------------------

    def upsert_metric_baseline(
        self,
        canonical_id: str,
        metric_name: str,
        value: float,
        ts: str,
    ) -> None:
        """Update rolling baseline for a metric using Welford's online algorithm."""
        cur = self.conn.execute("""
            SELECT baseline_value, stddev, sample_count
            FROM metric_baselines
            WHERE canonical_id = ? AND metric_name = ?
        """, (canonical_id, metric_name))
        existing = cur.fetchone()

        if existing is None:
            self.conn.execute("""
                INSERT INTO metric_baselines
                    (canonical_id, metric_name, baseline_value, stddev, sample_count, last_updated_ts)
                VALUES (?, ?, ?, 0.0, 1, ?)
            """, (canonical_id, metric_name, value, ts))
        else:
            old_mean, old_stddev, n = existing
            n += 1
            # Welford's online mean + variance
            delta = value - old_mean
            new_mean = old_mean + delta / n
            delta2 = value - new_mean
            # Running variance: M2 = stddev^2 * (n-1)
            old_m2 = old_stddev * old_stddev * (n - 1) if n > 1 else 0
            new_m2 = old_m2 + delta * delta2
            new_stddev = (new_m2 / n) ** 0.5 if n > 0 else 0.0

            self.conn.execute("""
                UPDATE metric_baselines
                SET baseline_value = ?, stddev = ?, sample_count = ?, last_updated_ts = ?
                WHERE canonical_id = ? AND metric_name = ?
            """, (new_mean, new_stddev, n, ts, canonical_id, metric_name))

        self.conn.commit()

    def get_metric_baseline(self, canonical_id: str, metric_name: str) -> Optional[dict]:
        """Get the current baseline for a metric."""
        cur = self.conn.execute("""
            SELECT baseline_value, stddev, sample_count, last_updated_ts
            FROM metric_baselines
            WHERE canonical_id = ? AND metric_name = ?
        """, (canonical_id, metric_name))
        result = cur.fetchone()
        if result is None:
            return None
        return {
            'baseline_value': result[0],
            'stddev': result[1],
            'sample_count': result[2],
            'last_updated_ts': result[3],
        }

    def is_metric_anomalous(
        self, canonical_id: str, metric_name: str, value: float, threshold_sigma: float = 2.0
    ) -> tuple[bool, float]:
        """Check if a metric value is anomalous relative to baseline.
        Returns (is_anomalous, z_score).
        """
        baseline = self.get_metric_baseline(canonical_id, metric_name)
        if baseline is None or baseline['sample_count'] < 3:
            return False, 0.0
        if baseline['stddev'] < 1e-9:
            # No variance — any different value is anomalous
            return abs(value - baseline['baseline_value']) > 1e-9, 0.0
        z_score = (value - baseline['baseline_value']) / baseline['stddev']
        return abs(z_score) >= threshold_sigma, z_score

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def stats(self) -> dict:
        """Quick stats for debugging."""
        return {
            'events': self.conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0],
            'services': self.conn.execute("SELECT COUNT(*) FROM service_identities").fetchone()[0],
            'causal_edges': self.conn.execute("SELECT COUNT(*) FROM causal_edges").fetchone()[0],
            'incidents': self.conn.execute("SELECT COUNT(*) FROM incident_episodes").fetchone()[0],
            'families': self.conn.execute("SELECT COUNT(*) FROM incident_families").fetchone()[0],
            'baselines': self.conn.execute("SELECT COUNT(*) FROM metric_baselines").fetchone()[0],
        }
