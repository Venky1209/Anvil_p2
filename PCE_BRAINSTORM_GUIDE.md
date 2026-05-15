# Persistent Context Engine — Complete Brainstorm & Implementation Guide
> Problem 02/04 · Open Track · ★★★★★ Frontier
> Repo: `git clone https://github.com/Sauhard74/Anvil-P-E && cd Anvil-P-E/bench-p02-context`

---

## TABLE OF CONTENTS

1. [What you're actually building](#1-what-youre-actually-building)
2. [Why every naive solution fails](#2-why-every-naive-solution-fails)
3. [The winning insight — canonical identity](#3-the-winning-insight--canonical-identity)
4. [Full architecture design](#4-full-architecture-design)
5. [Data models — every table and schema](#5-data-models--every-table-and-schema)
6. [Algorithms — step by step](#6-algorithms--step-by-step)
7. [Step-by-step implementation plan](#7-step-by-step-implementation-plan)
8. [Working with the benchmark repo](#8-working-with-the-benchmark-repo)
9. [Latency engineering](#9-latency-engineering)
10. [Memory evolution — the feedback loop](#10-memory-evolution--the-feedback-loop)
11. [The explain field — using LLM correctly](#11-the-explain-field--using-llm-correctly)
12. [Brainstorm space — open questions](#12-brainstorm-space--open-questions)
13. [Anti-patterns to avoid](#13-anti-patterns-to-avoid)
14. [24h execution timeline](#14-24h-execution-timeline)
15. [Self-check and benchmark commands](#15-self-check-and-benchmark-commands)

---

## 1. What you're actually building

A system that works like an **operational brain** for a distributed system.

Normal observability (Datadog, Grafana, etc.) stores raw telemetry — logs, metrics, traces. You query it when something breaks.

This system does something fundamentally different:
- It **remembers** that `payments-svc` crashed every time a specific kind of deploy landed
- It **knows** that `checkout-api` errors are downstream of `payments-svc` latency
- It **recognizes** the same failure pattern even after `payments-svc` is renamed to `billing-svc`
- It **suggests** the same rollback fix it learned worked 3 months ago

The interface is two methods:

```python
engine.ingest(events)                    # stream of JSONL events, 1000/sec
engine.reconstruct_context(signal, mode) # returns structured Context object
```

The output of `reconstruct_context` is NOT a search result. It's a structured `Context` object:

```python
{
  "related_events":         [...],   # what happened around this incident
  "causal_chain":           [...],   # deploy → latency → error, with confidence
  "similar_past_incidents": [...],   # INC-100 happened like this before
  "suggested_remediations": [...],   # rollback to v2.13.4 worked last time
  "confidence":             0.87,
  "explain":                "..."    # human-readable narrative
}
```

---

## 2. Why every naive solution fails

### Approach A: Vector similarity search (baseline in the SDK)
Store every event as an embedding. On incident, retrieve most similar events by cosine distance.

**Fails because:** After `payments-svc → billing-svc` rename, no events exist for `billing-svc`. Cosine distance to old `payments-svc` events is low because the service name string is different. `recall@5 → 0`.

### Approach B: Keyword/BM25 search
Index all events. Search by service name + error keywords.

**Fails because:** Same reason. Name changed. Also fails on paraphrase errors ("timeout" vs "connection refused" — same failure, different message).

### Approach C: Rule-based correlation
"If deploy happened in last 5 minutes, blame the deploy."

**Fails because:** Doesn't generalize. Doesn't handle multi-hop causality. Doesn't survive topology drift. Fails L3 adversarial with cascading renames.

### What the brief says explicitly:
> "Naive semantic retrieval, static vector similarity, or keyword pipelines are unlikely to clear the benchmark. Build a reasoning substrate, not a search bar."

---

## 3. The winning insight — canonical identity

**Every service has a canonical ID that never changes. Names are just aliases.**

```
canonical_id = "svc_7a3f"
  aliases: ["payments-svc", "billing-svc"]
  name_valid_ranges: [
    ("payments-svc", 2026-01-01, 2026-05-10T14:30),
    ("billing-svc",  2026-05-10T14:30, None)
  ]
```

When a `topology/rename` event arrives:
1. Look up `payments-svc` → get `canonical_id = svc_7a3f`
2. Add `billing-svc` as a new alias for `svc_7a3f`
3. All historical incidents, causal edges, remediations indexed under `svc_7a3f` are now instantly accessible when you query `billing-svc`

The rename is **invisible** to everything upstream. The memory substrate only ever sees canonical IDs.

This is not ML. It's a data model decision made in the first 2 hours of implementation.

---

## 4. Full architecture design

```
┌─────────────────────────────────────────────────────────────────┐
│  LAYER 1 — INGESTION PIPELINE                                   │
│                                                                 │
│  JSONL stream ──► Normalizer ──► Identity Resolver ──► Router   │
│                   (parse,         (name → canonical_id,          │
│                    dedupe,         rename handling,              │
│                    timestamp)      alias chain update)           │
└─────────────────────────────────────────────┬───────────────────┘
                                              │ resolved events
                                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  LAYER 2 — MEMORY SUBSTRATE                                     │
│                                                                 │
│  ┌──────────────────┐  ┌──────────────────┐  ┌───────────────┐ │
│  │  Identity nodes  │  │  Causal edges    │  │  Behavioral   │ │
│  │  canonical_id    │  │  cause→effect    │  │  fingerprints │ │
│  │  + alias list    │  │  confidence      │  │  per episode  │ │
│  │  + roles         │  │  + decay         │  │               │ │
│  └──────────────────┘  └──────────────────┘  └───────────────┘ │
│                                                                 │
│  ┌──────────────────┐  ┌──────────────────┐  ┌───────────────┐ │
│  │  Incident        │  │  Remediation     │  │  Topology     │ │
│  │  episodes        │  │  memory          │  │  snapshot log │ │
│  │  (full context   │  │  (action→outcome │  │  (append-only │ │
│  │   snapshots)     │  │   + confidence)  │  │   history)    │ │
│  └──────────────────┘  └──────────────────┘  └───────────────┘ │
└─────────────────────────────────────────────┬───────────────────┘
                                              │ query
                                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  LAYER 3 — CONTEXT RECONSTRUCTION                               │
│                                                                 │
│  fast mode (≤2s)                    deep mode (≤6s)             │
│  ─────────────────                  ────────────────────────    │
│  1. resolve signal → canonical_id   1. same as fast             │
│  2. fetch neighbor events (50ms)    2. compute fingerprint      │
│  3. lookup causal edges (10ms)      3. search episode store     │
│  4. rank by salience score (20ms)   4. multi-hop traversal      │
│  5. assemble Context (10ms)         5. LLM for explain only     │
│  6. generate explain (1.5s LLM)     6. full context assembly    │
└─────────────────────────────────────────────────────────────────┘
```

---

## 5. Data models — every table and schema

Use **DuckDB** as the storage backend. It handles analytical time-range queries 10x faster than SQLite and loads in-process with no server.

```python
import duckdb
conn = duckdb.connect("memory.db")  # or ":memory:" for tests
```

### Table 1: service_identities

```sql
CREATE TABLE service_identities (
    canonical_id    TEXT PRIMARY KEY,           -- e.g. "svc_a1b2"
    current_name    TEXT NOT NULL,              -- most recent alias
    aliases         JSON NOT NULL,              -- ["payments-svc", "billing-svc"]
    name_ranges     JSON NOT NULL,              -- [{"name": "payments-svc", "from": "...", "to": "..."}]
    behavioral_role TEXT,                       -- inferred: "payment_processor", "api_gateway", etc.
    first_seen_ts   TIMESTAMP NOT NULL,
    last_seen_ts    TIMESTAMP NOT NULL,
    fingerprint_vec FLOAT[32]                   -- pre-computed behavioral embedding (optional)
);
```

### Table 2: causal_edges

```sql
CREATE TABLE causal_edges (
    edge_id         TEXT PRIMARY KEY,
    cause_id        TEXT NOT NULL,              -- canonical_id of cause node
    effect_id       TEXT NOT NULL,              -- canonical_id of effect node
    edge_type       TEXT NOT NULL,              -- "deploy_caused_latency", "latency_caused_errors", etc.
    confidence      FLOAT NOT NULL DEFAULT 0.5,
    evidence_ids    JSON NOT NULL,              -- list of event IDs that support this edge
    first_seen_ts   TIMESTAMP NOT NULL,
    last_confirmed_ts TIMESTAMP NOT NULL,
    occurrence_count INTEGER DEFAULT 1,
    decay_rate      FLOAT DEFAULT 0.02          -- confidence decay per day without reinforcement
);
```

### Table 3: events_raw

```sql
CREATE TABLE events_raw (
    event_id        TEXT PRIMARY KEY,
    ts              TIMESTAMP NOT NULL,
    kind            TEXT NOT NULL,              -- deploy, log, metric, trace, topology, incident_signal, remediation
    canonical_id    TEXT,                       -- resolved service canonical_id (NULL for topology events)
    original_name   TEXT,                       -- the raw service name at ingestion time
    payload         JSON NOT NULL,              -- full original event JSON
    ingested_at     TIMESTAMP DEFAULT now()
);
CREATE INDEX idx_events_canonical ON events_raw(canonical_id, ts);
CREATE INDEX idx_events_kind ON events_raw(kind, ts);
```

### Table 4: incident_episodes

```sql
CREATE TABLE incident_episodes (
    episode_id      TEXT PRIMARY KEY,           -- usually the incident_id from the event
    incident_signal JSON NOT NULL,              -- original IncidentSignal
    causal_chain    JSON NOT NULL,              -- list of CausalEdge objects
    related_events  JSON NOT NULL,              -- list of event_ids
    fingerprint     JSON NOT NULL,              -- behavioral fingerprint (see Section 6)
    fingerprint_vec FLOAT[32],                  -- embedding of fingerprint for similarity search
    root_cause_id   TEXT,                       -- canonical_id of root cause
    remediation_id  TEXT,                       -- FK to remediations table
    status          TEXT DEFAULT 'open',        -- open / resolved / unresolved
    opened_ts       TIMESTAMP NOT NULL,
    resolved_ts     TIMESTAMP,
    resolution_time_sec INTEGER
);
```

### Table 5: remediations

```sql
CREATE TABLE remediations (
    remediation_id  TEXT PRIMARY KEY,
    episode_id      TEXT NOT NULL,
    action          TEXT NOT NULL,              -- "rollback", "restart", "scale", etc.
    target_id       TEXT NOT NULL,              -- canonical_id of target service
    target_version  TEXT,                       -- e.g. "v2.13.4"
    outcome         TEXT NOT NULL,              -- "resolved", "failed", "partial"
    confidence      FLOAT DEFAULT 0.5,          -- updated by reinforcement
    success_count   INTEGER DEFAULT 0,
    failure_count   INTEGER DEFAULT 0,
    created_ts      TIMESTAMP NOT NULL
);
```

### Table 6: topology_log

```sql
CREATE TABLE topology_log (
    log_id          TEXT PRIMARY KEY,
    ts              TIMESTAMP NOT NULL,
    change_type     TEXT NOT NULL,              -- "rename", "dependency_add", "dependency_remove"
    from_name       TEXT,
    to_name         TEXT,
    affected_ids    JSON,                       -- canonical_ids affected
    payload         JSON NOT NULL               -- original event
);
```

### Table 7: behavioral_patterns (incident families)

```sql
CREATE TABLE behavioral_patterns (
    pattern_id      TEXT PRIMARY KEY,
    family_name     TEXT,                       -- "deploy_regression", "cascade_latency", etc.
    causal_sequence JSON NOT NULL,              -- ["deploy", "latency_p99_up", "upstream_errors"]
    metric_signature JSON NOT NULL,             -- {"latency_p99_delta_pct": 400, "error_rate_delta_pct": 300}
    trigger_type    TEXT,                       -- "error_rate", "latency", "manual"
    episode_ids     JSON NOT NULL,              -- list of episode_ids in this family
    occurrence_count INTEGER DEFAULT 1,
    last_seen_ts    TIMESTAMP NOT NULL
);
```

---

## 6. Algorithms — step by step

### Algorithm A: Identity Resolution (runs on every event)

```python
def resolve_identity(service_name: str, event_ts: datetime) -> str:
    """
    Returns canonical_id for a service name at a given timestamp.
    Creates a new identity if service is unknown.
    """
    # 1. Direct lookup by current name
    row = db.execute(
        "SELECT canonical_id FROM service_identities WHERE current_name = ?",
        [service_name]
    ).fetchone()
    if row:
        return row[0]
    
    # 2. Lookup in historical alias ranges
    rows = db.execute(
        """SELECT canonical_id FROM service_identities 
           WHERE EXISTS (
             SELECT 1 FROM json_each(name_ranges) r
             WHERE r.value->>'name' = ?
           )""",
        [service_name]
    ).fetchall()
    if rows:
        return rows[0][0]
    
    # 3. Unknown service — create new identity
    canonical_id = f"svc_{uuid4().hex[:8]}"
    db.execute("""
        INSERT INTO service_identities
        VALUES (?, ?, ?, ?, NULL, ?, ?, NULL)
    """, [canonical_id, service_name, json.dumps([service_name]),
          json.dumps([{"name": service_name, "from": event_ts.isoformat(), "to": None}]),
          event_ts, event_ts])
    return canonical_id


def handle_rename(from_name: str, to_name: str, ts: datetime):
    """
    Called when a topology/rename event arrives.
    Merges to_name into the existing canonical identity.
    """
    canonical_id = resolve_identity(from_name, ts)
    
    # Close the old name range
    name_ranges = get_name_ranges(canonical_id)
    for r in name_ranges:
        if r["name"] == from_name and r["to"] is None:
            r["to"] = ts.isoformat()
    
    # Add new name range
    name_ranges.append({"name": to_name, "from": ts.isoformat(), "to": None})
    
    # Update the identity record
    db.execute("""
        UPDATE service_identities
        SET current_name = ?, aliases = json_insert(aliases, '$[#]', ?),
            name_ranges = ?, last_seen_ts = ?
        WHERE canonical_id = ?
    """, [to_name, to_name, json.dumps(name_ranges), ts, canonical_id])
    
    # Log to topology_log
    db.execute("""
        INSERT INTO topology_log VALUES (?, ?, 'rename', ?, ?, ?, ?)
    """, [uuid4().hex, ts, from_name, to_name,
          json.dumps([canonical_id]), json.dumps({"from": from_name, "to": to_name})])
```

---

### Algorithm B: Causal Edge Detection (runs after every deploy/metric/trace batch)

```python
def detect_causal_edges(deploy_event: dict):
    """
    After a deploy, scan the next 5 minutes for correlated degradation.
    Create causal edges with confidence scores.
    """
    canonical_id = resolve_identity(deploy_event["service"], deploy_event["ts"])
    deploy_ts = parse_ts(deploy_event["ts"])
    window_end = deploy_ts + timedelta(minutes=5)
    
    # 1. Find all metric events in the window for this service
    metrics_after = db.execute("""
        SELECT payload FROM events_raw
        WHERE canonical_id = ? AND kind = 'metric'
        AND ts BETWEEN ? AND ?
        ORDER BY ts
    """, [canonical_id, deploy_ts, window_end]).fetchall()
    
    # 2. Find all metric events in the 5 minutes BEFORE the deploy (baseline)
    metrics_before = db.execute("""
        SELECT payload FROM events_raw
        WHERE canonical_id = ? AND kind = 'metric'
        AND ts BETWEEN ? AND ?
        ORDER BY ts
    """, [canonical_id, deploy_ts - timedelta(minutes=5), deploy_ts]).fetchall()
    
    # 3. Compute delta for each metric
    for metric_name in ["latency_p99_ms", "error_rate", "cpu_percent"]:
        before_avg = avg_metric(metrics_before, metric_name)
        after_avg  = avg_metric(metrics_after,  metric_name)
        
        if before_avg and after_avg:
            delta_pct = (after_avg - before_avg) / before_avg * 100
            
            if delta_pct > 50:  # significant degradation threshold
                # Confidence: higher if few other deploys happened concurrently
                concurrent_deploys = count_concurrent_deploys(deploy_ts)
                confidence = 0.8 / max(concurrent_deploys, 1)
                
                upsert_causal_edge(
                    cause_id=canonical_id,
                    effect_id=canonical_id,       # self-edge for deploy→metric
                    edge_type=f"deploy_caused_{metric_name}_spike",
                    confidence=confidence,
                    evidence_ids=[deploy_event["event_id"]]
                )
    
    # 4. Find upstream callers that also degraded (from trace data)
    callers = get_callers_from_traces(canonical_id, deploy_ts, window_end)
    for caller_id in callers:
        caller_errors = count_errors(caller_id, deploy_ts, window_end)
        if caller_errors > 3:
            upsert_causal_edge(
                cause_id=canonical_id,
                effect_id=caller_id,
                edge_type="latency_caused_upstream_errors",
                confidence=0.7,
                evidence_ids=[]
            )


def upsert_causal_edge(cause_id, effect_id, edge_type, confidence, evidence_ids):
    """
    Insert edge or increment confidence if it already exists.
    """
    existing = db.execute("""
        SELECT edge_id, confidence, occurrence_count FROM causal_edges
        WHERE cause_id = ? AND effect_id = ? AND edge_type = ?
    """, [cause_id, effect_id, edge_type]).fetchone()
    
    if existing:
        # Reinforce: blend new confidence with existing
        new_conf = min(0.95, existing[1] + 0.05)
        db.execute("""
            UPDATE causal_edges
            SET confidence = ?, occurrence_count = ?, last_confirmed_ts = now()
            WHERE edge_id = ?
        """, [new_conf, existing[2] + 1, existing[0]])
    else:
        db.execute("""
            INSERT INTO causal_edges VALUES (?, ?, ?, ?, ?, ?, now(), now(), 1, 0.02)
        """, [uuid4().hex, cause_id, effect_id, edge_type,
              confidence, json.dumps(evidence_ids)])
```

---

### Algorithm C: Behavioral Fingerprint (the key to rename-invariant matching)

```python
def compute_fingerprint(episode_id: str, events: list, causal_chain: list) -> dict:
    """
    Computes a service-name-independent fingerprint for an incident episode.
    THIS MUST CONTAIN ZERO SERVICE NAMES.
    """
    
    # 1. Extract event kind sequence (not service names)
    event_kinds = [e["kind"] for e in sorted(events, key=lambda x: x["ts"])]
    
    # Reduce to meaningful transitions: deploy → metric_spike → log_error → incident
    causal_sequence = deduplicate_sequence(event_kinds)
    # e.g. ["deploy", "metric_spike", "log_error", "incident_signal"]
    
    # 2. Extract metric signature (direction + magnitude, not absolute values)
    metric_deltas = {}
    deploy_ts = get_deploy_ts(events)
    if deploy_ts:
        for metric_event in [e for e in events if e["kind"] == "metric"]:
            name = metric_event["payload"].get("name", "")
            delta_pct = compute_delta_from_baseline(metric_event, deploy_ts)
            if delta_pct:
                metric_deltas[name] = round(delta_pct, -1)  # round to nearest 10%
    
    # 3. Extract trigger type (not trigger service)
    trigger = get_trigger_type(events)  # "error_rate_spike", "latency_p99_spike", "manual"
    
    # 4. Extract structural role of root cause
    root_canonical_id = get_root_cause_id(causal_chain)
    root_role = get_service_role(root_canonical_id)  # "downstream_dependency", "api_gateway", etc.
    
    # 5. Extract resolution pattern
    resolution = get_resolution_pattern(events)  # "rollback", "restart", "config_change", "none"
    
    # 6. Timing features
    deploy_to_incident_mins = get_time_delta_mins(events, "deploy", "incident_signal")
    
    fingerprint = {
        "causal_sequence":        causal_sequence,
        "metric_signature":       metric_deltas,
        "trigger_type":           trigger,
        "root_cause_role":        root_role,
        "resolution_pattern":     resolution,
        "deploy_to_incident_mins": round(deploy_to_incident_mins) if deploy_to_incident_mins else None,
        # NO SERVICE NAMES ANYWHERE
    }
    
    return fingerprint


def fingerprint_similarity(fp1: dict, fp2: dict) -> float:
    """
    Compare two fingerprints. Returns 0.0–1.0.
    """
    score = 0.0
    
    # Causal sequence similarity (edit distance normalized)
    seq_sim = sequence_similarity(fp1["causal_sequence"], fp2["causal_sequence"])
    score += seq_sim * 0.4
    
    # Metric signature similarity (cosine on delta vectors)
    metric_sim = metric_vector_similarity(fp1["metric_signature"], fp2["metric_signature"])
    score += metric_sim * 0.3
    
    # Trigger type exact match
    if fp1["trigger_type"] == fp2["trigger_type"]:
        score += 0.15
    
    # Root cause role match
    if fp1["root_cause_role"] == fp2["root_cause_role"]:
        score += 0.1
    
    # Timing similarity (within same order of magnitude)
    t1 = fp1.get("deploy_to_incident_mins")
    t2 = fp2.get("deploy_to_incident_mins")
    if t1 and t2 and abs(t1 - t2) < 5:
        score += 0.05
    
    return min(score, 1.0)
```

---

### Algorithm D: Context Reconstruction — fast mode

```python
def reconstruct_context_fast(signal: IncidentSignal) -> Context:
    """
    Target: p95 ≤ 2 seconds.
    Strategy: all heavy work was done at ingest time.
    This function is purely lookups + sorting.
    """
    start = time.time()
    
    # 1. Resolve signal service to canonical_id (< 5ms)
    trigger_service = signal.get("trigger", "").split("/")[0]  # "checkout-api" from "alert:checkout-api/error-rate>5%"
    canonical_id = resolve_identity_from_signal(signal)
    
    # 2. Get time window around incident (< 10ms)
    incident_ts = parse_ts(signal["ts"])
    window_start = incident_ts - timedelta(minutes=30)
    window_end   = incident_ts + timedelta(minutes=5)
    
    # 3. Fetch related events from neighborhood (< 50ms)
    #    Include: the service itself + its direct neighbors in causal graph
    neighbor_ids = get_causal_neighbors(canonical_id)  # from causal_edges table
    all_ids = [canonical_id] + neighbor_ids
    
    related_raw = db.execute("""
        SELECT event_id, ts, kind, canonical_id, payload
        FROM events_raw
        WHERE canonical_id IN ({})
        AND ts BETWEEN ? AND ?
        ORDER BY ts DESC
        LIMIT 200
    """.format(",".join(["?"]*len(all_ids))),
        all_ids + [window_start, window_end]
    ).fetchall()
    
    # 4. Score and rank events by salience (< 20ms)
    #    Salience = recency × causal_proximity × event_importance
    ranked_events = rank_by_salience(related_raw, canonical_id, incident_ts)
    top_events = ranked_events[:20]  # return top 20
    
    # 5. Build causal chain from pre-computed edges (< 10ms)
    causal_chain = build_causal_chain(canonical_id, top_events)
    
    # 6. Find similar past incidents by fingerprint (< 50ms)
    #    Compute quick fingerprint from current signals, compare to stored ones
    current_fp = compute_quick_fingerprint(top_events, signal)
    similar = search_similar_episodes(current_fp, limit=5)
    
    # 7. Get suggested remediations from past similar incidents (< 10ms)
    remediations = get_remediations_for_episodes([s["episode_id"] for s in similar])
    
    # 8. Generate explain field (≤ 1.5s LLM call)
    explain = generate_explain(top_events, causal_chain, similar, remediations)
    
    elapsed = time.time() - start
    assert elapsed < 2.0, f"Fast mode too slow: {elapsed:.2f}s"
    
    return Context(
        related_events         = [format_event(e) for e in top_events],
        causal_chain           = causal_chain,
        similar_past_incidents = similar,
        suggested_remediations = remediations,
        confidence             = compute_confidence(causal_chain, similar),
        explain                = explain
    )
```

---

### Algorithm E: Salience Scoring

```python
def salience_score(event: dict, focal_canonical_id: str, incident_ts: datetime) -> float:
    """
    Score an event for relevance to the current incident.
    Higher = more relevant.
    """
    score = 0.0
    
    event_ts = parse_ts(event["ts"])
    age_minutes = (incident_ts - event_ts).total_seconds() / 60
    
    # 1. Recency: events close to incident time score higher
    recency = math.exp(-0.1 * max(age_minutes, 0))  # decay over 30 minutes
    score += recency * 0.3
    
    # 2. Event kind importance
    kind_weights = {
        "deploy":          0.9,   # deploys are almost always relevant
        "incident_signal": 0.8,   # other incidents very relevant
        "metric":          0.5,   # metrics contextual
        "trace":           0.6,   # traces show causality
        "log":             0.4,   # logs useful but noisy
        "topology":        0.7,   # topology changes highly relevant
        "remediation":     0.9,   # remediations very relevant
    }
    score += kind_weights.get(event["kind"], 0.3) * 0.3
    
    # 3. Causal proximity: is this event directly in the causal chain?
    if is_in_causal_chain(event["event_id"], focal_canonical_id):
        score += 0.4
    elif event["canonical_id"] == focal_canonical_id:
        score += 0.2  # same service, not in chain
    
    return min(score, 1.0)
```

---

## 7. Step-by-step implementation plan

### Hour 0–1: Repository setup

```bash
# Clone and explore
git clone https://github.com/Sauhard74/Anvil-P-E
cd Anvil-P-E/bench-p02-context

# Read these files IN ORDER:
cat schema.py          # Event, IncidentSignal, Context TypedDicts
cat adapter.py         # Base class: ingest(), reconstruct_context(), close()
cat self_check.py      # What metrics are checked, how they're computed
cat run.py             # Full benchmark runner

# Run the baseline to see what score you're starting from
python self_check.py --adapter adapters.baseline:Engine --quick
# Note the scores. You need to beat every axis.
```

**Deliverable at end of Hour 1:** Understand the exact shape of every input/output type. Write skeleton `adapters/team.py` with all methods stubbed.

---

### Hour 1–2: Data model + storage setup

```python
# Create adapters/team.py
from adapter import Adapter
from schema import Event, IncidentSignal, Context
import duckdb, json
from datetime import datetime, timedelta
from uuid import uuid4

class Engine(Adapter):
    def __init__(self):
        self.conn = duckdb.connect(":memory:")  # swap to file for persistence
        self._init_schema()
        self._alias_cache = {}   # name → canonical_id, hot path cache
    
    def _init_schema(self):
        self.conn.execute("""
            CREATE TABLE service_identities (
                canonical_id TEXT PRIMARY KEY,
                current_name TEXT,
                aliases TEXT,        -- JSON array
                name_ranges TEXT,    -- JSON array
                behavioral_role TEXT,
                first_seen_ts TIMESTAMP,
                last_seen_ts TIMESTAMP
            )
        """)
        # ... (create all other tables from Section 5)
    
    def ingest(self, events):
        for e in events:
            self._process_event(e)
    
    def reconstruct_context(self, signal, mode="fast"):
        if mode == "fast":
            return self._fast_reconstruct(signal)
        return self._deep_reconstruct(signal)
    
    def close(self):
        self.conn.close()
```

**Deliverable at end of Hour 2:** All tables created, skeleton methods exist, `self_check.py` runs without crashing (even if scores are 0).

---

### Hour 2–4: Identity resolver + event ingestion

Implement in this exact order:

1. `resolve_identity(name, ts)` → canonical_id
2. `handle_rename(from_name, to_name, ts)`
3. `_process_event(event)` → parse kind, call resolve_identity, insert into events_raw
4. Test: manually ingest the worked example JSONL and verify:
   - `payments-svc` gets a canonical_id
   - After the topology/rename event, `billing-svc` resolves to the SAME canonical_id
   - All 7 events from the worked example are in events_raw

```python
# Quick manual test
engine = Engine()
events = [
    {"ts":"2026-05-10T14:21:30Z","kind":"deploy","service":"payments-svc","version":"v2.14.0","actor":"ci"},
    # ... paste all 7 events from the brief
]
engine.ingest(events)

# Verify rename worked
id_before = engine.resolve_identity("payments-svc", datetime(2026, 5, 10, 14, 25))
id_after  = engine.resolve_identity("billing-svc",  datetime(2026, 5, 10, 14, 35))
assert id_before == id_after, "RENAME NOT WORKING"
print("Rename test PASSED:", id_before)
```

**Deliverable at end of Hour 4:** All event kinds ingested correctly, rename resolves to same canonical_id, `events_raw` populated correctly.

---

### Hour 4–6: Causal edge detection

Implement causal edge creation for these patterns:

**Pattern 1: Deploy → metric degradation**
- On every `deploy` event, scan next 5 min for metric spikes
- Create edge `deploy_caused_latency_spike` / `deploy_caused_error_rate`

**Pattern 2: Trace-based parent→child causality**
- On every `trace` event, extract spans with high `dur_ms`
- The span with highest duration relative to parent is the causal bottleneck
- Create edge `svc_A_caused_svc_B_latency`

**Pattern 3: Co-occurring error logs**
- If `checkout-api` throws an error containing service name X in the message
- And service X has high latency metrics at the same time
- Create/reinforce edge `X_latency_caused_checkout_errors`

```python
# Test causal edge detection
engine = Engine()
engine.ingest(worked_example_events)

edges = engine.conn.execute("SELECT * FROM causal_edges").fetchall()
print(edges)
# Should see: payments-svc deploy → latency spike, latency → checkout-api errors
```

**Deliverable at end of Hour 6:** At least 2 causal edges created from the worked example. Edges survive the rename (queried by canonical_id, not name).

---

### Hour 6–8: Behavioral fingerprint + episode storage

Implement:

1. `compute_fingerprint(episode_id, events, causal_chain)` → dict (NO service names)
2. `fingerprint_similarity(fp1, fp2)` → float 0..1
3. `store_episode(incident_id, events, causal_chain)` → insert into incident_episodes
4. `search_similar_episodes(fingerprint, limit=5)` → list of IncidentMatch

```python
# Test fingerprint rename invariance
engine = Engine()

# Ingest incident 1: payments-svc deploy regression
engine.ingest(old_incident_events)   # payments-svc

# Ingest rename
engine.ingest([rename_event])        # payments-svc → billing-svc

# Ingest incident 2: same pattern, now billing-svc
engine.ingest(new_incident_events)   # billing-svc

# Similarity should be HIGH (≥ 0.7)
fp1 = engine.get_episode_fingerprint("INC-100")
fp2 = engine.compute_quick_fingerprint(new_incident_events, new_signal)
sim = engine.fingerprint_similarity(fp1, fp2)
assert sim >= 0.7, f"Fingerprint similarity too low: {sim}"
print("Rename invariance test PASSED:", sim)
```

**Deliverable at end of Hour 8:** Fingerprint similarity correctly identifies same incident pattern across rename. First `self_check.py --quick` run with non-zero `recall@5`.

---

### Hour 8–10: Context reconstruction — fast mode

Implement `_fast_reconstruct(signal)` step by step:

1. Parse the trigger service from `signal["trigger"]`
2. Resolve to canonical_id
3. Fetch events from `events_raw` in 30-min window
4. Score each event with `salience_score()`
5. Build causal chain from `causal_edges`
6. Call `search_similar_episodes()` with quick fingerprint
7. Fetch remediations for matching episodes
8. Generate explain with a single LLM call
9. Return Context object

**Critical: check latency at this step.**

```bash
# Benchmark fast mode latency
python -c "
import time
from adapters.team import Engine
from bench_utils import load_sample_events, make_signal

engine = Engine()
engine.ingest(load_sample_events())

signal = make_signal('INC-714')
times = []
for _ in range(20):
    t = time.time()
    ctx = engine.reconstruct_context(signal, mode='fast')
    times.append(time.time() - t)

import statistics
print(f'p95: {sorted(times)[int(0.95*len(times))]:.3f}s')
print(f'mean: {statistics.mean(times):.3f}s')
"
```

**Target: p95 < 1.5s in fast mode** (leaves buffer for LLM variance).

**Deliverable at end of Hour 10:** Fast mode returns valid Context. `recall@5 > 0.4`, latency p95 < 2s.

---

### Hour 10–12: Deep mode + run self_check properly

Implement `_deep_reconstruct(signal)`:
- Same as fast, plus:
- Multi-hop traversal (2–3 hops through causal graph)
- Full fingerprint computation (not quick version)
- More similar episodes (limit=10)

Run full self-check and record baseline scores:

```bash
python self_check.py --adapter adapters.team:Engine --quick
# Record: recall@5, precision@5, remediation_acc, latency_p95
```

**Deliverable at end of Hour 12:** All metrics non-zero. System passes L1 canonical scenario.

---

### Hour 12–16: Fix the weak axes

Look at self-check output. Iterate on the lowest-scoring axis:

**If recall@5 is low:** Your fingerprint isn't topology-independent enough. Check: does it contain any service names? Remove them. Check: is the rename actually resolving to the same canonical_id?

**If precision@5 is low:** You're returning irrelevant past incidents. Tighten the similarity threshold. Add more features to the fingerprint (timing, metric direction).

**If remediation_acc is low:** Your remediation retrieval isn't connecting to the right canonical_id. Check that remediations are stored by canonical_id, not service name.

**If latency_p95 is too high:** You're computing too much at query time. Move work to ingest time. Pre-compute fingerprints. Cache canonical_id lookups.

---

### Hour 16–18: Memory evolution (reinforcement)

Implement the feedback loop:

```python
def handle_remediation_event(event: dict):
    """
    When a remediation with outcome arrives, reinforce the memory.
    """
    incident_id = event["incident_id"]
    outcome     = event["outcome"]      # "resolved" or "failed"
    
    # 1. Find the episode
    episode = get_episode(incident_id)
    if not episode:
        return
    
    # 2. Update remediation confidence
    if outcome == "resolved":
        db.execute("""
            UPDATE remediations SET
                confidence = MIN(0.95, confidence + 0.1),
                success_count = success_count + 1,
                outcome = 'resolved'
            WHERE episode_id = ?
        """, [incident_id])
        
        # Reinforce the causal edges that were in this episode's chain
        for edge_id in episode.get("causal_edge_ids", []):
            db.execute("""
                UPDATE causal_edges SET
                    confidence = MIN(0.95, confidence + 0.05),
                    occurrence_count = occurrence_count + 1
                WHERE edge_id = ?
            """, [edge_id])
    
    elif outcome == "failed":
        db.execute("""
            UPDATE remediations SET
                confidence = MAX(0.1, confidence - 0.15),
                failure_count = failure_count + 1
            WHERE episode_id = ?
        """, [incident_id])
    
    # 3. Mark episode resolved
    db.execute("""
        UPDATE incident_episodes
        SET status = ?, resolved_ts = ?
        WHERE episode_id = ?
    """, [outcome, event["ts"], incident_id])
```

**Test:** Run self-check before and after ingesting remediation events. `memory_evolution` metric should improve.

---

### Hour 18–20: Stress test at scale

```bash
# Stress test with more services and more days
python run.py --adapter adapters.team:Engine --mode fast \
  --seeds 9999 31415 27182 \
  --n-services 20 --days 14 \
  --out report_20svc.json

cat report_20svc.json  # check all metrics

# Try adversarial parameters
python run.py --adapter adapters.team:Engine \
  --seeds 99 100 101 102 \
  --n-services 30 --days 21 \
  --out report_stress.json
```

Fix any crashes or performance degradations found here.

---

### Hour 20–22: Edge cases + hardening

Handle these edge cases explicitly:

```python
# Edge case 1: signal for completely unknown service
def test_unknown_service():
    signal = {"ts": "2026-05-15T10:00:00Z", "trigger": "alert:new-svc/error-rate>5%"}
    ctx = engine.reconstruct_context(signal)
    assert ctx["confidence"] < 0.3   # should be low confidence, not a crash

# Edge case 2: signal during rename event (race condition)
def test_mid_rename_signal():
    engine.ingest([rename_event])
    signal = {"ts": rename_event["ts"], "trigger": "alert:billing-svc/latency>1s"}
    ctx = engine.reconstruct_context(signal)
    # Should resolve to canonical_id and find pre-rename history

# Edge case 3: cascading renames (payments-svc → billing-svc → finance-svc)
def test_cascading_rename():
    engine.ingest([rename_1, rename_2])
    id1 = engine.resolve_identity("payments-svc")
    id2 = engine.resolve_identity("billing-svc")
    id3 = engine.resolve_identity("finance-svc")
    assert id1 == id2 == id3   # all point to same canonical_id

# Edge case 4: empty event stream
def test_empty_ingest():
    engine2 = Engine()
    engine2.ingest([])
    ctx = engine2.reconstruct_context(signal)
    assert ctx["confidence"] == 0.0
```

---

### Hour 22–24: Demo, writeup, packaging

**Docker setup:**
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "run.py", "--adapter", "adapters.team:Engine", \
     "--mode", "fast", "--out", "report.json"]
```

**requirements.txt:**
```
duckdb>=0.10.0
sentence-transformers>=2.2.0
anthropic>=0.25.0   # for explain field LLM call
networkx>=3.0       # optional, for graph traversal
python-dateutil>=2.8.0
```

---

## 8. Working with the benchmark repo

### File structure you care about

```
bench-p02-context/
├── schema.py          ← READ FIRST: Event, IncidentSignal, Context TypedDicts
├── adapter.py         ← READ SECOND: base class interface
├── self_check.py      ← your main iteration tool
├── run.py             ← full benchmark runner
├── adapters/
│   ├── baseline.py    ← the baseline to beat
│   └── team.py        ← YOU CREATE THIS
└── bench_utils.py     ← helper functions
```

### The exact interface you must implement

```python
# adapter.py defines:
class Adapter:
    def ingest(self, events: Iterable[Event]) -> None:
        """
        Called with a stream of events. May be called multiple times.
        Must handle all 6 event kinds: deploy, log, metric, trace, topology, incident_signal, remediation
        """
        raise NotImplementedError
    
    def reconstruct_context(
        self,
        signal: IncidentSignal,
        mode: Literal["fast", "deep"] = "fast",
    ) -> Context:
        """
        Called with an incident signal. Must return a Context object.
        Fast mode: p95 ≤ 2s
        Deep mode: p95 ≤ 6s
        """
        raise NotImplementedError
    
    def close(self) -> None:
        raise NotImplementedError
```

### Running the benchmark — every command you need

```bash
# 1. Quick iteration check (fast, 2 seeds)
python self_check.py --adapter adapters.team:Engine --quick

# 2. Full self-check (all seeds)
python self_check.py --adapter adapters.team:Engine

# 3. Full run with custom seeds
python run.py --adapter adapters.team:Engine --mode fast \
  --seeds 9999 31415 27182 16180 11235 \
  --out report.json

# 4. Scale stress test (20 services, 14 days)
python run.py --adapter adapters.team:Engine \
  --n-services 20 --days 14 \
  --seeds 42 99 \
  --out report_large.json

# 5. Adversarial scale test (simulate L3 parameters)
python run.py --adapter adapters.team:Engine \
  --n-services 40 --days 30 \
  --seeds 1 2 3 4 5 \
  --out report_adversarial.json

# 6. Deep mode benchmark
python run.py --adapter adapters.team:Engine --mode deep \
  --seeds 9999 \
  --out report_deep.json

# 7. Check report output
python -c "
import json
with open('report.json') as f:
    r = json.load(f)
for k, v in r.items():
    print(f'{k}: {v}')
"
```

### Metrics to watch and their targets

| Metric            | Baseline | Target (yours) | How to improve |
|-------------------|----------|----------------|----------------|
| recall@5          | ~0.2     | ≥ 0.7          | Fingerprint rename-invariance |
| precision@5_mean  | ~0.3     | ≥ 0.6          | Tighten similarity threshold |
| remediation_acc   | ~0.2     | ≥ 0.6          | Fix canonical_id in remediation store |
| latency_p95_fast  | ~0.5s    | ≤ 1.5s         | Pre-compute at ingest, cache |
| latency_p95_deep  | ~2s      | ≤ 5s           | Async LLM, bounded traversal |
| memory_evolution  | ~0.0     | ≥ 0.3          | Implement reinforcement loop |
| adaptability_Δ    | large    | small          | Canonical identity layer |

---

## 9. Latency engineering

The latency budget is strict. Here's where time goes and how to stay under:

### Fast mode budget breakdown (total: 2000ms)

| Step | Budget | How to hit it |
|------|--------|---------------|
| Identity resolution | 5ms | In-memory dict cache |
| Event fetch from DB | 50ms | Indexed query on (canonical_id, ts) |
| Salience scoring | 20ms | Pure Python, no DB |
| Causal chain lookup | 10ms | In-memory adjacency list, not DB query |
| Fingerprint computation | 30ms | Pre-compute at ingest, just lookup here |
| Episode similarity search | 50ms | Pre-computed vectors, dot product |
| Remediation lookup | 10ms | Indexed by canonical_id |
| LLM explain call | 1200ms | Claude Haiku or GPT-3.5-turbo, ≤200 token output |
| Assembly + serialization | 25ms | Simple dict construction |
| **Total** | **1400ms** | **600ms buffer** |

### Key optimizations

**1. Alias cache in memory:**
```python
self._alias_cache: dict[str, str] = {}  # name → canonical_id

def resolve_identity(self, name: str) -> str:
    if name in self._alias_cache:
        return self._alias_cache[name]
    # ... DB lookup
    self._alias_cache[name] = canonical_id
    return canonical_id
```

**2. Causal graph in memory:**
```python
# Rebuild from DB on startup, keep in memory
self._causal_graph: dict[str, list[str]] = {}  # canonical_id → [neighbor_ids]
```

**3. Pre-compute fingerprints at episode creation time:**
Never compute fingerprints at query time. When you store an episode, compute and store the fingerprint vector immediately.

**4. Bounded LLM prompt:**
```python
EXPLAIN_PROMPT = """
Given this incident context, write a 2-sentence explanation for an SRE:

Trigger: {trigger}
Root cause (inferred): {root_cause}
Similar past incident: {similar_incident}
Recommended action: {remediation}

Be specific, not generic. Max 50 words.
"""
# Max tokens: 100. Use claude-haiku or gpt-3.5-turbo.
```

---

## 10. Memory evolution — the feedback loop

This is 10% of the score but it's what separates a static retrieval system from an actual memory engine. Here's the full loop:

```
Incident fires
     │
     ▼
reconstruct_context() → suggests remediation X
     │
     ▼
Operator applies remediation X
     │
     ├─► outcome: "resolved"
     │       │
     │       ▼
     │   Reinforce:
     │   - causal edges +0.05 confidence
     │   - remediation X +0.10 confidence  
     │   - fingerprint pattern +1 occurrence
     │   - episode marked resolved
     │
     └─► outcome: "failed"
             │
             ▼
         Decay:
         - remediation X -0.15 confidence
         - episode marked failed
         - look for alternative in history
```

**Test memory evolution concretely:**
```python
# Before feedback: first run
ctx1 = engine.reconstruct_context(signal1)
print("Remediation confidence before:", ctx1.suggested_remediations[0]["confidence"])
# e.g. 0.50

# Ingest resolution events (5 successful rollbacks)
engine.ingest(resolution_events)

# After feedback: same signal type
ctx2 = engine.reconstruct_context(signal2)  # similar incident
print("Remediation confidence after:", ctx2.suggested_remediations[0]["confidence"])
# Should be higher, e.g. 0.80
```

The benchmark measures this as `memory_evolution = (score_after_feedback - score_before_feedback)`. Any positive number is good. Target ≥ 0.3 improvement.

---

## 11. The explain field — using LLM correctly

The explain field is judge-graded 1–5. Here's how to make it score 4–5:

**What judges want to see:**
- Specific service names (use the current name, not canonical_id)
- Specific version numbers from deploy events
- Reference to past incident if similar one found
- Specific remediation recommendation with confidence
- Causal chain in plain English

**What they don't want:**
- Generic "a service experienced degradation"
- Uncertainty without a reason ("unclear what caused this")
- Technical jargon about your system internals
- Long paragraphs — 2-3 sentences is ideal

**Template for explain generation:**

```python
def generate_explain(events, causal_chain, similar_incidents, remediations):
    context_summary = {
        "trigger_service": get_trigger_service_name(events),  # use current alias
        "root_cause": summarize_causal_chain(causal_chain),
        "deploy_version": get_deploy_version(events),
        "time_since_deploy": get_time_since_deploy(events),
        "similar_incident": similar_incidents[0] if similar_incidents else None,
        "top_remediation": remediations[0] if remediations else None,
    }
    
    prompt = f"""
    SRE incident context. Write a 2-sentence explanation.
    Sentence 1: what happened and likely cause.
    Sentence 2: recommended action based on history.
    
    Facts:
    - Trigger: {context_summary['trigger_service']} showing high error rate
    - {context_summary['root_cause']}
    - Deploy {context_summary['deploy_version']} landed {context_summary['time_since_deploy']} ago
    {f"- Similar to {context_summary['similar_incident']['past_incident_id']} ({context_summary['similar_incident']['similarity']:.0%} match)" if context_summary['similar_incident'] else ""}
    {f"- Historical fix: {context_summary['top_remediation']['action']} (worked {context_summary['top_remediation']['historical_outcome']} last time)" if context_summary['top_remediation'] else ""}
    
    Be specific. 50 words max.
    """
    
    response = anthropic_client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=100,
        messages=[{"role": "user", "content": prompt}]
    )
    return response.content[0].text
```

---

## 12. Brainstorm space — open questions

Use this section to think through design decisions:

### Q1: How do I infer behavioral roles for services?
Options:
- A) From trace spans: the service always at the root of traces is the "entry point" / "api_gateway"
- B) From dependency graph: services with many callers are "shared dependencies" / "data stores"
- C) From error patterns: services that always error under load vs services that cascade errors
- D) Just hardcode common roles based on naming patterns? (risky, names change)

**Best answer:** A + B. Mine roles from trace structure. Entry point = service with no parent spans. Leaf service = service with no child spans. Role doesn't change even under rename.

---

### Q2: How many hops should the causal traversal go in deep mode?
- 1 hop: fast but misses cross-service causality
- 2 hops: catches most real-world cascades
- 3 hops: comprehensive but slow and noisy
- Unlimited: extremely slow, irrelevant results

**Best answer:** 2 hops with confidence threshold cutoff. Only follow edges with confidence > 0.4. This naturally limits traversal to meaningful paths.

---

### Q3: When should I create a new behavioral pattern vs add to an existing one?
Options:
- A) Always create new pattern, cluster later
- B) If fingerprint similarity to existing pattern > 0.7, add to it
- C) Use a clustering algorithm periodically

**Best answer:** B. Threshold of 0.7 is conservative enough to avoid false merges but catches real family members. Run cluster merge as a background process every N ingested events.

---

### Q4: What happens when the LLM is slow or unavailable?
The explain field is required but should not block the response.

**Best answer:** Wrap LLM call in try/except with timeout. On failure, use a template string:
```python
explain = (
    f"Incident triggered on {trigger_service}. "
    f"Likely cause: {causal_chain[0]['edge_type'] if causal_chain else 'unknown'}. "
    f"Consider: {remediations[0]['action'] if remediations else 'investigating the most recent deploy'}."
)
```

---

### Q5: How do I handle the case where two services are renamed to each other? (adversarial L3)
e.g. `svc-A` renamed to `svc-B`, and later `svc-B` renamed to `svc-A` again.

**Best answer:** The name_ranges table handles this correctly — each range has a validity period. Lookups are time-scoped: "which canonical_id did the name 'payments-svc' refer to at time T?" The same name can point to different canonical_ids at different times if it was recycled.

---

### Q6: Should I use embeddings at all?
The brief warns against pure vector similarity, but embeddings can be a *secondary* signal.

**Best answer:** Yes, but only as one component of fingerprint similarity — weighted at 20–30%, not as the primary signal. Embeddings of the `explain` field or error log messages can help catch paraphrased versions of the same failure.

---

## 13. Anti-patterns to avoid

| Anti-pattern | Why it fails | Fix |
|---|---|---|
| Storing service names in fingerprints | Fails rename test immediately | Store event kinds, roles, metric deltas only |
| Building causal chains at query time | Too slow for p95 ≤ 2s | Build edges at ingest time |
| Single vector store for everything | Baseline behavior, fails L2+ | Temporal property graph + fingerprint matching |
| LLM for retrieval, not just explanation | Too slow, non-deterministic | LLM only for `explain` field |
| Unlimited causal traversal in fast mode | Latency blowout | Hard limit: 2 hops, confidence threshold |
| Not GC'ing low-confidence edges | Memory bloat at scale | Decay edges below 0.2, delete after 7 days |
| Assuming topology is static | Fails L2 rename test immediately | Canonical identity layer handles all mutations |
| Caching results across seeds | Self-check detects this, fails | Fresh adapter instance per seed run |
| No error handling on LLM call | Crashes under any timeout | Fallback template explain always ready |

---

## 14. 24h execution timeline

```
Hour 0–1   : Read bench repo, understand schema, stub adapter
Hour 1–2   : DuckDB schema, init, skeleton methods green
Hour 2–4   : Identity resolver + event ingestion working
Hour 4–6   : Causal edge detection (deploy+trace based)
Hour 6–8   : Behavioral fingerprint + episode storage
Hour 8–10  : Fast mode reconstruct_context working
Hour 10–11 : First self_check --quick, record scores
Hour 11–14 : Iterate on weakest axis (likely recall@5)
Hour 14–15 : Deep mode reconstruct_context
Hour 15–16 : Memory evolution / reinforcement loop
Hour 16–17 : LLM explain field polishing
Hour 17–18 : Latency profiling + optimization
Hour 18–20 : Scale stress test (20 services, 14 days)
Hour 20–21 : Edge case hardening + cascading rename test
Hour 21–22 : Docker packaging + README quickstart
Hour 22–23 : 5-min demo recording (worked example)
Hour 23–24 : 3-page writeup PDF
```

---

## 15. Self-check and benchmark commands

```bash
# ── SETUP ──────────────────────────────────────────────────────
git clone https://github.com/Sauhard74/Anvil-P-E
cd Anvil-P-E/bench-p02-context
pip install duckdb sentence-transformers anthropic python-dateutil networkx

# ── ITERATE ────────────────────────────────────────────────────
# Fast iteration (30 seconds)
python self_check.py --adapter adapters.team:Engine --quick

# Full self-check (2-3 minutes)
python self_check.py --adapter adapters.team:Engine

# ── SCALE TEST ─────────────────────────────────────────────────
# L2 default scale
python run.py --adapter adapters.team:Engine --mode fast \
  --seeds 9999 31415 27182 16180 11235 \
  --out report_l2.json

# Simulate L3 adversarial scale
python run.py --adapter adapters.team:Engine --mode fast \
  --seeds 1 2 3 4 5 6 7 8 9 10 \
  --n-services 40 --days 21 \
  --out report_l3_sim.json

# ── READ RESULTS ───────────────────────────────────────────────
python -c "
import json
with open('report_l2.json') as f: r = json.load(f)
metrics = ['recall_at_5', 'precision_at_5_mean', 'remediation_acc',
           'latency_p95_fast', 'adaptability_delta', 'memory_evolution']
for m in metrics:
    val = r.get(m, 'N/A')
    print(f'{m:30s} {val}')
"

# ── RENAME TEST (manual verification) ──────────────────────────
python -c "
from adapters.team import Engine
import json

engine = Engine()

# Ingest pre-rename incident
events_pre = [
  {'ts':'2026-01-01T10:00:00Z','kind':'deploy','service':'payments-svc','version':'v1.0.0','actor':'ci'},
  {'ts':'2026-01-01T10:02:00Z','kind':'metric','service':'payments-svc','name':'latency_p99_ms','value':4000},
  {'ts':'2026-01-01T10:05:00Z','kind':'incident_signal','incident_id':'INC-100','trigger':'alert:payments-svc/latency>1s'},
  {'ts':'2026-01-01T10:30:00Z','kind':'remediation','incident_id':'INC-100','action':'rollback','target':'payments-svc','version':'v0.9.9','outcome':'resolved'},
]
engine.ingest(events_pre)

# Rename
engine.ingest([{'ts':'2026-03-01T00:00:00Z','kind':'topology','change':'rename','from':'payments-svc','to':'billing-svc'}])

# Ingest post-rename incident (same pattern)
events_post = [
  {'ts':'2026-04-01T10:00:00Z','kind':'deploy','service':'billing-svc','version':'v2.0.0','actor':'ci'},
  {'ts':'2026-04-01T10:02:00Z','kind':'metric','service':'billing-svc','name':'latency_p99_ms','value':5000},
  {'ts':'2026-04-01T10:05:00Z','kind':'incident_signal','incident_id':'INC-200','trigger':'alert:billing-svc/latency>1s'},
]
engine.ingest(events_post)

signal = {'ts':'2026-04-01T10:05:00Z','incident_id':'INC-200','trigger':'alert:billing-svc/latency>1s'}
ctx = engine.reconstruct_context(signal, mode='fast')

similar = ctx['similar_past_incidents']
print('Similar incidents found:', len(similar))
if similar:
    print('Top match:', similar[0]['past_incident_id'], 'similarity:', similar[0]['similarity'])
    assert similar[0]['past_incident_id'] == 'INC-100', 'RENAME TEST FAILED'
    print('RENAME TEST PASSED ✓')
    
remeds = ctx['suggested_remediations']
print('Suggested remediations:', len(remeds))
if remeds:
    print('Top:', remeds[0]['action'], remeds[0]['target'])
"
```

---

## Writeup outline (3 pages, PDF)

### Page 1: Memory representation + relationship synthesis
- Why temporal property graph over vector store alone
- The canonical identity layer and alias chain
- Causal edge schema and confidence model
- Behavioral fingerprint design (service-name-free)

### Page 2: Drift handling + evolution mechanism
- How topology/rename events are processed
- How identity resolution handles cascading renames
- The reinforcement loop: how remediation outcomes update confidence
- Salience decay and memory garbage collection

### Page 3: Latency engineering + what fails in the baseline
- Architecture choices that enable p95 ≤ 2s
- Pre-computation at ingest time vs query time
- Why the baseline fails (name-based retrieval fails under rename)
- Benchmark results table with improvement over baseline

---

*Built for Anvil P&E Challenge — Problem 02/04*
*Repository: https://github.com/Sauhard74/Anvil-P-E/bench-p02-context*
