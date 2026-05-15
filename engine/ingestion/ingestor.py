"""
Ingestor — The main orchestrator for the ingestion pipeline.

This is the single entry point for all telemetry. It wires together:
  - IdentityResolver (canonical ID resolution)
  - EventNormalizer (raw → normalized)
  - EventStore (DuckDB-backed storage + hot cache)
  - CausalEdgeDetector (relationship synthesis at ingest time)
  - Incident episode tracking

Processing pipeline for each event:
  1. Parse + validate
  2. Normalize (resolve canonical ID, classify metrics)
  3. Store in DuckDB + hot cache
  4. Detect causal edges (deploy attribution, trace linkage, etc.)
  5. Update incident episode state (if incident_signal or remediation)
  6. Update metric baselines (if metric)

This class is what the Engine adapter calls via ingest().
"""

import json
import time
from typing import Iterable, Optional
from datetime import datetime, timedelta

from engine.ingestion.storage import DuckDBStorage
from engine.ingestion.identity import IdentityResolver
from engine.ingestion.normalizer import EventNormalizer
from engine.ingestion.event_store import EventStore
from engine.ingestion.causal import CausalEdgeDetector
from engine.ingestion.parser import (
    parse_event,
    generate_event_id,
    classify_metric,
    extract_services_from_trace,
)


class Ingestor:
    """
    Main ingestion orchestrator.

    Usage:
        storage = DuckDBStorage()
        ingestor = Ingestor(storage)
        ingestor.ingest(events)

    After ingestion, downstream modules (memory, compiler) read from
    the same DuckDBStorage instance.
    """

    def __init__(self, storage: Optional[DuckDBStorage] = None, db_path: str = ":memory:"):
        # Storage layer — shared with all other modules
        self.storage = storage or DuckDBStorage(db_path)

        # Identity resolver — the foundation
        self.identity = IdentityResolver(self.storage)

        # Normalizer — bridges raw events to canonical IDs
        self.normalizer = EventNormalizer(self.identity)

        # Event store — DuckDB + hot cache
        self.event_store = EventStore(self.storage)

        # Causal edge detector — builds relationships at ingest time
        self.causal = CausalEdgeDetector(self.storage, self.identity)

        # Active incident tracking
        self._active_incidents: dict[str, dict] = {}  # incident_id → episode info

        # Ingestion stats
        self._ingest_count = 0
        self._ingest_errors = 0
        self._last_ingest_ts: Optional[str] = None

    def ingest(self, events: Iterable[dict]) -> dict:
        """
        Ingest a stream of raw events.

        Processes each event through the full pipeline:
        parse → normalize → store → causal detect → episode track.

        Returns ingestion stats.
        """
        start_time = time.monotonic()
        count = 0
        errors = 0
        causal_edges_created = 0

        for raw_event in events:
            try:
                self._ingest_one(raw_event)
                count += 1
            except Exception as e:
                errors += 1
                # Log but don't crash — ingestion must be resilient
                if errors <= 10:
                    import sys
                    print(f"[ingestor] Error processing event: {e}", file=sys.stderr)

        elapsed = time.monotonic() - start_time
        self._ingest_count += count
        self._ingest_errors += errors

        return {
            'events_processed': count,
            'errors': errors,
            'elapsed_seconds': round(elapsed, 3),
            'events_per_second': round(count / elapsed, 1) if elapsed > 0 else 0,
            'total_events': self._ingest_count,
            'total_errors': self._ingest_errors,
        }

    def _ingest_one(self, raw_event: dict) -> None:
        """Process a single raw event through the full pipeline."""
        # Step 1: Parse and validate
        parsed = parse_event(raw_event)

        # Step 2: Normalize (resolves canonical ID)
        normalized = self.normalizer.normalize(parsed)

        # Step 3: Store in DuckDB + hot cache
        self.event_store.append(normalized)

        # Step 4: Detect causal edges
        new_edges = self.causal.on_event(
            event_id=normalized['id'],
            event=parsed,
            canonical_id=normalized['canonical_service'],
        )

        # Step 5: Handle incident lifecycle
        kind = parsed.get('kind', '')
        if kind == 'incident_signal':
            self._handle_incident_signal(normalized, parsed)
        elif kind == 'remediation':
            self._handle_remediation(normalized, parsed, new_edges)

        # Step 6: Track last ingest timestamp
        self._last_ingest_ts = normalized['ts']

    def _handle_incident_signal(self, normalized: dict, raw_event: dict) -> None:
        """
        Process an incident signal — start tracking an incident episode.

        Looks back in time to find related events for this incident.
        """
        incident_id = raw_event.get('incident_id', '')
        if not incident_id:
            return

        ts = normalized['ts']
        canonical_id = normalized['canonical_service']

        # Find related events by looking back in time
        # Get events for the canonical service in the last 10 minutes
        related_cids = set()
        if canonical_id:
            related_cids.add(canonical_id)

        # Also check the trigger for service mentions
        trigger = raw_event.get('trigger', '')
        trigger_service = self.normalizer._extract_service_from_trigger(trigger)
        if trigger_service:
            trigger_cid = self.identity.resolve(trigger_service, ts)
            related_cids.add(trigger_cid)

        # Collect event IDs from the lookback window
        lookback_events = []
        for cid in related_cids:
            events = self.event_store.query_by_service(cid)
            # Filter to recent events (within 15 minutes before signal)
            for e in events:
                if e['ts'] <= ts:
                    lookback_events.append(e)

        # Get causal edges involving these events
        event_ids = [e['id'] for e in lookback_events]
        causal_edges = self.storage.get_causal_edges_for_events(event_ids)

        # Track the episode
        episode = {
            'incident_id': incident_id,
            'canonical_ids': list(related_cids),
            'ts_start': lookback_events[0]['ts'] if lookback_events else ts,
            'ts_incident_signal': ts,
            'ts_resolved': None,
            'remediation_action': None,
            'remediation_target': None,
            'remediation_outcome': None,
            'remediation_confidence': 0.5,
            'tier': 'hot',
            'last_accessed_ts': ts,
            'event_ids': event_ids[-50:],  # Keep last 50 event IDs
            'causal_chain_ids': [e['edge_id'] for e in causal_edges],
            'fingerprint_vec': None,  # Computed by the memory module
            'family_id': None,
        }

        self._active_incidents[incident_id] = episode
        self.storage.upsert_incident_episode(episode)

    def _handle_remediation(
        self, normalized: dict, raw_event: dict, new_edges: list[dict]
    ) -> None:
        """
        Process a remediation event — close the incident episode.

        Updates the episode with remediation details and reinforces
        causal edges if the remediation was successful.
        """
        incident_id = raw_event.get('incident_id', '')
        if not incident_id:
            return

        ts = normalized['ts']
        action = raw_event.get('action', '')
        target = raw_event.get('target', '')
        outcome = raw_event.get('outcome', '')

        # Resolve the remediation target to current name
        if target:
            target_cid = self.identity.resolve(target, ts)
            target_current_name = self.identity.get_current_name(target_cid)
        else:
            target_current_name = target

        # Update the episode
        episode = self._active_incidents.get(incident_id)
        if episode is None:
            # Remediation without a signal — create a minimal episode
            episode = {
                'incident_id': incident_id,
                'canonical_ids': [normalized['canonical_service']] if normalized['canonical_service'] else [],
                'ts_start': ts,
                'ts_incident_signal': ts,
                'event_ids': [],
                'causal_chain_ids': [],
                'fingerprint_vec': None,
                'family_id': None,
            }

        episode['ts_resolved'] = ts
        episode['remediation_action'] = action
        episode['remediation_target'] = target_current_name
        episode['remediation_outcome'] = outcome
        episode['last_accessed_ts'] = ts

        # Compute remediation confidence based on outcome
        if outcome == 'resolved':
            episode['remediation_confidence'] = 0.8
            episode['tier'] = 'warm'
            # Reinforce causal edges for successful remediation
            self.causal.reinforce_edges_for_incident(incident_id, boost=0.1)
        elif outcome == 'failed':
            episode['remediation_confidence'] = 0.2
            episode['tier'] = 'warm'
        else:
            episode['remediation_confidence'] = 0.5
            episode['tier'] = 'warm'

        self.storage.upsert_incident_episode(episode)

        # Remove from active tracking
        self._active_incidents.pop(incident_id, None)

    # ------------------------------------------------------------------
    # Public accessors for downstream modules
    # ------------------------------------------------------------------

    def get_event_store(self) -> EventStore:
        """Get the event store (for compiler module)."""
        return self.event_store

    def get_identity(self) -> IdentityResolver:
        """Get the identity resolver (for compiler module)."""
        return self.identity

    def get_storage(self) -> DuckDBStorage:
        """Get the DuckDB storage (for memory/compiler modules)."""
        return self.storage

    def get_causal_detector(self) -> CausalEdgeDetector:
        """Get the causal edge detector."""
        return self.causal

    def stats(self) -> dict:
        """Comprehensive ingestion stats."""
        return {
            'ingestion': {
                'total_events': self._ingest_count,
                'total_errors': self._ingest_errors,
                'last_ts': self._last_ingest_ts,
            },
            'storage': self.storage.stats(),
            'identity': self.identity.stats(),
            'active_incidents': len(self._active_incidents),
        }

    def close(self) -> None:
        """Clean shutdown."""
        self.storage.close()
