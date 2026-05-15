"""
Event Store — DuckDB-backed event storage with multi-index querying.

Replaces the naive dict-based store with DuckDB for:
  - Analytical range queries over 200k+ events
  - Time-windowed queries (last N minutes for service X)
  - Cross-index queries (service + kind + time range)
  - Persistent storage across engine restarts

Also maintains an in-memory hot cache for the last 1000 events
for sub-millisecond access during fast-mode context reconstruction.
"""

from collections import defaultdict
from typing import Optional
from engine.shared.types import NormalizedEvent
from engine.ingestion.storage import DuckDBStorage
from engine.ingestion.parser import classify_metric


class EventStore:
    """
    DuckDB-backed event store with in-memory hot cache.

    Write path: event → DuckDB + hot cache
    Read path: hot cache first → DuckDB fallback
    """

    # Max events in hot cache
    HOT_CACHE_SIZE = 2000

    def __init__(self, storage: DuckDBStorage):
        self.storage = storage
        # In-memory hot cache for fast-mode queries
        self._hot_events: list[NormalizedEvent] = []
        self._hot_by_service: dict[str, list[NormalizedEvent]] = defaultdict(list)
        self._hot_by_kind: dict[str, list[NormalizedEvent]] = defaultdict(list)
        self._hot_by_trace: dict[str, list[NormalizedEvent]] = defaultdict(list)
        self._hot_by_incident: dict[str, list[NormalizedEvent]] = defaultdict(list)
        self._hot_by_id: dict[str, NormalizedEvent] = {}

    def append(self, event: NormalizedEvent) -> None:
        """
        Store a normalized event in DuckDB and hot cache.
        """
        # Write to DuckDB
        metric_name = None
        metric_value = None
        metric_source = None
        raw_data = event.get('data', {})

        if event['kind'] == 'metric':
            metric_name = raw_data.get('name', '')
            metric_value = raw_data.get('value')
            metric_source = classify_metric(metric_name)

        self.storage.insert_event(
            event_id=event['id'],
            canonical_id=event['canonical_service'],
            kind=event['kind'],
            ts=event['ts'],
            raw_service=event['raw_service'],
            raw_json=raw_data,
            metric_name=metric_name,
            metric_value=metric_value,
            metric_source=metric_source,
            trace_id=event.get('trace_id'),
            incident_id=event.get('incident_id'),
        )

        # Write to hot cache
        self._hot_events.append(event)
        self._hot_by_id[event['id']] = event

        if event['canonical_service']:
            self._hot_by_service[event['canonical_service']].append(event)
        self._hot_by_kind[event['kind']].append(event)
        if event.get('trace_id'):
            self._hot_by_trace[event['trace_id']].append(event)
        if event.get('incident_id'):
            self._hot_by_incident[event['incident_id']].append(event)

        # Evict oldest from hot cache if over limit
        if len(self._hot_events) > self.HOT_CACHE_SIZE:
            self._evict_cold()

    def _evict_cold(self) -> None:
        """Evict the oldest half of hot cache entries."""
        cutoff = len(self._hot_events) // 2
        evicted = self._hot_events[:cutoff]
        self._hot_events = self._hot_events[cutoff:]

        # Remove from indexes
        evicted_ids = {e['id'] for e in evicted}
        for eid in evicted_ids:
            self._hot_by_id.pop(eid, None)

        # Rebuild service/kind indexes from remaining hot events
        self._hot_by_service.clear()
        self._hot_by_kind.clear()
        self._hot_by_trace.clear()
        self._hot_by_incident.clear()
        for e in self._hot_events:
            if e['canonical_service']:
                self._hot_by_service[e['canonical_service']].append(e)
            self._hot_by_kind[e['kind']].append(e)
            if e.get('trace_id'):
                self._hot_by_trace[e['trace_id']].append(e)
            if e.get('incident_id'):
                self._hot_by_incident[e['incident_id']].append(e)

    # ------------------------------------------------------------------
    # Query methods — hot cache first, DuckDB fallback
    # ------------------------------------------------------------------

    def get_by_id(self, event_id: str) -> Optional[NormalizedEvent]:
        """Get a single event by ID."""
        # Hot cache first
        if event_id in self._hot_by_id:
            return self._hot_by_id[event_id]
        # DuckDB fallback
        row = self.storage.get_event_by_id(event_id)
        if row:
            return self._row_to_normalized(row)
        return None

    def query_by_service(
        self, canonical_id: str, start: Optional[str] = None, end: Optional[str] = None
    ) -> list[NormalizedEvent]:
        """Get all events for a canonical service, optionally filtered by time."""
        rows = self.storage.query_events(
            canonical_id=canonical_id, start_ts=start, end_ts=end
        )
        return [self._row_to_normalized(r) for r in rows]

    def query_by_kind(
        self, kind: str, start: Optional[str] = None, end: Optional[str] = None
    ) -> list[NormalizedEvent]:
        """Get all events of a given kind."""
        rows = self.storage.query_events(kind=kind, start_ts=start, end_ts=end)
        return [self._row_to_normalized(r) for r in rows]

    def query_by_trace(self, trace_id: str) -> list[NormalizedEvent]:
        """Get all events for a trace."""
        rows = self.storage.query_events(trace_id=trace_id)
        return [self._row_to_normalized(r) for r in rows]

    def query_by_incident(self, incident_id: str) -> list[NormalizedEvent]:
        """Get all events for an incident."""
        rows = self.storage.query_events(incident_id=incident_id)
        return [self._row_to_normalized(r) for r in rows]

    def get_all(
        self, start: Optional[str] = None, end: Optional[str] = None, limit: int = 1000
    ) -> list[NormalizedEvent]:
        """Get all events, optionally filtered by time range."""
        rows = self.storage.query_events(start_ts=start, end_ts=end, limit=limit)
        return [self._row_to_normalized(r) for r in rows]

    def get_recent_for_service(
        self, canonical_id: str, limit: int = 200
    ) -> list[NormalizedEvent]:
        """Get the most recent events for a service (fast mode hot path)."""
        # Try hot cache first
        hot = self._hot_by_service.get(canonical_id, [])
        if len(hot) >= limit:
            return hot[-limit:]
        # DuckDB fallback
        rows = self.storage.query_events(canonical_id=canonical_id, limit=limit)
        return [self._row_to_normalized(r) for r in rows]

    def count(self) -> int:
        """Total events stored."""
        return self.storage.count_events()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _row_to_normalized(self, row: dict) -> NormalizedEvent:
        """Convert a DuckDB row back to NormalizedEvent format."""
        raw_json = row.get('raw_json', {})
        return NormalizedEvent(
            id=row['event_id'],
            ts=row['ts'],
            kind=row['kind'],
            canonical_service=row.get('canonical_id', ''),
            raw_service=row.get('raw_service', ''),
            data=raw_json,
            trace_id=row.get('trace_id'),
            incident_id=row.get('incident_id'),
        )
