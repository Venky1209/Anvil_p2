# Teammate 2: Memory Graph & Relationship Synthesis

## Your Mission
You own the **brain** — building relationships between events, fingerprinting incident patterns topology-independently, and finding similar past incidents. You read from Module A's EventStore/AliasRegistry. Module C reads from YOUR MemoryGraph.

## Branch: `feat/memory`

---

## Phase 0: Scaffold (Hour 0-1, ALL TOGETHER)
Same as Teammate 1 — everyone creates the shared structure and agrees on `engine/shared/types.py`. Then:
```bash
git checkout -b feat/memory
```

---

## Phase 1: Build Your Module (Hours 1-8)

### Step 1: `engine/memory/graph.py` — Core Graph

```python
from collections import defaultdict
from engine.shared.types import NormalizedEvent, MemoryEdge

class TemporalGraph:
    """In-memory graph with temporal edges between events."""

    def __init__(self):
        self._edges: list[MemoryEdge] = []
        self._adjacency: dict[str, list[MemoryEdge]] = defaultdict(list)
        self._reverse: dict[str, list[MemoryEdge]] = defaultdict(list)

    def add_edge(self, edge: MemoryEdge) -> None:
        self._edges.append(edge)
        self._adjacency[edge['source_event_id']].append(edge)
        self._reverse[edge['target_event_id']].append(edge)

    def get_outgoing(self, event_id: str) -> list[MemoryEdge]:
        return self._adjacency.get(event_id, [])

    def get_incoming(self, event_id: str) -> list[MemoryEdge]:
        return self._reverse.get(event_id, [])

    def get_all_edges(self) -> list[MemoryEdge]:
        return self._edges

    def get_connected_component(self, event_id: str, max_depth: int = 5) -> set[str]:
        """BFS to find all connected event IDs within max_depth."""
        visited = set()
        queue = [(event_id, 0)]
        while queue:
            current, depth = queue.pop(0)
            if current in visited or depth > max_depth:
                continue
            visited.add(current)
            for edge in self.get_outgoing(current) + self.get_incoming(current):
                neighbor = edge['target_event_id'] if edge['source_event_id'] == current else edge['source_event_id']
                queue.append((neighbor, depth + 1))
        return visited
```

### Step 2: `engine/memory/edge_builder.py` — Relationship Synthesis

This is where the MAGIC happens. You create edges from patterns in the data.

```python
from engine.shared.types import NormalizedEvent, MemoryEdge
from engine.memory.graph import TemporalGraph

class EdgeBuilder:
    """Synthesize relationships between events using rules."""

    def __init__(self, graph: TemporalGraph):
        self.graph = graph

    def build_edges(self, events: list[NormalizedEvent]) -> None:
        """Main entry: analyze events and create all edges."""
        sorted_events = sorted(events, key=lambda e: e['ts'])

        self._build_deploy_to_metric_edges(sorted_events)
        self._build_trace_edges(sorted_events)
        self._build_deploy_to_error_edges(sorted_events)
        self._build_incident_to_remediation_edges(sorted_events)
        self._build_temporal_proximity_edges(sorted_events)

    def _build_deploy_to_metric_edges(self, events):
        """Deploy followed by metric spike on same service = likely causal."""
        deploys = [e for e in events if e['kind'] == 'deploy']
        metrics = [e for e in events if e['kind'] == 'metric']

        for deploy in deploys:
            svc = deploy['canonical_service']
            deploy_ts = deploy['ts']
            # Find metrics on same service within 30 min window after deploy
            for metric in metrics:
                if (metric['canonical_service'] == svc and
                    metric['ts'] > deploy_ts and
                    self._within_window(deploy_ts, metric['ts'], minutes=30)):
                    # Check if metric value is anomalous (simple: > 1000ms for latency)
                    value = metric['data'].get('value', 0)
                    if value > 1000:  # latency spike threshold
                        self.graph.add_edge(MemoryEdge(
                            source_event_id=deploy['id'],
                            target_event_id=metric['id'],
                            relationship='caused_by',
                            confidence=0.7,
                            evidence=f"Deploy {deploy['data'].get('version','')} preceded latency spike ({value}ms)",
                            timestamp=metric['ts'],
                        ))

    def _build_trace_edges(self, events):
        """Events sharing a trace_id are linked."""
        by_trace = {}
        for e in events:
            tid = e.get('trace_id')
            if tid:
                by_trace.setdefault(tid, []).append(e)

        for trace_id, trace_events in by_trace.items():
            trace_events.sort(key=lambda e: e['ts'])
            for i in range(len(trace_events) - 1):
                self.graph.add_edge(MemoryEdge(
                    source_event_id=trace_events[i]['id'],
                    target_event_id=trace_events[i+1]['id'],
                    relationship='trace_linked',
                    confidence=0.9,
                    evidence=f"Same trace: {trace_id}",
                    timestamp=trace_events[i+1]['ts'],
                ))

    def _build_deploy_to_error_edges(self, events):
        """Deploy followed by error logs on same/dependent service."""
        deploys = [e for e in events if e['kind'] == 'deploy']
        errors = [e for e in events if e['kind'] == 'log' and e['data'].get('level') == 'error']

        for deploy in deploys:
            svc = deploy['canonical_service']
            for error in errors:
                if (error['ts'] > deploy['ts'] and
                    self._within_window(deploy['ts'], error['ts'], minutes=30)):
                    # Same service or mentions the service
                    if (error['canonical_service'] == svc or
                        svc in error['data'].get('msg', '')):
                        self.graph.add_edge(MemoryEdge(
                            source_event_id=deploy['id'],
                            target_event_id=error['id'],
                            relationship='caused_by',
                            confidence=0.6,
                            evidence=f"Deploy preceded error: {error['data'].get('msg','')}",
                            timestamp=error['ts'],
                        ))

    def _build_incident_to_remediation_edges(self, events):
        """Link incidents to their remediations."""
        by_incident = {}
        for e in events:
            iid = e.get('incident_id')
            if iid:
                by_incident.setdefault(iid, []).append(e)

        for iid, ievents in by_incident.items():
            signals = [e for e in ievents if e['kind'] == 'incident_signal']
            remediations = [e for e in ievents if e['kind'] == 'remediation']
            for sig in signals:
                for rem in remediations:
                    self.graph.add_edge(MemoryEdge(
                        source_event_id=sig['id'],
                        target_event_id=rem['id'],
                        relationship='remediated_by',
                        confidence=0.95,
                        evidence=f"Remediation {rem['data'].get('action','')} for {iid}",
                        timestamp=rem['ts'],
                    ))

    def _build_temporal_proximity_edges(self, events):
        """Events on same service within tight window are co-occurrent."""
        by_service = {}
        for e in events:
            svc = e['canonical_service']
            if svc:
                by_service.setdefault(svc, []).append(e)

        for svc, svc_events in by_service.items():
            svc_events.sort(key=lambda e: e['ts'])
            for i in range(len(svc_events) - 1):
                if self._within_window(svc_events[i]['ts'], svc_events[i+1]['ts'], minutes=5):
                    if svc_events[i]['kind'] != svc_events[i+1]['kind']:
                        self.graph.add_edge(MemoryEdge(
                            source_event_id=svc_events[i]['id'],
                            target_event_id=svc_events[i+1]['id'],
                            relationship='co_occurred',
                            confidence=0.4,
                            evidence="Temporal proximity on same service",
                            timestamp=svc_events[i+1]['ts'],
                        ))

    def _within_window(self, ts1: str, ts2: str, minutes: int) -> bool:
        from datetime import datetime, timedelta
        try:
            t1 = datetime.fromisoformat(ts1.replace('Z', '+00:00'))
            t2 = datetime.fromisoformat(ts2.replace('Z', '+00:00'))
            return abs(t2 - t1) <= timedelta(minutes=minutes)
        except:
            return False
```

### Step 3: `engine/memory/fingerprinter.py` — Behavioral Fingerprinting

**THE WINNING DIFFERENTIATOR.** This creates topology-independent signatures.

```python
from collections import Counter
from engine.shared.types import NormalizedEvent, IncidentFingerprint

class IncidentFingerprinter:
    """Create topology-independent behavioral signatures for incidents."""

    def __init__(self):
        self._incident_history: dict[str, IncidentFingerprint] = {}
        self._family_counter = 0

    def fingerprint(self, incident_id: str, related_events: list[NormalizedEvent]) -> IncidentFingerprint:
        """Create a behavioral fingerprint from an incident's events.

        Key insight: we use event KIND SEQUENCES, not service names.
        This makes the fingerprint survive renames.
        """
        sorted_events = sorted(related_events, key=lambda e: e['ts'])

        # Pattern = ordered sequence of event kinds
        pattern = [e['kind'] for e in sorted_events]

        # Count unique canonical services (the NUMBER, not the names)
        services = set(e['canonical_service'] for e in sorted_events if e['canonical_service'])

        # Find remediation action if any
        remediations = [e for e in sorted_events if e['kind'] == 'remediation']
        typical_rem = remediations[0]['data'].get('action', 'unknown') if remediations else 'unknown'

        fp = IncidentFingerprint(
            family_id=self._assign_family(pattern),
            pattern=pattern,
            services_involved=len(services),
            typical_remediation=typical_rem,
            occurrences=1,
        )

        self._incident_history[incident_id] = fp
        return fp

    def _assign_family(self, pattern: list[str]) -> str:
        """Assign a family ID based on pattern similarity."""
        # Simplified: hash the pattern to find family
        pattern_key = self._pattern_to_key(pattern)
        for iid, fp in self._incident_history.items():
            existing_key = self._pattern_to_key(fp['pattern'])
            if self._patterns_similar(pattern_key, existing_key):
                return fp['family_id']

        self._family_counter += 1
        return f"family-{self._family_counter}"

    def _pattern_to_key(self, pattern: list[str]) -> tuple:
        """Convert pattern to a comparable key (kind sequence without timestamps)."""
        # Keep unique ordered kinds
        seen = []
        for k in pattern:
            if k not in seen:
                seen.append(k)
        return tuple(seen)

    def _patterns_similar(self, p1: tuple, p2: tuple) -> bool:
        """Check if two patterns are similar enough to be same family."""
        if p1 == p2:
            return True
        # Jaccard similarity on kind sets
        s1, s2 = set(p1), set(p2)
        if not s1 or not s2:
            return False
        jaccard = len(s1 & s2) / len(s1 | s2)
        return jaccard >= 0.6

    def get_fingerprint(self, incident_id: str) -> IncidentFingerprint:
        return self._incident_history.get(incident_id)

    def get_all_fingerprints(self) -> dict[str, IncidentFingerprint]:
        return self._incident_history
```

### Step 4: `engine/memory/similarity.py` — Incident Matching

```python
from engine.shared.types import IncidentFingerprint

class IncidentMatcher:
    """Find similar past incidents using fingerprints, not service names."""

    def find_similar(self, current_fp: IncidentFingerprint,
                     history: dict[str, IncidentFingerprint],
                     current_id: str, top_k: int = 5) -> list[tuple[str, float, str]]:
        """Returns list of (past_incident_id, similarity_score, rationale)."""
        results = []

        for past_id, past_fp in history.items():
            if past_id == current_id:
                continue

            sim = self._compute_similarity(current_fp, past_fp)
            if sim > 0.3:
                rationale = self._build_rationale(current_fp, past_fp, sim)
                results.append((past_id, sim, rationale))

        results.sort(key=lambda x: x[1], reverse=True)
        return results[:top_k]

    def _compute_similarity(self, fp1: IncidentFingerprint, fp2: IncidentFingerprint) -> float:
        """Multi-factor similarity score."""
        score = 0.0

        # Pattern similarity (most important — 50%)
        p1 = set(fp1['pattern'])
        p2 = set(fp2['pattern'])
        if p1 and p2:
            jaccard = len(p1 & p2) / len(p1 | p2)
            score += 0.5 * jaccard

        # Same remediation type (25%)
        if fp1['typical_remediation'] == fp2['typical_remediation']:
            score += 0.25

        # Similar service count (15%)
        count_diff = abs(fp1['services_involved'] - fp2['services_involved'])
        if count_diff == 0:
            score += 0.15
        elif count_diff == 1:
            score += 0.08

        # Same family (10% bonus)
        if fp1['family_id'] == fp2['family_id']:
            score += 0.10

        return min(score, 1.0)

    def _build_rationale(self, fp1, fp2, sim) -> str:
        shared = set(fp1['pattern']) & set(fp2['pattern'])
        return (f"Similar behavioral pattern (score={sim:.2f}). "
                f"Shared event types: {', '.join(shared)}. "
                f"Both involved {fp1['services_involved']}/{fp2['services_involved']} services. "
                f"Remediation: {fp2['typical_remediation']}.")
```

### Step 5: `engine/memory/decay.py`

```python
from datetime import datetime, timedelta
from engine.shared.types import MemoryEdge

class DecayManager:
    """Apply temporal decay to edge confidence."""

    def __init__(self, half_life_days: int = 7):
        self.half_life = timedelta(days=half_life_days)

    def apply_decay(self, edge: MemoryEdge, current_time: str) -> float:
        try:
            edge_time = datetime.fromisoformat(edge['timestamp'].replace('Z', '+00:00'))
            now = datetime.fromisoformat(current_time.replace('Z', '+00:00'))
            age = now - edge_time
            decay_factor = 0.5 ** (age / self.half_life)
            return edge['confidence'] * decay_factor
        except:
            return edge['confidence']

    def reinforce(self, edge: MemoryEdge, boost: float = 0.1) -> float:
        """Reinforce confidence when a pattern is confirmed."""
        return min(edge['confidence'] + boost, 1.0)
```

### Step 6: Main `engine/memory/__init__.py` — MemoryGraph Facade

```python
from engine.shared.types import NormalizedEvent, MemoryEdge, IncidentFingerprint
from engine.memory.graph import TemporalGraph
from engine.memory.edge_builder import EdgeBuilder
from engine.memory.fingerprinter import IncidentFingerprinter
from engine.memory.similarity import IncidentMatcher
from engine.memory.decay import DecayManager

class MemoryGraph:
    """Facade for the entire memory subsystem."""

    def __init__(self):
        self.graph = TemporalGraph()
        self.edge_builder = EdgeBuilder(self.graph)
        self.fingerprinter = IncidentFingerprinter()
        self.matcher = IncidentMatcher()
        self.decay = DecayManager()
        self._events: list[NormalizedEvent] = []

    def add_events(self, events: list[NormalizedEvent]) -> None:
        self._events.extend(events)
        self.edge_builder.build_edges(events)

    def get_causal_chain(self, incident_id: str, event_store) -> list[MemoryEdge]:
        incident_events = event_store.query_by_incident(incident_id)
        if not incident_events:
            return []
        chain = []
        visited = set()
        for ie in incident_events:
            self._traverse_causes(ie['id'], chain, visited, max_depth=5)
        chain.sort(key=lambda e: e['timestamp'])
        return chain

    def _traverse_causes(self, event_id, chain, visited, max_depth, depth=0):
        if depth > max_depth or event_id in visited:
            return
        visited.add(event_id)
        for edge in self.graph.get_incoming(event_id):
            if edge['relationship'] in ('caused_by', 'trace_linked'):
                chain.append(edge)
                self._traverse_causes(edge['source_event_id'], chain, visited, max_depth, depth+1)

    def find_similar_incidents(self, incident_id: str, related_events: list[NormalizedEvent], top_k=5):
        fp = self.fingerprinter.fingerprint(incident_id, related_events)
        return self.matcher.find_similar(fp, self.fingerprinter.get_all_fingerprints(), incident_id, top_k)

    def get_fingerprint(self, incident_id):
        return self.fingerprinter.get_fingerprint(incident_id)

    def get_related_event_ids(self, event_ids: list[str], max_depth=3) -> set[str]:
        all_related = set()
        for eid in event_ids:
            all_related |= self.graph.get_connected_component(eid, max_depth)
        return all_related

    def get_remediations(self, incident_id: str, event_store) -> list[dict]:
        events = event_store.query_by_incident(incident_id)
        return [e['data'] for e in events if e['kind'] == 'remediation']
```

### Step 7: Tests — `tests/test_memory.py`

```python
from engine.shared.types import NormalizedEvent, MemoryEdge
from engine.memory import MemoryGraph

def make_event(id, ts, kind, svc, **kwargs):
    return NormalizedEvent(id=id, ts=ts, kind=kind, canonical_service=svc,
                          raw_service=svc, data=kwargs, trace_id=kwargs.get('trace_id'),
                          incident_id=kwargs.get('incident_id'))

def test_edge_building():
    mg = MemoryGraph()
    events = [
        make_event("d1", "2026-05-10T14:21:30Z", "deploy", "payments-svc", version="v2.14.0"),
        make_event("m1", "2026-05-10T14:22:01Z", "metric", "payments-svc", name="latency_p99_ms", value=4820),
        make_event("l1", "2026-05-10T14:22:01Z", "log", "checkout-api", level="error",
                   msg="timeout calling payments-svc", trace_id="abc123"),
    ]
    mg.add_events(events)
    edges = mg.graph.get_all_edges()
    assert len(edges) > 0, "Should create edges from deploy+metric pattern"

def test_fingerprint_similarity():
    mg = MemoryGraph()
    # Two incidents with same pattern but different services
    events1 = [
        make_event("a1", "T1", "deploy", "svc-a", incident_id="INC-1"),
        make_event("a2", "T2", "metric", "svc-a", value=5000, incident_id="INC-1"),
        make_event("a3", "T3", "remediation", "svc-a", action="rollback", incident_id="INC-1"),
    ]
    events2 = [
        make_event("b1", "T4", "deploy", "svc-b", incident_id="INC-2"),
        make_event("b2", "T5", "metric", "svc-b", value=6000, incident_id="INC-2"),
        make_event("b3", "T6", "remediation", "svc-b", action="rollback", incident_id="INC-2"),
    ]
    mg.fingerprinter.fingerprint("INC-1", events1)
    fp2 = mg.fingerprinter.fingerprint("INC-2", events2)
    matches = mg.matcher.find_similar(fp2, mg.fingerprinter.get_all_fingerprints(), "INC-2")
    assert len(matches) > 0, "Should find INC-1 as similar"
    assert matches[0][0] == "INC-1"

if __name__ == "__main__":
    test_edge_building()
    test_fingerprint_similarity()
    print("ALL MEMORY TESTS PASSED")
```

---

## Phase 2: Integration Handoff (Hour 8-10)

Your module is done when:
- [ ] EdgeBuilder creates edges from deploy→metric, traces, deploy→error, incident→remediation
- [ ] Fingerprinter creates topology-independent signatures
- [ ] Matcher finds similar incidents across different services
- [ ] All tests pass
- [ ] Push to `feat/memory`, PR to main

**What you need from Module A:** `EventStore` and `AliasRegistry` instances
**What Module C needs from you:** `MemoryGraph` instance with all methods above
