# Teammate 3: Context Compiler & Reconstruction

## Your Mission
You own the **output** — when an incident signal arrives, you reconstruct the full `Context` object that the benchmark scores. You read from Module A's EventStore/AliasRegistry and Module B's MemoryGraph. Your output MUST match the benchmark TypedDict exactly.

## Branch: `feat/compiler`

---

## Phase 0: Scaffold (Hour 0-1, ALL TOGETHER)
Same shared setup. Then:
```bash
git checkout -b feat/compiler
```

---

## Phase 1: Build Your Module (Hours 1-8)

### Step 1: `engine/compiler/reconstructor.py` — Main Entry Point

```python
from engine.shared.types import Context, IncidentSignal, NormalizedEvent
from engine.compiler.causal_builder import CausalChainBuilder
from engine.compiler.matcher import PastIncidentMatcher
from engine.compiler.ranker import EventRanker
from engine.compiler.remediator import RemediationSuggester
from engine.compiler.explainer import NarrativeExplainer

class ContextCompiler:
    def __init__(self, event_store, alias_registry, memory_graph):
        self.store = event_store
        self.aliases = alias_registry
        self.memory = memory_graph
        self.causal = CausalChainBuilder(memory_graph, event_store)
        self.matcher = PastIncidentMatcher(memory_graph, event_store)
        self.ranker = EventRanker(event_store, alias_registry)
        self.remediator = RemediationSuggester(memory_graph, event_store)
        self.explainer = NarrativeExplainer(alias_registry)

    def reconstruct(self, signal: IncidentSignal, mode: str = "fast") -> Context:
        incident_id = signal['incident_id']
        trigger_ts = signal['ts']

        # 1. Find related events
        max_depth = 3 if mode == "fast" else 6
        time_window_mins = 30 if mode == "fast" else 120
        related = self.ranker.find_related_events(
            incident_id, trigger_ts, max_depth, time_window_mins
        )

        # 2. Build causal chain
        causal_chain = self.causal.build(incident_id, related, mode)

        # 3. Find similar past incidents
        similar = self.matcher.find_similar(incident_id, related)

        # 4. Suggest remediations
        remediations = self.remediator.suggest(incident_id, similar, related)

        # 5. Compute overall confidence
        confidence = self._compute_confidence(causal_chain, similar, related)

        # 6. Generate explanation
        explain = self.explainer.explain(
            incident_id, related, causal_chain, similar, remediations
        )

        # 7. Convert related events back to raw Event format for output
        raw_related = [e['data'] for e in related]

        return Context(
            related_events=raw_related,
            causal_chain=causal_chain,
            similar_past_incidents=similar,
            suggested_remediations=remediations,
            confidence=confidence,
            explain=explain,
        )

    def _compute_confidence(self, causal_chain, similar, related) -> float:
        score = 0.0
        if causal_chain:
            avg_conf = sum(e['confidence'] for e in causal_chain) / len(causal_chain)
            score += 0.4 * avg_conf
        if similar:
            score += 0.3 * similar[0]['similarity']
        if related:
            score += 0.3 * min(len(related) / 5, 1.0)
        return round(min(score, 1.0), 3)
```

### Step 2: `engine/compiler/causal_builder.py`

```python
from engine.shared.types import CausalEdge, NormalizedEvent

class CausalChainBuilder:
    def __init__(self, memory_graph, event_store):
        self.memory = memory_graph
        self.store = event_store

    def build(self, incident_id: str, related_events: list[NormalizedEvent],
              mode: str) -> list[CausalEdge]:
        """Build causal chain from memory graph edges."""
        chain_edges = self.memory.get_causal_chain(incident_id, self.store)

        causal_chain = []
        seen = set()

        for edge in chain_edges:
            key = (edge['source_event_id'], edge['target_event_id'])
            if key in seen:
                continue
            seen.add(key)

            if edge['relationship'] in ('caused_by', 'trace_linked', 'preceded'):
                causal_chain.append(CausalEdge(
                    cause_id=edge['source_event_id'],
                    effect_id=edge['target_event_id'],
                    evidence=edge['evidence'],
                    confidence=edge['confidence'],
                ))

        # Sort by timestamp order (cause should precede effect)
        causal_chain.sort(key=lambda e: e['confidence'], reverse=True)

        # Limit based on mode
        limit = 10 if mode == "fast" else 25
        return causal_chain[:limit]
```

### Step 3: `engine/compiler/matcher.py`

```python
from engine.shared.types import IncidentMatch, NormalizedEvent

class PastIncidentMatcher:
    def __init__(self, memory_graph, event_store):
        self.memory = memory_graph
        self.store = event_store

    def find_similar(self, incident_id: str,
                     related_events: list[NormalizedEvent]) -> list[IncidentMatch]:
        """Find similar past incidents using topology-independent fingerprints."""
        matches = self.memory.find_similar_incidents(incident_id, related_events, top_k=5)

        return [
            IncidentMatch(
                past_incident_id=past_id,
                similarity=round(sim, 3),
                rationale=rationale,
            )
            for past_id, sim, rationale in matches
        ]
```

### Step 4: `engine/compiler/ranker.py`

```python
from datetime import datetime, timedelta
from engine.shared.types import NormalizedEvent

class EventRanker:
    def __init__(self, event_store, alias_registry):
        self.store = event_store
        self.aliases = alias_registry

    def find_related_events(self, incident_id: str, trigger_ts: str,
                            max_depth: int, time_window_mins: int) -> list[NormalizedEvent]:
        """Find and rank related events for an incident."""
        # 1. Get direct incident events
        direct = self.store.query_by_incident(incident_id)

        # 2. Get services involved
        services = set()
        for e in direct:
            if e['canonical_service']:
                services.add(e['canonical_service'])

        # 3. Compute time window
        try:
            ts = datetime.fromisoformat(trigger_ts.replace('Z', '+00:00'))
            start = (ts - timedelta(minutes=time_window_mins)).isoformat()
            end = (ts + timedelta(minutes=time_window_mins // 2)).isoformat()
        except:
            start, end = None, None

        # 4. Get events from same services in window
        related = list(direct)
        seen_ids = {e['id'] for e in direct}

        for svc in services:
            svc_events = self.store.query_by_service(svc, start, end)
            for e in svc_events:
                if e['id'] not in seen_ids:
                    related.append(e)
                    seen_ids.add(e['id'])

        # 5. Follow trace IDs
        for e in list(related):
            if e.get('trace_id'):
                trace_events = self.store.query_by_trace(e['trace_id'])
                for te in trace_events:
                    if te['id'] not in seen_ids:
                        related.append(te)
                        seen_ids.add(te['id'])

        # 6. Sort by relevance (direct events first, then by proximity)
        related.sort(key=lambda e: (
            0 if e.get('incident_id') == incident_id else 1,
            e['ts']
        ))

        # Deduplicate and limit
        return related[:50] if max_depth <= 3 else related[:100]
```

### Step 5: `engine/compiler/remediator.py`

```python
from engine.shared.types import Remediation, IncidentMatch, NormalizedEvent

class RemediationSuggester:
    def __init__(self, memory_graph, event_store):
        self.memory = memory_graph
        self.store = event_store

    def suggest(self, incident_id: str, similar_incidents: list[IncidentMatch],
                related_events: list[NormalizedEvent]) -> list[Remediation]:
        """Suggest remediations based on similar past incidents."""
        suggestions = []
        seen_actions = set()

        # 1. From similar past incidents
        for match in similar_incidents:
            past_rems = self.memory.get_remediations(match['past_incident_id'], self.store)
            for rem_data in past_rems:
                action = rem_data.get('action', 'unknown')
                if action not in seen_actions:
                    seen_actions.add(action)
                    suggestions.append(Remediation(
                        action=action,
                        target=rem_data.get('target', ''),
                        historical_outcome=rem_data.get('outcome', 'unknown'),
                        confidence=round(match['similarity'] * 0.9, 3),
                    ))

        # 2. From current incident's own remediations (if any already exist)
        current_rems = [e for e in related_events if e['kind'] == 'remediation']
        for rem in current_rems:
            action = rem['data'].get('action', 'unknown')
            if action not in seen_actions:
                seen_actions.add(action)
                suggestions.append(Remediation(
                    action=action,
                    target=rem['data'].get('target', rem['canonical_service']),
                    historical_outcome=rem['data'].get('outcome', 'unknown'),
                    confidence=0.8,
                ))

        suggestions.sort(key=lambda r: r['confidence'], reverse=True)
        return suggestions[:5]
```

### Step 6: `engine/compiler/explainer.py`

```python
class NarrativeExplainer:
    def __init__(self, alias_registry):
        self.aliases = alias_registry

    def explain(self, incident_id, related_events, causal_chain,
                similar_incidents, remediations) -> str:
        """Generate human-readable explanation of the reconstructed context."""
        parts = []

        # Header
        parts.append(f"## Context Reconstruction for {incident_id}\n")

        # Services involved
        services = set(e['canonical_service'] for e in related_events if e['canonical_service'])
        aliases_info = []
        for svc in services:
            all_names = self.aliases.get_all_aliases(svc)
            if len(all_names) > 1:
                aliases_info.append(f"{svc} (also known as: {', '.join(n for n in all_names if n != svc)})")
            else:
                aliases_info.append(svc)
        parts.append(f"**Services involved:** {', '.join(aliases_info)}")

        # Causal chain narrative
        if causal_chain:
            parts.append(f"\n**Causal chain ({len(causal_chain)} edges):**")
            for i, edge in enumerate(causal_chain[:5]):
                parts.append(f"  {i+1}. {edge['evidence']} (confidence: {edge['confidence']:.2f})")

        # Similar incidents
        if similar_incidents:
            parts.append(f"\n**Similar past incidents ({len(similar_incidents)} found):**")
            for match in similar_incidents[:3]:
                parts.append(f"  - {match['past_incident_id']} "
                           f"(similarity: {match['similarity']:.2f}) — {match['rationale']}")

        # Remediations
        if remediations:
            parts.append(f"\n**Suggested remediations:**")
            for rem in remediations[:3]:
                parts.append(f"  - **{rem['action']}** on {rem['target']} "
                           f"(historical outcome: {rem['historical_outcome']}, "
                           f"confidence: {rem['confidence']:.2f})")

        # Summary
        parts.append(f"\n**Summary:** Analyzed {len(related_events)} related events across "
                     f"{len(services)} services. Found {len(causal_chain)} causal relationships "
                     f"and {len(similar_incidents)} similar historical incidents.")

        return "\n".join(parts)
```

### Step 7: Tests — `tests/test_compiler.py`

```python
from engine.shared.types import IncidentSignal, NormalizedEvent
from engine.ingestion.alias_registry import AliasRegistry
from engine.ingestion.event_store import EventStore
from engine.ingestion.normalizer import EventNormalizer
from engine.memory import MemoryGraph
from engine.compiler.reconstructor import ContextCompiler

def test_full_reconstruction():
    """Test the worked example from the spec."""
    alias_reg = AliasRegistry()
    store = EventStore()
    normalizer = EventNormalizer(alias_reg)
    memory = MemoryGraph()

    # Ingest the worked example events
    raw_events = [
        {"ts": "2026-05-10T14:21:30Z", "kind": "deploy",
         "service": "payments-svc", "version": "v2.14.0", "actor": "ci"},
        {"ts": "2026-05-10T14:22:01Z", "kind": "log",
         "service": "checkout-api", "level": "error",
         "msg": "timeout calling payments-svc", "trace_id": "abc123"},
        {"ts": "2026-05-10T14:22:01Z", "kind": "metric",
         "service": "payments-svc", "name": "latency_p99_ms", "value": 4820},
        {"ts": "2026-05-10T14:22:08Z", "kind": "trace",
         "trace_id": "abc123", "spans": [
            {"svc": "checkout-api", "dur_ms": 5012},
            {"svc": "payments-svc", "dur_ms": 4980}
         ]},
        {"ts": "2026-05-10T14:30:00Z", "kind": "topology",
         "change": "rename", "from": "payments-svc", "to": "billing-svc"},
        {"ts": "2026-05-10T14:32:11Z", "kind": "incident_signal",
         "incident_id": "INC-714", "trigger": "alert/error-rate>5%"},
        {"ts": "2026-05-10T15:10:00Z", "kind": "remediation",
         "incident_id": "INC-714", "action": "rollback",
         "target": "billing-svc", "version": "v2.13.4", "outcome": "resolved"},
    ]

    normalized = []
    for raw in raw_events:
        n = normalizer.normalize(raw)
        store.append(n)
        normalized.append(n)

    memory.add_events(normalized)

    # Now reconstruct context
    compiler = ContextCompiler(store, alias_reg, memory)
    signal = IncidentSignal(
        ts="2026-05-10T14:32:11Z",
        kind="incident_signal",
        incident_id="INC-714",
        trigger="alert/error-rate>5%",
    )
    ctx = compiler.reconstruct(signal, mode="fast")

    # Validate output structure
    assert 'related_events' in ctx
    assert 'causal_chain' in ctx
    assert 'similar_past_incidents' in ctx
    assert 'suggested_remediations' in ctx
    assert 'confidence' in ctx
    assert 'explain' in ctx
    assert isinstance(ctx['confidence'], float)
    assert 0 <= ctx['confidence'] <= 1

    # Should have related events
    assert len(ctx['related_events']) > 0

    # Remediation should include rollback
    if ctx['suggested_remediations']:
        actions = [r['action'] for r in ctx['suggested_remediations']]
        assert 'rollback' in actions

    print(f"Context reconstructed successfully!")
    print(f"  Related events: {len(ctx['related_events'])}")
    print(f"  Causal edges: {len(ctx['causal_chain'])}")
    print(f"  Similar incidents: {len(ctx['similar_past_incidents'])}")
    print(f"  Remediations: {len(ctx['suggested_remediations'])}")
    print(f"  Confidence: {ctx['confidence']}")
    print(f"  Explain preview: {ctx['explain'][:200]}...")

if __name__ == "__main__":
    test_full_reconstruction()
    print("ALL COMPILER TESTS PASSED")
```

---

## Phase 2: Integration (Hour 8-10)

### `adapters/myteam.py` — YOU own this file during integration

```python
"""Benchmark adapter — thin glue wiring all three modules."""
# Adjust the import path based on where adapter.py lives in the bench
# from adapter import Adapter
# from schema import Event, IncidentSignal, Context

from engine.ingestion.alias_registry import AliasRegistry
from engine.ingestion.event_store import EventStore
from engine.ingestion.normalizer import EventNormalizer
from engine.memory import MemoryGraph
from engine.compiler.reconstructor import ContextCompiler

class Engine:
    def __init__(self):
        self.alias_registry = AliasRegistry()
        self.event_store = EventStore()
        self.normalizer = EventNormalizer(self.alias_registry)
        self.memory_graph = MemoryGraph()
        self.compiler = ContextCompiler(
            self.event_store, self.alias_registry, self.memory_graph
        )
        self._batch = []

    def ingest(self, events):
        normalized = []
        for event in events:
            n = self.normalizer.normalize(event)
            self.event_store.append(n)
            normalized.append(n)
        self.memory_graph.add_events(normalized)

    def reconstruct_context(self, signal, mode="fast"):
        return self.compiler.reconstruct(signal, mode)

    def close(self):
        pass
```

Your module is done when:
- [ ] `ContextCompiler.reconstruct()` returns valid `Context` for the worked example
- [ ] Output matches benchmark TypedDict exactly
- [ ] Explain narrative is human-readable
- [ ] All tests pass
- [ ] Push to `feat/compiler`, PR to main

---

## Integration Test (ALL 3 TOGETHER, Hour 8-10)

```bash
# Merge all branches to main
git checkout main
git merge feat/ingestion
git merge feat/memory
git merge feat/compiler

# Run unit tests
python tests/test_ingestion.py
python tests/test_memory.py
python tests/test_compiler.py

# Run benchmark self-check (when repo is available)
python self_check.py --adapter adapters.myteam --quick
```
