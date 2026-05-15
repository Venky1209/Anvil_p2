# P2 Standout Strategies + Troubleshooting

## 5 Things That Will Make You Win

### 1. Behavioral Fingerprinting (Topology-Independent Matching)
Most teams will match incidents by service name or log text similarity. That breaks the moment a service is renamed. Your system matches by **behavioral shape**: the *sequence of event kinds* (deploy → metric spike → error → rollback) regardless of which service it happens on. This is the single most tested thing in the benchmark.

**How to verify:** Create two incidents on different services with the same event pattern. Your system must match them as similar with score > 0.5.

### 2. Alias Registry with Temporal Awareness
Don't just track "payments-svc became billing-svc." Track *when* it happened. Events before the rename use the old name; events after use the new name. Both must resolve to the same canonical identity. The L3 adversarial tests will chain renames: A → B → C.

**How to verify:** Ingest 3 renames in sequence. Query by any name at any time. All must resolve to the original canonical.

### 3. Causal Chain with Evidence Pointers
Don't just say "deploy caused metric spike." Include the *evidence*: "Deploy v2.14.0 at 14:21:30 preceded latency spike to 4820ms at 14:22:01 on the same canonical service." The benchmark scores confidence AND evidence quality. Judges grade `explain` narratives 1-5.

### 4. Memory Evolution (Train vs Full)
The benchmark explicitly measures improvement between train-only ingestion and full ingestion. Your system should get *smarter* as it sees more data. Implement reinforcement: when a remediation resolves an incident, boost the confidence of the causal edges that led to it. When patterns recur, bump the family occurrence count.

### 5. Fast vs Deep Mode Differentiation
Don't just make "deep" slower. Make it *smarter*. Fast mode: bounded BFS (depth 3), 30-min time window, top-5 results. Deep mode: exhaustive traversal (depth 6), 2-hour window, top-15, plus LLM-enhanced explain narrative. This shows architectural intent, not just a flag check.

---

## How to Simulate Before the Benchmark Repo Opens

Since the repo (`Sauhard74/Anvil-P-E`) is private until the mentoring session, simulate with your own test data.

### Create `tests/fixtures/sample_events.jsonl`

```json
{"ts":"2026-05-10T14:00:00Z","kind":"deploy","service":"auth-svc","version":"v1.0.0","actor":"ci"}
{"ts":"2026-05-10T14:05:00Z","kind":"metric","service":"auth-svc","name":"latency_p99_ms","value":120}
{"ts":"2026-05-10T14:10:00Z","kind":"deploy","service":"payments-svc","version":"v2.14.0","actor":"ci"}
{"ts":"2026-05-10T14:12:00Z","kind":"metric","service":"payments-svc","name":"latency_p99_ms","value":4820}
{"ts":"2026-05-10T14:12:30Z","kind":"log","service":"checkout-api","level":"error","msg":"timeout calling payments-svc","trace_id":"abc123"}
{"ts":"2026-05-10T14:13:00Z","kind":"trace","trace_id":"abc123","spans":[{"svc":"checkout-api","dur_ms":5012},{"svc":"payments-svc","dur_ms":4980}]}
{"ts":"2026-05-10T14:15:00Z","kind":"incident_signal","incident_id":"INC-001","trigger":"alert/error-rate>5%"}
{"ts":"2026-05-10T14:20:00Z","kind":"remediation","incident_id":"INC-001","action":"rollback","target":"payments-svc","version":"v2.13.4","outcome":"resolved"}
{"ts":"2026-05-10T15:00:00Z","kind":"topology","change":"rename","from":"payments-svc","to":"billing-svc"}
{"ts":"2026-05-10T16:00:00Z","kind":"deploy","service":"billing-svc","version":"v2.15.0","actor":"ci"}
{"ts":"2026-05-10T16:02:00Z","kind":"metric","service":"billing-svc","name":"latency_p99_ms","value":5100}
{"ts":"2026-05-10T16:02:30Z","kind":"log","service":"checkout-api","level":"error","msg":"timeout calling billing-svc","trace_id":"def456"}
{"ts":"2026-05-10T16:05:00Z","kind":"incident_signal","incident_id":"INC-002","trigger":"alert/error-rate>5%"}
{"ts":"2026-05-10T16:15:00Z","kind":"remediation","incident_id":"INC-002","action":"rollback","target":"billing-svc","version":"v2.14.0","outcome":"resolved"}
```

**This tests the CRITICAL scenario:** INC-002 on `billing-svc` should match INC-001 on `payments-svc` because they share the same behavioral pattern (deploy → latency spike → error → rollback) and `billing-svc` is the same canonical service as `payments-svc`.

### Simulation Script — `tests/simulate.py`

```python
"""Quick simulation to verify core pipeline works end-to-end."""
import json
import time

from engine.ingestion.alias_registry import AliasRegistry
from engine.ingestion.event_store import EventStore
from engine.ingestion.normalizer import EventNormalizer
from engine.memory import MemoryGraph
from engine.compiler.reconstructor import ContextCompiler
from engine.shared.types import IncidentSignal

def run_simulation():
    # Init all modules
    aliases = AliasRegistry()
    store = EventStore()
    normalizer = EventNormalizer(aliases)
    memory = MemoryGraph()
    compiler = ContextCompiler(store, aliases, memory)

    # Load test events
    with open("tests/fixtures/sample_events.jsonl") as f:
        raw_events = [json.loads(line) for line in f if line.strip()]

    # Ingest
    print(f"Ingesting {len(raw_events)} events...")
    start = time.time()
    normalized = []
    for raw in raw_events:
        n = normalizer.normalize(raw)
        store.append(n)
        normalized.append(n)
    memory.add_events(normalized)
    ingest_time = time.time() - start
    print(f"  Ingested in {ingest_time:.3f}s ({len(raw_events)/max(ingest_time,0.001):.0f} events/sec)")

    # Check alias resolution
    print(f"\nAlias check:")
    print(f"  'billing-svc' resolves to: {aliases.resolve('billing-svc')}")
    print(f"  'payments-svc' resolves to: {aliases.resolve('payments-svc')}")
    print(f"  All aliases: {aliases.get_all_aliases('payments-svc')}")

    # Reconstruct context for INC-002 (the POST-rename incident)
    signal = IncidentSignal(
        ts="2026-05-10T16:05:00Z",
        kind="incident_signal",
        incident_id="INC-002",
        trigger="alert/error-rate>5%",
    )

    print(f"\nReconstructing context for INC-002 (fast mode)...")
    start = time.time()
    ctx = compiler.reconstruct(signal, mode="fast")
    recon_time = time.time() - start

    print(f"  Completed in {recon_time:.3f}s")
    print(f"  Related events: {len(ctx['related_events'])}")
    print(f"  Causal edges: {len(ctx['causal_chain'])}")
    print(f"  Similar past incidents: {len(ctx['similar_past_incidents'])}")
    print(f"  Remediations: {len(ctx['suggested_remediations'])}")
    print(f"  Confidence: {ctx['confidence']}")

    # THE CRITICAL CHECK
    print(f"\n--- CRITICAL VALIDATION ---")
    similar_ids = [m['past_incident_id'] for m in ctx['similar_past_incidents']]
    if 'INC-001' in similar_ids:
        print(f"  ✅ PASS: INC-001 found as similar to INC-002 (across rename!)")
        for m in ctx['similar_past_incidents']:
            if m['past_incident_id'] == 'INC-001':
                print(f"     Similarity: {m['similarity']}")
                print(f"     Rationale: {m['rationale']}")
    else:
        print(f"  ❌ FAIL: INC-001 NOT found as similar to INC-002")
        print(f"     Found: {similar_ids}")
        print(f"     This means rename handling or fingerprinting is broken!")

    rem_actions = [r['action'] for r in ctx['suggested_remediations']]
    if 'rollback' in rem_actions:
        print(f"  ✅ PASS: Rollback suggested as remediation")
    else:
        print(f"  ❌ FAIL: Rollback not suggested")

    if recon_time < 2.0:
        print(f"  ✅ PASS: Fast mode latency {recon_time:.3f}s < 2.0s budget")
    else:
        print(f"  ❌ FAIL: Fast mode latency {recon_time:.3f}s exceeds 2.0s budget")

    print(f"\n--- EXPLAIN NARRATIVE ---")
    print(ctx['explain'][:500])

if __name__ == "__main__":
    run_simulation()
```

---

## Troubleshooting Common Issues

### 1. "billing-svc not matching payments-svc"
**Cause:** AliasRegistry isn't processing topology events before subsequent events.
**Fix:** Ensure events are processed in timestamp order. The topology rename MUST be processed before any events using the new name.

### 2. "No causal edges found"
**Cause:** EdgeBuilder time window is too tight, or event timestamps aren't being parsed correctly.
**Fix:** Check `_within_window()` — ensure ISO timestamps with `Z` suffix are parsed correctly. Use `.replace('Z', '+00:00')` for `fromisoformat()`.

### 3. "Similar incidents empty"
**Cause:** Fingerprinter hasn't seen enough incidents yet, or patterns don't share enough event kinds.
**Fix:** Make sure you fingerprint incidents during ingestion (when you see remediation events that close an incident), not just at query time.

### 4. "Latency exceeds budget"
**Cause:** BFS traversal visiting too many nodes, or querying all events instead of windowed.
**Fix:** In fast mode, cap BFS depth at 3 and time window at 30 minutes. Use the indexed queries (`query_by_service`, `query_by_incident`) not `get_all()`.

### 5. "KeyError on 'from' field in topology events"
**Cause:** `from` is a Python reserved word.
**Fix:** Access it as `raw_event.get('from')` (dict access works fine, only `from` as a variable name is reserved).

### 6. "Tests pass but benchmark score is low"
**Cause:** Likely the identity/rename boundary test. The benchmark runs multiple seeds — each seed generates different rename chains.
**Fix:** Don't hardcode service names anywhere. Always go through `AliasRegistry.resolve()`. Never compare raw service names directly.

### 7. "Memory evolution score is 0"
**Cause:** Your system doesn't improve with more data.
**Fix:** Implement reinforcement in DecayManager: when a remediation resolves an incident, boost the confidence of edges in the causal chain that predicted it. Track `occurrences` in fingerprints.

---

## Integration Checklist (Hour 8-10)

```
[ ] All 3 branches merged to main without conflicts
[ ] `python tests/test_ingestion.py` passes
[ ] `python tests/test_memory.py` passes
[ ] `python tests/test_compiler.py` passes
[ ] `python tests/simulate.py` shows all ✅
[ ] INC-001 found as similar to INC-002 across rename
[ ] Rollback suggested as remediation
[ ] Fast mode latency < 2s
[ ] adapters/myteam.py wires everything correctly
```

## Final Deliverables Checklist (Hour 20-24)

```
[ ] README.md with quickstart
[ ] Dockerfile that builds and runs
[ ] requirements.txt with pinned versions
[ ] bench/run.sh works with self_check.py
[ ] 3-page writeup PDF (memory representation, drift handling, latency engineering)
[ ] 5-min demo video walking through the worked example
[ ] All external dependencies disclosed
```
