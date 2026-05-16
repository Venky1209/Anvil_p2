# Anvil Persistent Context Engine

Submission for Anvil P-02: Persistent Context Engine for autonomous SRE.

The adapter implements an operational memory substrate, not a keyword or static
vector retrieval wrapper. It ingests telemetry into a canonical service identity
graph, synthesizes causal edges during ingestion, stores resolved incident
episodes, and reconstructs incident context with topology-independent behavioral
matching.

## Quickstart

From the public benchmark directory:

```powershell
$env:PYTHONPATH='D:\apps\anvil'
python run.py --adapter Anvil.adapters.myteam:Engine --mode fast --seeds 314159 271828 161803 141421 173205 --out report.json
```

On Unix-like systems, from this repository:

```bash
bench/run.sh
```

`bench/run.sh` expects the benchmark at one of:

- `../Anvil-P-E/bench-p02-context`
- `../Anvil-P-E-eval/bench-p02-context`
- `$ANVIL_BENCH_DIR`

## Adapter

Use:

```text
Anvil.adapters.myteam:Engine
```

Surface:

```python
class Engine:
    def ingest(self, events): ...
    def reconstruct_context(self, signal, mode="fast"): ...
    def close(self): ...
```

Returned `Context` shape:

```python
{
    "related_events": list,
    "causal_chain": list,
    "similar_past_incidents": list,
    "suggested_remediations": list,
    "confidence": float,
    "explain": str,
}
```

`related_events` are ordered, deduped, and include provenance in
`event["attrs"]["provenance"]`.

## Architecture

- **Identity resolution:** service names are mapped to stable canonical IDs.
  Topology rename events merge aliases into the same operational identity, so
  `payments-svc -> billing-svc` remains one memory entity.
- **Causal synthesis:** deploy, metric, log, and trace events are linked at
  ingest time into confidence-weighted causal edges.
- **Incident memory:** incident episodes store canonical IDs, event IDs, causal
  chain IDs, remediation outcome, and a 48-float behavioral fingerprint.
- **Behavioral matching:** reconstruction uses tiered hybrid scoring. Canonical
  matches are sorted above cross-service matches, but raw behavioral similarity
  is preserved inside each tier to avoid score-ceiling compression.
- **Memory evolution:** successful remediations reinforce stored causal edges and
  feed future remediation suggestions.
- **Glacier-style tiering:** operational memory follows an AWS Glacier-like
  lifecycle. Active incidents and recently accessed matches are `hot`, recent
  resolved episodes are `warm`, and older inactive resolved episodes become
  `cold`. Long-horizon recall still searches all tiers, but tiering gives the
  engine an inspectable salience model for memory evolution.

## Expected Benchmark Performance

Latest local L3-style run:

```json
{
  "recall@5": 1.0,
  "precision@5_mean": 0.9776,
  "remediation_acc": 1.0,
  "latency_p95_ms": 14.75,
  "latency_mean_ms": 9.926,
  "n_signals_total": 125
}
```

Automated score:

```json
{
  "weighted_score": 0.7966,
  "max_automated": 0.8
}
```

The public harness reports `manual_context` and `manual_explain` as `null`
because they are panel-graded. The adapter still returns structured
`related_events`, `causal_chain`, and a human-readable `explain` string for
manual review.

## Optional Groq Enrichment

Unknown or weakly explained incidents can enrich only the `explain` field with
Groq:

```bash
export GROQ_API_KEY="your_key"
```

or place this in `.env`:

```env
GROQ_API_KEY=your_key
```

Without `GROQ_API_KEY`, LLM enrichment is disabled silently. Retrieval,
remediation, and automated benchmark scores do not depend on Groq.

Egress disclosure: when enabled, the adapter sends a tiny summary of at most five
error or critical logs to Groq using `llama-3.3-70b-versatile`. It never sends
the full raw event stream. Each engine instance caps Groq enrichment at three
calls, with a 2.5 second timeout and silent fallback.

## Dependencies

Runtime uses Python standard library for core ingestion, identity, storage, and
scoring. Optional dependencies are pinned in `requirements.txt`.

- `groq==1.2.0`, optional explain enrichment
- `networkx==3.6.1`, disclosed graph traversal dependency
- `numpy==2.4.4`, disclosed vector math dependency

## Reproducibility

Build:

```bash
docker build -t anvil-pce .
```

Run with a mounted benchmark:

```bash
docker run --rm -e ANVIL_BENCH_DIR=/bench -v /path/to/Anvil-P-E/bench-p02-context:/bench anvil-pce
```

## Files To Review

- `adapters/myteam.py` - benchmark adapter and reconstruction logic
- `engine/ingestion/identity.py` - topology drift and rename handling
- `engine/ingestion/causal.py` - relationship synthesis
- `engine/ingestion/storage.py` - incident and event memory
- `engine/compiler/fingerprint.py` - behavioral fingerprinting
- `WRITEUP.md` - technical defense for judging
