# Anvil PCE

**Persistent Context Engine for autonomous SRE**

Not a dashboard. Not a log viewer. Not a retrieval wrapper.  
Anvil PCE turns telemetry into operational memory that survives renames,
topology drift, recurring failure families, chaos shifts, and noisy incident
signatures.

```text
telemetry stream
   -> canonical identity graph
   -> causal edge synthesis
   -> hot / warm / cold incident memory
   -> adaptive context reconstruction
```

## Scorecard

Example local L3-style validation run, 5 arbitrary seeds, 125 eval signals:

| Metric | Result |
|---|---:|
| recall@5 | `1.0000` |
| precision@5 mean | `0.9776` |
| remediation accuracy | `1.0000` |
| fast p95 latency | `< 20 ms` |
| automated score | `0.7966 / 0.8000` |

These numbers are **not hardcoded assumptions**. The benchmark creates a fresh
adapter per seed, and final judging may use hidden seeds, higher L3 parameters,
a held-out 20-incident eval set, and a runtime chaos topology shift.

## Run

Adapter:

```text
adapters.myteam:Engine
```

From this repository, pointing at the public `bench-p02-context` directory:

```bash
BENCH_DIR=/path/to/bench-p02-context
PYTHONPATH="$PWD:$BENCH_DIR" python -m run \
  --adapter adapters.myteam:Engine \
  --mode fast \
  --out report.json
```

Stress with arbitrary seeds:

```bash
BENCH_DIR=/path/to/bench-p02-context
PYTHONPATH="$PWD:$BENCH_DIR" python -m run \
  --adapter adapters.myteam:Engine \
  --mode fast \
  --seeds 9999 31415 27182 16180 11235 \
  --out report.json
```

From this repo:

```bash
bash bench/run.sh
```

Custom benchmark location or args:

```bash
ANVIL_BENCH_DIR=/path/to/bench-p02-context bash bench/run.sh \
  --mode fast --seeds 9999 31415 --out report.json
```

## Evaluation Readiness

| Layer | Readiness |
|---|---|
| L1 canonical | Worked example returns deploy, metric, trace, upstream log, rename, match, and rollback. |
| L2 property-based | Designed for arbitrary seeds; no cross-seed cache assumptions. |
| L3 adversarial | Handles cascading renames, denser drift, decoys, and morphed families. |
| Chaos shift | Runtime topology renames update canonical identity and preserve recall across aliases. |
| Manual review | `related_events` include provenance; `causal_chain` includes evidence/confidence; `explain` is populated. |

## Contract Output

`reconstruct_context()` returns the SDK `Context` shape:

```python
{
    "related_events": list,          # chronological, deduped, with attrs.provenance
    "causal_chain": list,            # evidence + confidence
    "similar_past_incidents": list,  # incident_id + similarity + rationale
    "suggested_remediations": list,  # action + target + historical_outcome
    "confidence": float,             # 0..1
    "explain": str,                  # human-readable narrative
}
```

Worked-example behavior:

```text
payments-svc deploy -> latency spike -> checkout timeout
payments-svc renamed to billing-svc
query on billing-svc
   -> retrieves prior payments-svc rollback pattern
   -> suggests rollback billing-svc
```

The public harness reports `manual_context` and `manual_explain` as `null`
because those axes are panel-graded. The adapter still returns the structured
context used for that review.

## How It Works

| Layer | What it does |
|---|---|
| Canonical identity | Merges service aliases across renames and topology drift. |
| Causal graph | Links deploys, metrics, logs, and traces at ingest time. |
| Behavioral fingerprint | Builds a 48-float incident shape independent of service names. |
| Tiered matcher | Sorts canonical matches above cross-service matches while preserving raw cosine variance. |
| Memory evolution | Reinforces successful causal paths and remediation actions. |

## Glacier-Style Memory

Operational memory uses an AWS Glacier-like lifecycle:

| Tier | Meaning |
|---|---|
| `hot` | Active incidents and recently accessed matches. |
| `warm` | Recently resolved operational memory. |
| `cold` | Older inactive incidents retained for long-horizon recall. |

Cold memory is not discarded. It remains searchable so old incidents can still
surface under rename chains and signature drift.

## Optional Groq Explain Enrichment

Groq is optional and affects only `explain`, never retrieval or scoring.

```bash
export GROQ_API_KEY="your_key"
```

or create `.env`:

```env
GROQ_API_KEY=your_key
```

No key means silent fallback.

Egress disclosure: when enabled, Anvil sends a tiny summary of at most five
error/critical logs to Groq `llama-3.3-70b-versatile`. It never sends the full
event stream. Each engine instance caps Groq enrichment at three calls with a
2.5 second timeout.

## Reproducibility

```bash
docker build -t anvil-pce .
docker run --rm -e ANVIL_BENCH_DIR=/bench -v /path/to/Anvil-P-E/bench-p02-context:/bench anvil-pce
```

Dependencies are pinned in `requirements.txt`:

```text
groq==1.2.0
networkx==3.6.1
numpy==2.4.4
```

## Files

| File | Purpose |
|---|---|
| `adapters/myteam.py` | SDK adapter and reconstruction path. |
| `engine/ingestion/identity.py` | Rename-safe canonical identity. |
| `engine/ingestion/causal.py` | Dynamic relationship synthesis. |
| `engine/ingestion/storage.py` | Event, edge, and episode memory. |
| `engine/compiler/fingerprint.py` | Behavioral fingerprinting and weighted cosine. |
| `WRITEUP.md` | Technical defense for judges. |
| `bench/run.sh` | Submission runner. |
