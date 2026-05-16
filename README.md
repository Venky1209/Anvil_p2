# Anvil PCE

**Persistent Context Engine for autonomous SRE**

Not a dashboard. Not a log viewer. Not a retrieval wrapper.  
Anvil PCE turns telemetry into operational memory that survives renames,
topology drift, recurring failure families, and noisy incident signatures.

```text
telemetry stream
   -> canonical identity graph
   -> causal edge synthesis
   -> hot / warm / cold incident memory
   -> adaptive context reconstruction
```

## Scorecard

Latest local L3-style benchmark, 5 seeds, 125 eval signals:

| Metric | Result |
|---|---:|
| recall@5 | `1.0000` |
| precision@5 mean | `0.9776` |
| remediation accuracy | `1.0000` |
| fast p95 latency | `17.65 ms` |
| automated score | `0.7966 / 0.8000` |

The harness reports `manual_context` and `manual_explain` as `null` because
those axes are panel-graded. The adapter still returns ordered provenance-rich
events, evidence-backed causal chains, and a narrative `explain` field.

## Run

Adapter:

```text
Anvil.adapters.myteam:Engine
```

From the public `bench-p02-context` directory:

```powershell
$env:PYTHONPATH='D:\apps\anvil'
python run.py --adapter Anvil.adapters.myteam:Engine --mode fast --seeds 314159 271828 161803 141421 173205 --out report.json
```

From this repo on Unix-like systems:

```bash
bash bench/run.sh
```

If the benchmark lives somewhere custom:

```bash
ANVIL_BENCH_DIR=/path/to/bench-p02-context bash bench/run.sh
```

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
