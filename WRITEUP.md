# Anvil PCE Technical Writeup

## 1. Memory Representation

Anvil PCE stores operational memory as three connected layers.

The first layer is a canonical identity registry. Every service name resolves to
a stable canonical ID. Rename topology events merge aliases into the same
identity, which preserves continuity across drift such as
`payments-svc -> billing-svc`.

The second layer is an event and causal graph. Raw telemetry is stored with
provenance, normalized service IDs, metric fields, trace IDs, and incident IDs.
During ingestion, the engine creates confidence-weighted causal edges for deploy
attribution, metric degradation, trace latency propagation, upstream log
mentions, and metric-log correlation.

The third layer is incident memory. Resolved incidents are stored as episodes
containing canonical IDs, event IDs, causal edge IDs, remediation action/outcome,
and a 48-float behavioral fingerprint. This lets the engine retrieve recurring
operational behavior instead of relying on string equality.

Incident memory is tiered with a Glacier-style lifecycle. Active incidents and
recently accessed matches are `hot`, recently resolved but inactive incidents are
`warm`, and older inactive incidents become `cold`. Cold memory is not discarded:
it remains searchable for long-horizon recall, while the tier gives judges and
operators a visible signal of memory salience and recency.

## 2. Relationship Synthesis Algorithm

Relationship synthesis happens at ingest time to keep reconstruction fast.

Deploys are linked to subsequent degradation metrics and slow traces within a
bounded temporal window. Error logs are linked to metrics and mentioned upstream
services. Traces infer caller/callee relationships and mark the slow callee as
the likely cause when it dominates caller duration. Successful remediations
reinforce causal edges associated with the incident.

At reconstruction time, the engine compiles a bounded local context around the
signal service, recent deploys, adjacent causal IDs, and the incident window.
Returned events are deduped, chronological, and annotated with provenance in
`attrs.provenance`.

## 3. Drift Handling Strategy

Topology drift is handled through canonical IDs and alias history. A renamed
service keeps the same memory identity, so historical deployments and
remediations remain available under the new name. Reconstruction also considers
behavioral fingerprints so cross-service recurring families can surface when
canonical overlap is absent or incomplete.

The matcher uses tiered hybrid scoring:

- Tier A: canonical overlap, sorted by `1.0 + raw_behavioral_cosine`
- Tier B: cross-service behavior, sorted by `raw_behavioral_cosine`

Scores are kept unbounded for sorting and clamped only in final output. This
prevents score-ceiling compression where unrelated same-service incidents all
collapse to one displayed score and sort arbitrarily.

## 4. Latency Engineering

The engine uses in-memory SQLite and indexed tables for event, causal edge, and
incident lookups. Causal relationships are computed once during ingestion rather
than recomputed during reconstruction. Query-time traversal is bounded by time
window, canonical IDs, and top-k limits.

Latest local L3-style benchmark across five seeds:

```json
{
  "recall@5": 1.0,
  "precision@5_mean": 0.9776,
  "remediation_acc": 1.0,
  "latency_p95_ms": 14.75,
  "latency_mean_ms": 9.926
}
```

This is well under the fast-mode 2 second p95 budget.

## 5. Evolution Mechanism

Memory evolves through resolved incidents. When remediation succeeds, the
episode is persisted with remediation outcome and causal chain IDs. Associated
causal edges are reinforced, increasing their future confidence. Recurrent
families therefore become easier to reconstruct over time, and suggested
remediations are drawn from historically successful actions for matching
incident families.

The hot/warm/cold lifecycle is also updated during reconstruction. Matching a
historical incident promotes it to `hot` and updates `last_accessed_ts`.
Unaccessed resolved incidents age from `warm` to `cold`. This is analogous to
AWS storage classes: recent operational memory stays immediately salient, while
older memory remains durable and queryable for drift-heavy recurrence.

## 6. Explainability

The output is structured, not narrative-only. `related_events` carry source
provenance, `causal_chain` edges include evidence dictionaries and confidences,
`similar_past_incidents` include rationale strings, and `suggested_remediations`
include historical outcome and confidence.

The `explain` field summarizes the reconstructed identity, alias history, causal
edge count, similar incidents, and remediation suggestion. Optional Groq
enrichment is used only for weak or unknown causal chains and only appends to
`explain`; it never affects retrieval or automated scoring.

## 7. What Fails In The Baseline

Static vector or keyword retrieval loses continuity when services are renamed,
when upstream errors reference an old alias, or when multiple incident families
share the same service. Anvil PCE avoids this by combining canonical identity,
causal graph memory, and behavioral fingerprints with tiered sorting.
