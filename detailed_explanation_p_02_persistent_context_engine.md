# Detailed Breakdown of P-02 — Persistent Context Engine for Autonomous SRE

Source: ANVIL 2026 Problem Statement P-02 fileciteturn3file0L1-L260

---

# 1. What This Problem REALLY Is

This problem is NOT:

- a chatbot over logs
- a vector database
- RAG over telemetry
- Grafana dashboard clone
- Elasticsearch wrapper
- semantic search system

The judges explicitly warn against this:

> “Naive semantic retrieval, static vector similarity, or keyword pipelines are unlikely to clear the benchmark.”

That single sentence tells you almost the entire judging philosophy.

The problem is actually asking:

# “Can you build a system that REMEMBERS operational behavior over time?”

Not telemetry.

Behavior.

---

# 2. The Core Pain Point They’re Solving

In real production systems:

- services get renamed
- dependencies change
- deployments alter behavior
- telemetry signatures drift
- infrastructure topology evolves
- incidents mutate over time

Example:

## Month 1

payments-svc deploy
→ latency spike
→ checkout errors
→ rollback fixed it

## Month 3

payments-svc renamed to billing-svc
new dependency graph introduced
log formats changed

Now:

billing-svc deploy
→ upstream API failures
→ rollback fixes issue

Humans can immediately recognize:

> “This is basically the same incident.”

Traditional observability tools usually cannot.

Why?

Because they depend on:

- service names
- keyword matches
- embedding similarity
- static topology assumptions

Once infrastructure drifts, they degrade badly.

That is the ENTIRE motivation behind this PS.

---

# 3. The Core Question

The PS literally asks:

> “How can operational telemetry be transformed into persistent memory capable of adaptive reasoning across evolving distributed environments?”

This is the main research idea.

Not storage.

Not search.

Persistent operational memory.

---

# 4. What Is “Operational Memory”?

Traditional systems store:

- logs
- metrics
- traces
- alerts

This PS wants a system that stores:

- causal relationships
- recurring behaviors
- remediation history
- temporal patterns
- operational equivalence
- evolving incident families

Think of it this way:

| Traditional Observability | P-02 Engine |
|---|---|
| stores data | stores understanding |
| query-based | memory-based |
| exact matching | behavioral reasoning |
| topology dependent | topology independent |
| static | evolving |

---

# 5. What Makes This Problem Hard

The benchmark is specifically designed to destroy simplistic approaches.

The hidden difficulty is:

# TOPOLOGY DRIFT

Meaning:

The infrastructure changes continuously.

Example:

## Before

checkout-api → payments-svc → db

## After

checkout-api → billing-svc → ledger-service → db

A weak system sees:

“Completely different architecture.”

A strong system sees:

“Operationally similar behavior pattern.”

That distinction is EVERYTHING.

---

# 6. The Input Data Structure

The engine receives telemetry as JSON events.

The PS defines six guaranteed event types.

---

## A. Deploy Events

```json
{
  "kind": "deploy",
  "service": "payments-svc",
  "version": "v2.14.0"
}
```

Meaning:

A new version of a service was deployed.

Operational importance:

Deployments are often root causes.

---

## B. Log Events

```json
{
  "kind": "log",
  "service": "checkout-api",
  "level": "error",
  "msg": "timeout calling payments-svc"
}
```

Meaning:

Error symptoms.

Logs usually represent downstream effects.

---

## C. Metric Events

```json
{
  "kind": "metric",
  "name": "latency_p99_ms",
  "value": 4820
}
```

Meaning:

Performance degradation.

Metrics help identify:

- spikes
- trends
- anomalies

---

## D. Trace Events

```json
{
  "kind": "trace",
  "spans": [...]
}
```

Meaning:

Request execution flow across services.

Useful for:

- dependency inference
- cascade detection
- bottleneck discovery

---

## E. Topology Events

```json
{
  "kind": "topology",
  "change": "rename",
  "from": "payments-svc",
  "to": "billing-svc"
}
```

This is CRITICAL.

The benchmark intentionally mutates infrastructure.

If your system depends heavily on exact service names:

it dies here.

---

## F. Incident Signals

```json
{
  "kind": "incident_signal",
  "incident_id": "INC-714"
}
```

This triggers context reconstruction.

---

## G. Remediation Events

```json
{
  "kind": "remediation",
  "action": "rollback",
  "outcome": "resolved"
}
```

This teaches the engine:

- which fixes worked
- which failed
- remediation success rates

This becomes long-term memory.

---

# 7. The Two Main Functions

The interface is intentionally minimal.

---

## Function 1 — ingest()

```python
class Engine:
    def ingest(self, events):
        ...
```

Purpose:

Continuously absorb telemetry.

What SHOULD happen internally:

- event normalization
- temporal ordering
- relationship synthesis
- graph updates
- memory reinforcement
- drift tracking

The judges do NOT care HOW you do this.

They care about resulting behavior.

---

## Function 2 — reconstruct_context()

```python
def reconstruct_context(signal, mode="fast"):
```

THIS is the heart of the benchmark.

Given a new incident:

Your engine must reconstruct:

- what likely caused it
- related events
- similar historical incidents
- probable fixes
- causal chains

The output is structured.

Not free text.

---

# 8. Understanding the Output Structure

The engine returns:

```python
class Context(TypedDict):
```

with several fields.

---

## A. related_events

The engine must surface relevant telemetry.

Not random nearby events.

Signal-dense context.

Example:

- deployment
- latency spike
- timeout logs
- traces

All tied together.

---

## B. causal_chain

The engine should infer relationships.

Example:

deploy
→ latency increase
→ upstream failures

with confidence scores.

Important:

The benchmark checks temporal ordering.

Cause must precede effect.

---

## C. similar_past_incidents

This is probably the MOST important part.

The system must identify:

> “We have seen something operationally equivalent before.”

Even if:

- services renamed
- topology changed
- telemetry mutated

This is the central intelligence test.

---

## D. suggested_remediations

Example:

```text
Rollback billing-svc
```

Confidence should depend on:

- historical success rate
- incident similarity
- operational outcomes

---

## E. explain

Human-readable narrative.

Example:

> “A deployment to billing-svc was followed by elevated latency and checkout-api timeout errors. Similar behavior was observed in INC-421 where rollback resolved the incident.”

Judges manually inspect this.

---

# 9. The Hidden Core Idea — INCIDENT SHAPES

This is the MOST important concept in the entire PS.

Weak systems store:

```text
payments timeout
```

Strong systems store:

```text
deploy
→ latency spike
→ upstream errors
→ rollback success
```

That sequence is an INCIDENT SHAPE.

Incident shapes survive:

- renames
- topology mutations
- telemetry drift

That is what your architecture should revolve around.

---

# 10. Why Embeddings Alone Fail

The PS explicitly warns about semantic retrieval.

Reason:

Embeddings depend heavily on:

- wording
- naming
- telemetry representation

Example:

## Old Incident

payments timeout

## New Incident

billing congestion causing upstream saturation

Operationally similar.

Semantically different.

Vector similarity may fail badly.

That is why this is framed as:

# a systems problem

not an NLP problem.

---

# 11. Dynamic Relationship Synthesis

The PS says:

> “Construct relationships without predefined schemas.”

Meaning:

Do NOT hardcode:

```text
deploy always causes latency
```

Instead relationships should emerge dynamically.

Possible signals:

- repeated co-occurrence
- time proximity
- trace correlation
- remediation success
- topology adjacency
- recurring failure order

The graph evolves over time.

---

# 12. Long-Horizon Memory

This section matters a lot.

The engine must preserve understanding across:

- weeks
- topology drift
- infrastructure mutations
- telemetry evolution

Not just short-term correlation.

The benchmark evaluates this.

---

# 13. Incident Family Recognition

The PS mentions:

> “Recurring incident families.”

This is another key concept.

Example families:

---

## Family A

Deploy instability family:

- deploy
- latency
- upstream timeout
- rollback success

---

## Family B

Dependency cascade family:

- dependency change
- trace fanout explosion
- DB overload

Your engine should classify incidents into evolving families.

This massively improves recall.

---

# 14. Memory Evolution

The system should improve over time.

Example:

First time:

confidence = 0.45

After repeated successful rollback patterns:

confidence = 0.88

This is operational learning.

---

# 15. The Evaluation Metrics

These metrics are VERY important.

---

## Incident Recall

Can you retrieve similar historical incidents?

This is likely the highest-impact metric.

---

## Context Quality

Are surfaced events actually useful?

Random event dumps score badly.

---

## Pattern Recognition

Can your system detect recurring behavioral families?

---

## Temporal Reasoning

Did your causal ordering make sense?

Cause before effect.

---

## Adaptability

How much does your system degrade after topology drift?

Smaller degradation = better system.

---

## Memory Evolution

Does the engine improve after seeing more incidents?

---

# 16. The Hidden Adversarial Tests

The benchmark has three layers.

---

## L1 — Canonical

Basic public scenario.

Easy.

---

## L2 — Multi-seed

Fresh infrastructure every run.

Kills hardcoded solutions.

---

## L3 — Adversarial

Hidden scenarios:

- cascading rename chains
- correlated outages
- topology shifts mid-evaluation

This is where weak systems collapse.

---

# 17. What Winning Teams Will Probably Build

Likely features:

- temporal memory graphs
- behavioral fingerprints
- incident family clustering
- topology alias tracking
- remediation reinforcement
- causal scoring
- drift-invariant representations

NOT giant LLM wrappers.

---

# 18. What Weak Teams Will Build

Probably:

```text
logs → embeddings → vector search
```

These systems:

- fail rename robustness
- fail topology drift
- fail behavioral equivalence
- fail hidden scenarios

The PS basically warns about this directly.

---

# 19. The Smart Architectural Direction

If you want a strong but feasible solution:

Focus on:

# Behavioral Graph Memory

Core components:

- event graph
- temporal edges
- causal scoring
- incident fingerprints
- family clustering
- remediation memory
- topology alias mapping

This aligns extremely well with benchmark goals.

---

# 20. The North Star

The PS itself says:

> “Not a dashboard. Not a log viewer. Not a retrieval wrapper. An operational memory engine.”

That is the entire philosophy.

If your project still feels like:

- search
- retrieval
- observability UI
- RAG

then you are probably solving the wrong problem.

---

# 21. Final Simplified Interpretation

The entire problem can be summarized as:

> “Build a system that remembers how infrastructure failures behave over time, even when the infrastructure itself changes.”

