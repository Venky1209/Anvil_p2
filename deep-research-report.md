# Persistent Context Engine for AI SRE – Deep Research Report

## Executive Summary  
The **Persistent Context Engine (P2)** problem challenges us to build an SRE “memory” system that continuously ingests telemetry (logs, metrics, traces, events) and preserves *operational context* over time. In practice, this means correlating past incidents and system states despite evolving infrastructure (renamed services, shifting dependencies, rollouts, etc.)【35†L333-L337】【37†L236-L244】. Judges score solutions on six axes: Incident Recall, Context Quality, Pattern Recognition, Adaptability, Latency/Scale, and Memory Evolution (each marked “★” in the spec), emphasizing long-term learning over naive search. In other words, we must surface root causes and relevant history consistently, in high volume and at scale, even as the topology “drifts” and telemetry changes.

Our survey of the state of the art finds a clear trend: **knowledge-graph and context-memory approaches** dominate modern AI-SRE systems. Commercial platforms like Ciroos and Chronosphere explicitly build evolving knowledge graphs as the persistent memory “backbone”【37†L236-L244】【35†L333-L337】. Observe Inc. has similarly integrated a knowledge graph into its AI-SRE agents【22†L127-L134】. Open-source tools (e.g. Rootly’s Graphify, Incidentfox) and academic systems (e.g. OpsAgent, KGroot) also exploit graph structures and multi-stage pipelines to integrate metrics, logs, and topology into a unified representation. These approaches enable *graph-based retrieval and reasoning* far beyond keyword or semantic search. 

**Key insights:** Persistent context requires (a) **entity normalization** and identity mapping across versions; (b) a **temporal graph** model that links events and components over time; (c) causal-chain reconstruction algorithms; and (d) continual learning so that each resolved incident **updates the memory** (Chronosphere calls this “Investigation Notebooks” feeding back into the graph【35†L408-L416】). Out-of-the-box LLM-based retrieval (RAG) is discouraged by the spec; instead we should build structured, explainable context with “evidence paths”【24†L103-L112】【33†L52-L61】. 

We identify **9 promising features** to stand out: e.g. a dynamic graph DB backend, robust entity/version mapping (handling renamed services), hybrid graph+vector retrieval, causal-pattern mining (GCN or rule-based), uncertainty scoring, and visual explanation tools. We present a **development pipeline**: ingestion → normalization → graph construction → indexing (vector store + graph queries) → retrieval/scoring → explanation. A sample pipeline diagram is included below.

Our minimal demo will ingest sample incident logs/alerts (JSON lines), normalise entities, build a temporal property graph, and implement queries like “find past incidents matching this error signature” via both graph traversals and embedding search. We will validate correctness using the provided benchmark harness and measure recall/precision on held-out incident queries. Baselines include simple text search and static vector search (e.g. GPT-2 encoder embeddings). 

**Demo ideas** highlight explainability: e.g. interactive causal-chain visualisations; “similar incident” retrieval with side-by-side timeline; and highlighting how the system’s answers improve after each investigation. 

Finally, we outline a **risk plan** (e.g. limited data volume, model hallucinations, time constraints) with mitigations and a time-boxed milestone roadmap. We also suggest specific libraries (NetworkX, Neo4j/neo4j‑driver, Python I/O) and give pseudocode for core parts (entity normalization, graph update, chain reconstruction, ranking). 

Our sources include both the official spec (Anvil Hackathon P2 documents) and a wide range of papers, blogs, and products on observability knowledge graphs and temporal retrieval【37†L236-L244】【35†L333-L337】【28†L119-L128】. Together, these inform our design and show that a successful solution will converge on a continuously-learning graph of SRE context.

## 1. Official P2 Specification and Scoring  

The **P2 problem** is titled *“Persistent Context Engine for autonomous SRE”*.  Its description emphasises that modern observability systems collect telemetry but **do not preserve operator reasoning**.  Every new incident often requires re-deriving causal chains from logs and dashboards.  The spec points out that production environments “evolve continuously” – services are renamed, dependencies shift, deployments change – making static analyses outdated【37†L236-L244】.  Crucially, AI/semantic search alone is said to **degrade under topology drift, temporal evolution, noisy telemetry, or incremental model changes**, so entrants should “build a reasoning substrate, not a search bar”【37†L236-L244】【24†L103-L112】. 

Concretely, the task is to ingest streams of events/alerts and answer queries about incidents by leveraging past knowledge.  The scoring rubric (each metric marked ★ in the spec) comprises:

- **Incident Recall (★):**  How well the system finds relevant past incidents or context when queried.  
- **Context Quality (★):** Accuracy and completeness of the reconstructed context or timeline.  
- **Pattern Recognition (★):** Ability to identify underlying common patterns or root causes across incidents.  
- **Adaptability (★):** Robustness to evolving topology, renamed entities, and data schema changes.  
- **Latency / Scale (★):** Query response time and ability to handle large data volumes.  
- **Memory Evolution (★):** How the system improves with time (learning from each incident, forgetting stale info gracefully).

No numeric weights are given, but all six are critical.  (For comparison, P1 and P4 scores out of 100 used similar categories【10†L217-L225】【8†L3-L10】.)  The spec warns that naïve methods (simple embeddings or keyword search) will *not* suffice【37†L236-L244】【33†L52-L61】.  Instead, competitors should implement structures that explicitly **normalize entities** and model causality over time.  

In summary, the official spec calls for a **long-term incident memory** that continuously links past and present. It should handle renamed services/dependencies (“topology drift”), reliably match new failures to old ones, and allow explainable queries (e.g. “why did metric X spike last week” with references to past incidents)【37†L236-L244】【35†L333-L337】. 

## 2. State of the Art (Recent & Foundational)  

We surveyed recent (≈last 5 years) and foundational work on **operational memory, incident context, temporal graphs, normalization, topology drift**.  The key findings:

- **Commercial AI-Observability Platforms:** Many leading vendors now center on a **knowledge graph** of the system. Ciroos (Signal Intelligence) explicitly labels its core “Persistent Context” graph as a **dynamic knowledge graph that persistently compounds over time**. It ingests telemetry, service mappings and eBPF data, and “human feedback” so that each incident’s solution “never requires human input twice”【37†L236-L244】. Chronicle, Grafana, Observe and others similarly integrate an entity-graph. For example, Chronosphere’s “Temporal Knowledge Graph” continuously maps services/infrastructure and all telemetry types, fusing changes and human notes【35†L333-L337】. These platforms highlight that linking *infrastructure context + telemetry* in a graph is now industry best practice.  
  - *Strength:* Very powerful context model; often includes advanced analytics (suggestions, diff-diagnosis).  
  - *Weakness:* Proprietary; often heavyweight and closed.  
  - *Relevance:* Directly aligns with P2’s goals of persistent context and drift handling.

- **Observability Graph OSS/Tools:** 
  - *Rootly Graphify (OSS)* – a plugin that extracts entities and relations from incident data into a persistent knowledge graph【9†L1-L8】. It’s inspired by ideas (e.g. Andrej Karpathy’s LLM wiki) to build up knowledge rather than re-asking.  
    - *Core idea:* LLM-driven parsing of incident text into a graph of services, alerts, etc.  
    - *Strength:* Open-source, language-agnostic.  
    - *Weakness:* Depends on unstructured text; may lack formal topology.  
    - *Relevance:* Shows viability of graph from raw data.  
  - *IncidentFox (OSS)* – a Slackbot integrating with Prometheus, Grafana, etc., that “on setup reads your codebase and past incidents so it actually knows which service talks to which”【23†L99-L107】. It auto-builds a context model to answer Slack queries.  
    - *Core idea:* Code and logs ingested to map service interactions.  
    - *Strength:* Real integration with real stack, user-friendly Slack interface.  
    - *Weakness:* Possibly brittle; open beta.  
    - *Relevance:* Demonstrates live cross-tool correlation and mapping, including alert meanings.  

- **AI SRE / Knowledge-Graph Vendors:** 
  - *Chronosphere Guided Troubleshooting* – as above, a product with LLM agent; uses a “Temporal Knowledge Graph” normalizing custom telemetry【35†L333-L337】 and capturing all investigation steps in notebooks.  
  - *Observe Inc.* – offers an “AI SRE Agent” on top of an open data lake. Its PR says the **knowledge graph** helps agents “quickly gather more context from the massive volume of observability data”【22†L127-L134】. 
  - *Bacca.ai* – a start-up emphasising that a structured **Knowledge Graph** (not just RAG) is needed for reliable incident response【24†L81-L90】【24†L103-L112】. They ingest telemetry, historical incidents and Slack logs into a proprietary graph with nodes for components and edges for relationships【24†L123-L132】, continuously updating with each human resolution【24†L147-L156】【24†L164-L173】.  
    - *Core idea:* Transform unstructured ops data into a continuously-learning graph encoding queries, investigations, and mappings.  
    - *Strength:* Focus on structured agentic response; emphasizes auditability.  
    - *Weakness:* Closed/proprietary; theory-heavy.  
    - *Relevance:* Directly aligns (KG-based SRE).  

- **Academic Papers & Preprints:** 
  - **OpsAgent (Huang et al., arXiv 2025)** – a multi-agent diagnosis system. OpsAgent ingests raw metrics, logs, traces and runs them through a *“training-free data processor”* to produce **unified, semantically-aligned descriptions**. Three expert agents (anomaly, failure, root-cause) then reason over the same processed evidence【30†L392-L400】. They use chain-of-thought with peer review to ensure interpretability【30†L415-L423】.   
    - *Core idea:* MAS with joint data processing to avoid “agent confusion” by aligning modalities.  
    - *Strength:* Very thorough interpretability and continual learning.  
    - *Weakness:* Complex to implement; heavy research focus.  
    - *Relevance:* Shows how to unify heterogeneous telemetry; stresses audit trails.  
  - **KGroot (Wang et al., arXiv 2024)** – constructs an evolving **fault-event knowledge graph** (FEKG) from historical data, and an “online graph” at fault time. They apply a GCN to compare and rank likely root causes【32†L51-L60】.  
    - *Core idea:* Use graph neural networks over a knowledge graph of events to locate faults.  
    - *Strength:* High reported accuracy (93% in top-3); formal approach.  
    - *Weakness:* Focused on microservice logs; real-time build speed unclear.  
    - *Relevance:* An example of graph+machine-learning for RCA.  
  - **LogKG (Zhang et al., IEEE 2023)** – (cited in OpsAgent) builds a knowledge graph from log templates and system topology for diagnosis【29†L1-L4】. It’s a precursor idea of mapping log events into a graph.  
  - **KG-based RCA (Other)** – “AetherLog” (ICSE 2024) uses LLMs on log templates to refine anomaly decisions (RAG style); “Praxis” (ICSE 2023) integrates static code analysis with monitoring; other works combine anomaly detectors with domain KGs【31†L6-L15】. We highlight especially that pure RAG approaches are seen as unreliable【24†L81-L90】【33†L52-L61】, motivating structured context.  

- **Graph-Based Retrieval and Memory:**  Outside SRE, general *temporal graph retrieval* research is relevant.  For example, Zhu et al. (2025) propose a **STAR-RAG** framework for “Time-aligned GraphRAG” that builds a summarized rule graph for efficient QA on temporal KGs【18†L35-L44】. They show that enforcing temporal proximity in retrieval improves accuracy and efficiency. Adnan Masood’s “Context Graphs” (2026) conceptualises a governed graph layer for LLMs, emphasizing explainability (answer + evidence paths + provenance)【33†L52-L61】. These works underscore that merging graph structure with retrieval yields better factual consistency – a lesson directly pertinent to P2’s emphasis on correctness under drift.  

Below is a summary list of surveyed items, each with a brief note on strengths/weaknesses and relevance:

- **Ciroos (Signal Intelligence)** – Product. Dynamic knowledge graph as persistent system memory【37†L236-L244】. *Strength:* End-to-end context learning, cross-domain. *Weak:* Proprietary, enterprise cost. *Relevance:* Exemplifies the “learn once, recall forever” principle.  

- **Chronosphere Guided Troubleshooting** – Product. Temporal Knowledge Graph of infrastructure and telemetry【35†L333-L337】, with LLM-driven investigation notebooks. *Strength:* Integrates analytics (DDx, leaf error tracing) to separate noise. *Weak:* Limited availability, cloud-centric. *Relevance:* Advanced example of graph-based context and learning from each incident【35†L408-L416】.  

- **Observe Inc. (AI SRE Agent)** – Product. Open data lake + central knowledge graph used by AI-SRE agents【22†L127-L134】. *Strength:* Scalable lake, built-in knowledge graph, native integrations (via MCP). *Weak:* Also proprietary. *Relevance:* Shows industry trend of combining raw data with knowledge graphs for LLM-based SRE.  

- **Grafana Knowledge Graph** – Feature/Doc. Grafana’s knowledge graph auto-discovers services, pods, databases, etc., and connects telemetry for RCA【13†L64-L72】【13†L90-L99】. *Strength:* Fully integrated into Grafana ecosystem, easy activation. *Weak:* Not open for arbitrary customization, mostly visual. *Relevance:* Highlights how context graphs surface multi-service impact (used in RCA workbench).  

- **Rootly Graphify (OSS)** – Library. Parses incident notes/tickets into a graph of entities and relations【9†L1-L8】. *Strength:* Open-source, extensible. *Weak:* Depends on unstructured data; may miss low-level telemetry. *Relevance:* Demonstrates building a cumulative incident graph via NLP and knowledge accumulation.  

- **IncidentFox (OSS)** – Project on GitHub. Slack-based AI SRE that correlates logs, metrics, deployments by ingesting codebase and incident history【23†L99-L107】. *Strength:* End-to-end Slack workflow, multi-tool integration, active project. *Weak:* Early stage (GitHub “demo”), unknown maturity. *Relevance:* Good example of combining graph context with an interactive UI.  

- **Bacca.ai (Startup Blog)** – Company. Uses a proprietary KG to encode system state, telemetry, past incidents and human steps【24†L123-L132】【24†L147-L156】. *Strength:* Focus on structured causal model and continuous learning. *Weak:* Technology unreleased/closed. *Relevance:* Reinforces that KGs (not raw RAG) are preferred for AI SRE agents【24†L79-L88】.  

- **OpsAgent (Paper, 2025)** – ArXiv. A modular multi-agent incident diagnosis system. Ingestion of metrics/logs/traces into a unified description for three experts; uses CoT and cross-review to produce an auditable report【30†L392-L400】【30†L415-L423】. *Strength:* Provable audit trail, continual self-improvement. *Weak:* Very complex; offline training pipeline. *Relevance:* Offers a blueprint for aligning data and roles to improve consistency of context.  

- **KGroot (Paper, 2024)** – ArXiv. Constructs a historical event **Knowledge Graph (FEKG)**, then builds a real-time “online graph” per incident. A graph-convolutional network ranks possible root causes【32†L51-L60】. *Strength:* High accuracy in tests; uses both historical and live graphs. *Weak:* Focused on structured microservices logs. *Relevance:* Shows advanced use of GCNs on temporal graphs for RCA.  

- **LogKG (Paper, 2023)** – IEEE TSC. Transforms logs into a KG to pinpoint failures (cited by OpsAgent). *Strength:* Early effort in log→graph mapping. *Weak:* Requires log templates. *Relevance:* Confirms KG can drive log-based diagnosis.  

- **STAR-RAG (Paper, 2025)** – ArXiv. Proposes a *time-aligned* Graph-RAG method: build a summarized temporal “rule graph” and apply personalized PageRank to restrict retrieval to relevant time windows【18†L35-L44】. *Strength:* Grounding QA in temporal structure. *Weak:* More QA-focused, not full SRE context. *Relevance:* Underlines the importance of temporal constraints in any retrieval over evolving data.  

- **Context Graphs (Blog, 2026)** – Article by Adnan Masood. Defines “Context Graphs” as a governed memory layer for LLMs, linking entities/events to answer *why*, not just what【33†L52-L61】. Emphasizes explainability (answer + evidence paths). *Strength:* Theoretical framework for auditability. *Weak:* Conceptual, no implementation. *Relevance:* Aligns with P2’s need for traceability and governed context to avoid “hallucinations.”  

- **Other Relevant Works:** Several frameworks (e.g. AIOps anomaly+KG) and platforms (Dynatrace Davis AI, hybrid APM/AI tools) exist but are less directly tuned to “persistent memory.” We note that mainstream research (and P2’s hint) downplays raw LLM QA in favor of **structured context**【24†L81-L90】【33†L52-L61】.

### Summary of Surveyed Items  

| Name / Source                          | Type (Paper/OSS/Prod)      | Core Idea                                     | Maturity             | Integration Ease | Likely P2 Performance | Notes                         |
|----------------------------------------|----------------------------|-----------------------------------------------|----------------------|------------------|----------------------|-------------------------------|
| Ciroos Signal Intelligence             | Product (Enterprise)       | Evolving knowledge graph as SRE persistent memory【37†L236-L244】 | Commercial, mature   | Moderate (API)    | High (context recall) | Strong cross-domain context   |
| Chronosphere Guided Troubleshooting    | Product                    | Temporal knowledge graph + AI suggestions【35†L333-L337】【35†L440-L447】 | GA/Limited preview   | Low (closed)      | High (RCA support)    | Focus on Notebook + feedback  |
| Observe AI-SRE Agent                   | Product (Newswire)         | Data lake + central KG for AI agents【22†L127-L134】 | Commercial, rolling  | Moderate (open API)| High (scales)        | Strong LLM + KG integration   |
| Grafana Cloud Knowledge Graph          | Product/Docs               | Discovered service graph, insights-driven RCA【13†L64-L72】 | Released by Grafana  | High (built-in)  | Medium (visual RCA)  | Useful for triage, less queryable |
| Rootly Graphify                        | OSS (Python lib)           | NLP-driven incident knowledge graph【9†L1-L8】   | Alpha-stage on PyPI  | High (pip install) | Medium (text only)   | Handles Slack/incident notes  |
| IncidentFox (GitHub)                   | OSS (Slackbot)             | AI SRE Slackbot correlating multi-tool data【23†L99-L107】 | Early open-source    | Moderate (setup)  | Medium (proof-of-concept) | Good concept, still small     |
| Bacca.ai Knowledge Graph               | Product/Blog               | Proprietary KG + agent learns from ops data【24†L123-L132】【24†L147-L156】 | Product (closed)     | Low               | High (structured KG) | Emphasizes determinism & audit |
| OpsAgent (Huang et al., 2025)          | Paper (ArXiv)              | Multi-agent incident diagnosis with unified evidence【30†L392-L400】 | Research prototype   | Low               | High (holistic)      | Very thorough but complex     |
| KGroot (Wang et al., 2024)             | Paper (ArXiv)              | Fault-event knowledge graph + GCN ranking【32†L51-L60】 | Prototype (code avail) | Moderate         | High (accuracy)      | Targets microservice logs     |
| LogKG (Shen et al., 2023)              | Paper (IEEE)               | Builds KG from logs for failure diagnosis【29†L1-L4】 | Published research   | Low               | Medium              | Foundational, older method    |
| STAR-RAG (Zhu et al., 2025)            | Paper (ArXiv)              | Temporal RAG with graph summarization for QA【18†L35-L44】 | Preprint            | Low               | Medium              | Highlights temporal retrieval |
| Context Graphs (Masood, 2026)          | Blog/Essay                 | Theory of governed context graphs for LLMs【33†L52-L61】 | Conceptual essay     | N/A              | Medium (guidance)     | Emphasizes explainability     |
| (Other) AIOps Hybrid Methods           | Varied                     | Combo of anomaly detection + KGs【31†L6-L15】        | Mixed (academic)      | Low               | Medium             | Shows combining data-driven + domain KB |

*(“Maturity” and “Integration Ease” are rough qualitative estimates: e.g. commercial products vs research. “Performance” is speculation on how well each approach meets P2’s criteria.)*

## 3. Improvement Opportunities and Unique Features  

Based on the above survey and brainstorming, we identify several **concrete improvements/features** to distinguish our P2 solution. For each, we outline the technical approach, required components, and estimated effort (for a 3-person team over 24h/48h/72h):

1. **Entity Normalization & Renaming Map**  
   *Approach:* Build a normalization layer that maps ephemeral IDs (pod names, container IDs, hostnames) to stable service/component names. Techniques include regex rules, lookup tables from CI/CD metadata, or embedding-based clustering. Maintain a dictionary of historical renames (e.g. service “auth-v1” → “auth-v2”).  
   *Components:* Preprocessing script (Python) to scan config or logs for name patterns; a simple database (SQLite/JSON) of entity aliases. Possibly use tokenizers or word embeddings for fuzzy match.  
   *Effort:* 24h – implement basic regex/table lookup; 48h – add semi-automated alias detection using text matching; 72h – refine with ML-based name similarity (e.g. edit distance, embedding) and UI to edit mappings.  

2. **Temporal Knowledge Graph Backend**  
   *Approach:* Use a graph database (e.g. Neo4j, TigerGraph or an in-memory NetworkX graph) to store entities and events. Each node has timestamps (lifetime, version), and edges represent interactions or causality. The graph evolves as new telemetry arrives.  
   *Components:* Graph DB server or Python networkx/IGraph; schema design (nodes: service, host, alert, deployment; edges: “runs_on”, “depends_on”, “alerts_at”, “causes”). ETL pipeline to insert updates.  
   *Effort:* 24h – choose a simple in-memory graph (NetworkX), ingest small sample; 48h – upgrade to persistent store (Neo4j Docker) with indexes; 72h – add historical versioning (effectivity dates, e.g. label nodes with active ranges) for topology-drift.  

3. **Hybrid Retrieval: Graph + Vector Search**  
   *Approach:* Combine a **graph query engine** (for structured pattern search) with a **vector similarity search** (for fuzzy matching on log text). For example, use Neo4j (or Cypher queries) to follow edges and find related nodes, and use a library like Faiss or OpenSearch for embedding-based similarity of incident descriptions.  
   *Components:* Pre-trained embedder (e.g. Sentence-Transformers) for logs/alerts; vector index (Annoy/Faiss/Weaviate). Query logic that fetches candidates from both methods and merges by score.  
   *Effort:* 24h – implement one method (e.g. vector recall of similar incident logs); 48h – implement both and simple rank fusion (e.g. max score); 72h – tune weights and allow “fallback” to vector if graph returns no result.  

4. **Causal Chain Reconstruction Algorithm**  
   *Approach:* Given a set of correlated events, infer likely cause-effect relations. Methods include: temporal ordering (assuming earlier events may cause later ones), cross-correlation of time-series anomalies, or using explicit causal inference (e.g. Granger causality or rule mining). One option: treat events as nodes with a directed edge if event A consistently precedes B (over multiple incidents) or if logs indicate a causal keyword.  
   *Components:* Time-series processor (pandas), statistic tests for lead-lag, or sequence mining (prefix-span). Graph algorithm (e.g. find longest path, critical path).  
   *Effort:* 24h – baseline: assume causality by ordering timestamps; 48h – add simple correlation metrics (Pearson on anomaly vectors); 72h – implement a refined technique (e.g. build partial order via mutual information or use a GCN classifier on event pairs).  

5. **Continual Learning from Feedback**  
   *Approach:* After each “incident resolution” (ground truth), update the memory. For example, if the user confirms a diagnosis, add it as a new graph substructure, and reinforce edges along the shown causal path. Could implement as graph weight updates or storing “templates” of confirmed incidents.  
   *Components:* Feedback API (e.g. accept a resolved incident JSON), update routines (increment edge weights, store resolution notes). Optionally incorporate an LLM to summarize final investigation text into facts.  
   *Effort:* 24h – simple: on feedback, append nodes/edges to graph; 48h – track “frequency” or weight of graph patterns; 72h – incorporate pattern mining (e.g. find common subgraphs) to generalize from multiple cases.  

6. **Explainable Evidence Path Generation**  
   *Approach:* Alongside retrieving past incidents, generate a human-readable explanation. E.g. retrieve not just matching incident IDs, but the *path* in the graph linking the query symptoms to stored context. This could be a sequence of nodes/edges (service A → error X → service B). Provide confidence scores on each link.  
   *Components:* Query engine that returns graph subpaths; template text to describe steps; rule engine to produce a final narrative.  
   *Effort:* 24h – output graph IDs or names; 48h – build simple sentence templates (“Service A experienced high CPU → it called Service B which errored”); 72h – polish wording, include metric screenshots or links, possibly integrate a small LLM prompt with the subgraph for natural phrasing.  

7. **Topology-Drift Robustness (Time-aware Queries)**  
   *Approach:* When querying, consider time validity: e.g. if a service was renamed, allow historical names in queries. Maintain a mapping of name→(time range) so queries can match old names appropriately. Also index edges by time; a query “during time T” will use only nodes/edges active then.  
   *Components:* Augment graph schema with temporal attributes (create_time, end_time). Query interface that accepts time constraints.  
   *Effort:* 24h – tag nodes with a version; 48h – implement query filters (e.g. Cypher `WHERE timestamp < event.time`); 72h – build a UI slider to select time horizons and show graph evolution visually.  

8. **Pattern Recognition / Graph Embedding**  
   *Approach:* Use graph-mining or embedding techniques to detect repeated failure patterns. For example, apply node2vec or GNN to cluster similar subgraphs of incident pathways. Then use these clusters to suggest likely root causes for new incidents (pattern matching).  
   *Components:* Graph embedding library (e.g. PyTorch Geometric), clustering (KMeans). Offline training on accumulated graph.  
   *Effort:* 24h – treat each incident path as text and do TF-IDF clustering; 48h – run node2vec on the graph and cluster vectors; 72h – integrate a GCN (like KGroot) to do end-to-end scoring and output top-K patterns for each query.  

9. **User-friendly Visualization/Notebook**  
   *Approach:* Provide an interactive “Investigation Notebook” akin to Chronosphere: timeline charts of alerts, graph diagrams of dependencies, with annotations of the inference steps. Judges love demos they can click through. Use Mermaid or d3.js for diagrams.  
   *Components:* Web UI (Flask/Streamlit), Mermaid-js graph views (for causal chains), timeline charts (Plotly).  
   *Effort:* 24h – static diagrams for a sample incident; 48h – integrate with queries (dynamically show a subgraph); 72h – add filtering (e.g. by service type) and explanation text.  

Each of these features is technically feasible with standard open-source tools and pre-trained models (e.g. HuggingFace transformers for embeddings, NetworkX/Neo4j for graphs). We will prioritise based on contest time: for instance, we would at least implement (1) normalization, (2) basic graph storage, (3) simple retrieval, and (4) a rudimentary explanation path in the first 24–48h, then add advanced components (learning, visualization) as time permits.

## 4. Development Pipeline & MVP Architecture  

We propose the following **pipeline architecture** (Mermaid diagram below):

```mermaid
flowchart LR
  subgraph Ingestion
    A[Raw Telemetry] --> B[Entity Extractor/Normalizer]
  end
  B --> C[Temporal Graph DB]
  B --> D[Vector Index (Embeddings)]
  C --> E[Query & Retrieval]
  D --> E
  E --> F[Ranking & Explanation]
  F --> G[Answer / Dashboard]
  F --> H[Memory Update]
  H --> C
```

- **Data Flow:** Telemetry logs/events (**A**) in JSON/line format are first processed by an *Extractor* that pulls out entities (services, hosts, error codes) and metrics. A **Normalization** module maps raw fields to canonical node/edge types (handling synonyms and renames). The normalized elements feed into a **Temporal Graph DB** (e.g. Neo4j or an in-memory graph) as nodes/edges with timestamps. Simultaneously, textual parts (error messages, descriptions) are fed to an **Embedding** service (Sentence-Transformer) to build a vector index for similarity search. 

- **Query Handling:** A user’s query (e.g. “why did API latency spike yesterday at midnight”) triggers both: (a) a graph query (e.g. traverse from nodes matching “API” events to their causal factors) and (b) a nearest-neighbor search over incident vectors. The **Retrieval** stage (E) merges these candidates and computes a combined score.

- **Ranking & Explanation:** Results are ranked (e.g. by recency, similarity, context overlap). We produce an *evidence path*: for graph matches, the path in the graph (nodes + relation labels); for vector matches, the nearest incident ID plus a highlight of similar symptoms. We attach confidence and generate a textual explanation (possibly via templates or a lightweight LLM inference on the evidence graph). 

- **Answer/Output:** The system returns the most relevant past incident(s) and an explanation trail. In a web demo, we’d show the graph path diagram and relevant metrics.

- **Continuous Learning:** When an investigation concludes (user labels it solved), the findings are written to the memory: new nodes/edges (e.g. linking “cache misconfiguration” to a symptom) are appended or updated in the graph (H). This improves future recall and “Memory Evolution.”  

**Data Format:** We assume input telemetry as JSONL (each line: `{timestamp, service, host, message, metric, value, tags...}`). We’ll define a normalized schema, e.g. `Service`, `Host`, `Event`, and relations like `HOSTED_ON`, `EMITS`, `DEPENDS_ON`, etc. Benchmarks will provide sample incidents with fields; we may adapt those or use our own sample dataset.

**Temporal Graph Design:** Each entity node carries attributes (e.g. name, type, active_time). Each edge has an optional timestamp or effective interval. We may use Neo4j’s temporal features, or encode time as node labels. Entities like “auth-service” will have version history via separate nodes or time-valid ranges. This handles topology drift: e.g. if “auth” was v1 then v2, queries constrained by incident date will join to the correct node.

**Retrieval Algorithms:**  
- *Graph Retrieval:* Use pattern queries (Cypher or networkx traversal). E.g. find all paths from a failed service to any database, or match subgraphs. Possibly use graph embeddings (like GraphSAGE) for similarity.  
- *Vector Retrieval:* Compute embeddings of incident logs (and query if textual) using a pre-trained encoder (e.g. Sentence-BERT). We keep a Faiss index of incident vectors for nearest-neighbors search.  

**Evaluation Harness:** We will adapt the official benchmark (likely a set of query-and-answer pairs for incidents). The harness likely measures recall@k or F1 on incident matching tasks. We’ll run our system on held-out queries to compute metrics aligning with “Incident recall” (e.g. fraction of relevant incidents retrieved) and “Latency” (end-to-end time).  

**Testing Plan:** Unit-test parsing/normalization; integration tests with small synthetic incidents; end-to-end test on sample queries. We’ll include simple baselines: e.g. an Elasticsearch/Kibana style keyword search or a pure embeddings RAG system (using e.g. OpenAI’s GPT-2 embedder) as fallbacks to show our system’s superiority under drift. 

In summary, the pipeline prioritises correctness over brute force: structured graph storage, explicit join on time and entities, and multi-modal retrieval. Next, we detail five demo concepts to illustrate explainability.

## 5. Demo Ideas (Judge-Friendly, Explainability-Focused)  

1. **“Causal Path Explorer”:** Given an alert or query symptom, show an interactive graph of the discovered causal path. Nodes (services, events) are clickable; edges labelled with reasoning (e.g. “CPU spike → service slow-down”). The judge can hover to see evidence (log snippets or metric charts) supporting each link. This directly visualizes *why* the system thinks those events are related.  

2. **“Incident Playback Notebook”:** Similar to Chronosphere’s notebooks. We simulate an incident timeline: plotting relevant metrics (e.g. CPU, error rate) and overlays of detected anomalies. As the user steps through time, the system annotates with suggestions/hypotheses (e.g. “Payment service failed 2 min before checkout errors”【35†L380-L388】). This shows how our system correlates signals.  

3. **“Graph vs. Text Comparison”:** Demonstrate a side-by-side of “Our Graph Retrieval” vs. “Keyword Search/RAG” on a query. For a tricky renaming scenario, show that naive text search fails to match a renaming, whereas our graph finds the root cause via history. E.g. “find similar incident to service-X crash” – the graph layer resolves that X was previously called Y.  

4. **“Learning over Time”:** Show a dataset of two similar incidents at different times. First query: system finds moderate similarity. After “learning” (simulated feedback), re-run the query to show higher confidence and faster retrieval. Optionally illustrate adding a node to the graph from the first incident so the second query succeeds. This highlights *memory evolution*.  

5. **“What-If Topology Change”:** Let the judge toggle a “topology version” mode: for example, simulate that a service was renamed between versions. Show that asking a query in the “old” context vs “new” context maps correctly (thanks to our rename mapping). This emphasizes robustness to drift.  

Each demo will emphasise **explainability**: we will not only output answers but show the underlying context paths, metrics, and “why” behind suggestions. The judge should see how the engine “remembers” past incidents and adapts.

## 6. Risk Analysis and Milestones  

**Key Risks:**  
- *Data Quality/Volume:* The available telemetry may be sparse or synthetic. If few labeled incidents exist, patterns may not emerge. *Mitigation:* We will generate toy data and fallback to simple rule-based matching if needed. Also, pre-train any embedding models on similar domains (e.g. using dummy logs).  
- *Model Hallucination:* Using any LLM for explanation risks fabricating facts. *Mitigation:* We restrict LLM use to formatting known facts (templates), and always cite the evidence path from our graph.  
- *Topology Complexity:* Handling arbitrary schema or too-frequent changes may break simple approaches. *Mitigation:* Implement robust normalization early, and if impossible cases arise, document them. Possibly use cloud APIs for service registry.  
- *Performance/Latency:* Graph queries on large data could be slow. *Mitigation:* Time-box indexing (we will downsample or index only key fields in 72h). Use caching for repeated queries.  
- *Time Constraint:* 72 hours is short. *Mitigation:* Break tasks into milestones below, focusing on core functionality first.  

**Mitigations:** Besides the above, we plan to keep the solution modular. If time runs short, we will ensure at least a minimal graph and retrieval demo works, then add complexity. We will use containerized or local databases (to avoid cloud deploy delays), and rely on available libraries (e.g. Neo4j via Docker, HuggingFace models).

**Proposed Milestones:**  
- **Day 1 (24h):** Set up ingest/normalization pipeline. Implement a basic graph (NetworkX) or simple key-value memory. Implement a naive retrieval baseline (text search or single-step graph). Show a static demo of retrieving one past incident based on matching service names or IDs.  
- **Day 2 (48h):** Extend graph to hold multiple incidents. Add embeddings + vector search. Implement the query interface merging graph and vector candidates. Add a simple explanation generation (e.g. print the graph nodes involved). Run initial evaluation with the benchmark or sample queries.  
- **Day 3 (72h):** Polish UI and explanation (Mermaid diagrams, text). Implement at least one of the advanced features (e.g. entity rename mapping or dynamic learning). Optimize any slow queries. Finalise evaluation: measure recall/precision and response time on test cases. Prepare demo scenarios (from Section 5) for presentation.  

These milestones are tight but sequential: core retrieval first, then enhancements. We will prioritize features (1)-(5) in order: normalization, graph store, hybrid search, explanation, topology handling. Visualization and learning (features 6-9) will be last if time remains.

## 7. Core Components (Libraries & Code Snippets)  

We plan to use primarily Python. Key libraries:  
- **Graph Storage:** [NetworkX](https://networkx.org) for prototyping; possibly Neo4j (via `neo4j` Python driver) if scale needed.  
- **Vector Search:** [Faiss](https://github.com/facebookresearch/faiss) or [Annoy](https://github.com/spotify/annoy) for ANN indexing; [HuggingFace Transformers](https://huggingface.co) for sentence embeddings (e.g. `sentence-transformers/all-MiniLM-L6-v2`).  
- **Time-Series Analysis:** Pandas, SciPy (for correlation).  
- **LLM/Language:** [OpenAI GPT-3.5/4] (if allowed) or local model (LLaMA/Llama-2) for any summarization or complex query (but we avoid hallucination).  
- **Visualization:** [Mermaid](https://mermaid-js.github.io/) (can embed graphs via Markdown), [Matplotlib]/[Plotly] for charts.  

### Pseudocode Snippets

**Entity Normalization Example:**  
```python
# Example dictionary for known renames
name_alias = {
    "auth-service-v1": "auth-service",
    "auth-service-v2": "auth-service",
    "payment-svc-prod": "payment-service"
}
def normalize_entity(raw_name):
    # Remove version tags or match aliases
    base = raw_name.split("-")[0]  # naive heuristic
    return name_alias.get(raw_name, name_alias.get(base, base))
# Example usage:
normalize_entity("auth-service-v2")  # -> "auth-service"
```

**Graph Insertion:**  
```python
import networkx as nx
G = nx.DiGraph()
def add_event_to_graph(event):
    # event: dict with keys 'service', 'host', 'type', 'time', etc.
    svc = normalize_entity(event['service'])
    node_s = f"Service:{svc}"
    node_e = f"Event:{event['type']}@{event['time']}"
    # Add nodes and edge
    G.add_node(node_s, label="Service", name=svc)
    G.add_node(node_e, label="Event", time=event['time'])
    G.add_edge(node_s, node_e, relation="emitted")
```

**Causal Chain Reconstruction (Sketch):**  
```python
def infer_causal_edges(graph, events):
    # events: list of event nodes sorted by time
    for i, e1 in enumerate(events):
        for e2 in events[i+1:]:
            # Simple rule: if two events share a component or service, connect them
            common = set(graph.predecessors(e2)).intersection(set(graph.successors(e1)))
            if common:
                graph.add_edge(e1, e2, relation="maybe-causes")
```

**Ranking Candidate Incidents:**  
```python
def score_incident(query_vec, incident_vec, incident_time, current_time):
    sim = cosine_similarity(query_vec, incident_vec)  # e.g. from numpy
    time_diff = abs((current_time - incident_time).days)
    # Example scoring: decay older incidents, weight by similarity
    score = sim * exp(-alpha * time_diff)
    return score
```

**Retrieval Flow (Simplified):**  
```python
def retrieve(query):
    # 1. Vector-based recall
    q_vec = embed(query.text)
    vec_cands = vec_index.search(q_vec, top_k=10)
    # 2. Graph-based recall (e.g. find nodes with same type or known error code)
    graph_cands = graph_query(query)
    # 3. Combine and rank
    final_cands = {}
    for cand in vec_cands + graph_cands:
        final_cands[cand.id] = final_cands.get(cand.id, 0) + cand.score
    # Sort by combined score
    return sorted(final_cands.items(), key=lambda x: x[1], reverse=True)
```

These snippets illustrate the core logic. In practice we’d refine each with error handling, data structures, and logging.

## 8. Sources  

We cite relevant sources for factual claims and design inspirations:

- Ciroos press release on Persistent Context / knowledge graph【37†L236-L244】  
- Grafana Cloud Knowledge Graph docs【13†L64-L72】  
- Chronosphere AI-Guided Observability blog【35†L333-L337】【35†L440-L447】  
- Observe Inc. AI-SRE announcement【22†L127-L134】  
- Rootly Graphify blog post【9†L1-L8】  
- IncidentFox Reddit announcement【23†L99-L107】  
- Bacca.ai “Why Knowledge Graphs Beat RAG” blog【24†L79-L88】【24†L103-L112】  
- OpsAgent (Huang et al., 2025)【30†L392-L400】【30†L415-L423】  
- KGroot (Wang et al., 2024)【32†L51-L60】  
- Zhu et al. (STAR-RAG, 2025)【18†L35-L44】  
- Masood (Context Graphs, Medium 2026)【33†L52-L61】  

Each source is cited above at the relevant point. These form the basis of our technical plan and justify our approach to the P2 challenge.  

