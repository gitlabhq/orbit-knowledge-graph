# Orbit

A property graph built from GitLab instance data — SDLC metadata and source code structure — queryable over HTTP, gRPC, and MCP.

## Language

### Product

**Orbit**:
The GitLab Knowledge Graph product. Builds a property graph from GitLab data and serves queries against it.
_Avoid_: KGaaS, Knowledge Graph Service

**GKG**:
Engineering abbreviation for Orbit. Used in binary names (`gkg-server`), config prefixes (`GKG_*`), and metrics. Stands for "GitLab Knowledge Graph."
_Avoid_: using GKG in user-facing contexts

**Orbit Remote**:
The hosted Knowledge Graph service, queried via `glab orbit remote`. Indexes all GitLab.com SDLC and code data; queries are user-scoped and JWT-authenticated.
_Avoid_: "the server", "production GKG"

**Orbit Local**:
A standalone CLI that indexes a single repository into a local DuckDB database for offline analysis. Managed via `glab orbit local`.
_Avoid_: "the CLI" (ambiguous — both Remote and Local have CLIs)

### Graph model

**Property Graph**:
A data model where typed **Nodes** carry properties and typed directed **Relationships** connect them. The foundational abstraction of Orbit.
_Avoid_: knowledge graph (too vague), graph database (refers to the storage layer)

**Node**:
A typed entity in the property graph representing a GitLab object (e.g., Project, MergeRequest, File, Definition). Each node type is defined in the **Ontology** and stored in a dedicated table (ClickHouse in **Orbit Remote**, DuckDB in **Orbit Local**). Called `entity` in the **Query DSL**.
_Avoid_: vertex

**Relationship**:
A typed directed connection between two **Nodes** (e.g., `AUTHORED`, `CONTAINS`, `IN_PROJECT`). Defined in the **Ontology** with one or more source→target **Node** type variants. Stored in physical edge tables.
_Avoid_: interaction, link

**Derived Entity**:
A third **Ontology** shape alongside **Node** and **Relationship**: a **Datalake** extract with no node table, whose rows a named Rust transform turns into **Relationships**. Declared per domain in `schema.yaml` and named via `etl.transform`; it stays dormant until that transform is registered in Rust. Used for entities (e.g. SystemNote) whose graph shape can't be a SQL row-projection — they need multi-hop datalake reads or free-text parsing. See ADR 015.
_Avoid_: storageless node, derived node

**Ontology**:
The YAML-defined schema of the property graph. Declares all **Node** types, **Relationship** types, **Derived Entity** definitions, their properties, and valid source→target pairings. Lives in `config/ontology/`. The single source of truth for what the graph can contain.
_Avoid_: schema (too generic), data model (refers to the broader design)

**WorkItem**:
The unified **Node** type for all trackable units of work — issues, epics, tasks, incidents, test cases, requirements, objectives, key results. Distinguished by the `work_item_type` property. There are no separate Issue or Epic node types.
_Avoid_: Issue, Epic (these are work item types, not separate graph entities)

### Graph partitions

**SDLC (Software Development Lifecycle) Data**:
The sub-graph of GitLab platform entities — projects, groups, merge requests, work items, pipelines, vulnerabilities, users. Distinguished from **Code Graph** data. Indexed from the **Datalake** via the SDLC indexing pipeline.
_Avoid_: namespace graph (outdated alias)

**Code Graph**:
The sub-graph of source code structure and relationships — branches, directories, files, definitions, imported symbols, and their connections (containment, calls, imports, inheritance). Distinguished from **SDLC Data**. Built by parsing repository contents via Gitaly.
_Avoid_: call graph (refers only to invocation relationships, not the full sub-graph)

**Namespace Partitioning**:
The physical `PARTITION BY` of every graph table carrying a **Traversal Path**, keyed by a hash bucket of the top-level **Namespace** (`sipHash64(top_level_ns) % N`, declared once in `settings.partition`). Gives each tenant bucket its own ClickHouse part budget so one tenant's reindex burst cannot dead-letter inserts for the rest. A query scoped to a single top-level namespace also prunes to one bucket: the compiler emits the same bucket expression as a predicate. A storage-layer property, distinct from the logical SDLC/Code sub-graphs and from the read-side extraction slices used for parallel initial loads.
_Avoid_: sharding (Orbit does not shard); conflating with the SDLC/Code "graph partitions" sub-graph split.

### Authorization

**Traversal Path**:
The slash-delimited ancestor **Namespace** hierarchy for an entity (e.g., `"42/100/1000/"`). Encodes **Organization**, group, and subgroup lineage. Used for hierarchical permission filtering — queries are scoped to paths the user is authorized for via prefix matching.
_Avoid_: traversal ID, traversal_ids (these refer to the array encoding of the same concept)

**Namespace**:
A GitLab group at any level of the hierarchy. The root namespace (top-level group) is the unit of indexing dispatch and deletion. The full namespace hierarchy is encoded in the **Traversal Path** of descendant entities. Represented in the graph as a Group **Node**.
_Avoid_: tenant (Orbit uses **Organization** for tenant isolation)

**Organization**:
The top-level tenant boundary. The first segment of every **Traversal Path**. All data within Orbit is segregated by organization at the storage layer.
_Avoid_: org, tenant (too informal / too generic)

**Redaction**:
Post-query authorization filtering where the service calls GitLab Rails to check per-resource permissions (`Ability.allowed?`). Rows the user cannot access are removed from the result. Handles cases that **Traversal Path** filtering cannot catch — confidential issues, runtime access controls, role-gated entities.
_Avoid_: filtering (too generic), content masking (misleading — entire rows are removed, not obscured)

### Data pipeline

**CDC (Change Data Capture)**:
The pattern of capturing row-level changes from a source database as a stream of events. In Orbit, CDC flows from the GitLab PostgreSQL database through **Siphon** into the **Datalake**.
_Avoid_: replication (too broad)

**Dispatch ID**:
A UUID stamped on each indexing request message, identifying one dispatch unit — per (namespace × cycle) for SDLC namespace dispatch, per cycle for the global and code dispatchers. Propagated to the `IndexingObserver` and tracing spans for correlation.
_Avoid_: request ID, trace ID (`dispatch_id` groups many requests, not a single one)

**Campaign**:
The parent correlation above **Dispatch ID**: one campaign per "re-index everything" decision, `null` in steady state. Today a campaign is a schema migration — opened (`migration-v<N>`) when the dispatcher marks a version `migrating`, attached to every dispatch while the migration runs, and closed when the migration completes (promotion to `active`). Held in process memory (`CampaignState`), not persisted. Lets analysts aggregate the cost of one re-index across pipelines without time-based joins.
_Avoid_: batch, job (a campaign spans many dispatches and both pipelines)

**Siphon**:
The GitLab CDC service. Captures PostgreSQL logical replication events and publishes them to NATS JetStream. External to Orbit — owned by the Analytics team.
_Avoid_: CDC bridge, producer

**Datalake**:
The ClickHouse database containing raw CDC rows replicated from GitLab PostgreSQL via **Siphon**. Tables are prefixed `siphon_`. The source data for ETL into graph tables.
_Avoid_: data lake, raw data tables, lake

### Query system

**Query DSL**:
The JSON-based query language for the property graph. Supports four query types: traversal, aggregation, path_finding, and neighbors. Compiled to parameterized ClickHouse SQL. Versioned by `QUERY_DSL_VERSION`.
_Avoid_: intermediate query language, intermediary LLM query language, JSON query language

**Named Query**:
A graph query defined in YAML under `config/named_queries/` and invoked by name, instead of the client authoring the **Query DSL** string. Compiled against the ontology at `gkg-server` build time so drift fails the build.
_Avoid_: preset query, query template

**Hop**:
A single **Relationship** traversal in the graph. Multi-hop queries traverse multiple relationships in sequence. Hard-capped at 3 hops for security and performance.
_Avoid_: depth (ambiguous with tree depth)

**Hydration**:
Fetching properties for **Nodes** discovered dynamically during query execution. Required for PathFinding and Neighbors queries where the result set's node types aren't known upfront.
_Avoid_: enrichment, decoration

**GOON (Graph Object Output Notation)**:
A line-oriented text format for representing graph query results compactly. Designed for LLM consumption — measured at −11% cost, −15% duration, and +4.8pp correctness vs raw JSON on Haiku 4.5 (ADR 012). Used when queries specify `format=llm`.
_Avoid_: LLM format, text format
