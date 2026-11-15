# Orbit

A property graph built from GitLab SDLC data and source code. You query it over HTTP, gRPC, and MCP.

## Language

### Product

**GitLab Orbit**:
The product, previously named GitLab Knowledge Graph. It builds a property graph from GitLab data and serves user-scoped queries. It has two parts: the **GitLab Orbit API** and the **GitLab Orbit CLI**. In docs, do not shorten it to Orbit.
_Avoid_: KGaaS, Knowledge Graph Service, Orbit Remote, "Orbit service"

**GKG**:
The retired engineering abbreviation for Orbit. It stays only in existing names, such as the `gkg-server` binary, metrics, and pinned wire and database names.
_Avoid_: GKG in user-facing text

**GitLab Orbit API**:
The REST, GraphQL, and MCP endpoints of **GitLab Orbit** on a GitLab instance. The **GitLab Orbit CLI**, agents, and GitLab Duo Agent Platform call them.
_Avoid_: remote graph, "the server", "production GKG"

**GitLab Orbit CLI**:
The `orbit` binary. `glab orbit` installs it and forwards verbs to it. Some verbs call the **GitLab Orbit API**, some use only the **Local graph**, and `grep` and `context` use both. Run `orbit --help` for the current verbs.
_Avoid_: Orbit CLI, Orbit Local, "local mode", "remote mode"

**Local graph**:
The DuckDB graph that the **GitLab Orbit CLI** builds from a repository on your machine. It covers local code that the GitLab instance does not have.
_Avoid_: Orbit Local

**Skill Placeholder**:
An HTML comment in the skill that the **GitLab Orbit API** serves. It marks where the CLI inserts a section from its embedded local skill. Each placeholder must match one local section.
_Avoid_: include directive

### Graph model

**Property Graph**:
A data model where typed **Nodes** carry properties and typed, directed **Relationships** connect them.
_Avoid_: knowledge graph (too vague), graph database (the storage layer)

**Ontology**:
The YAML schema of the graph. It is the single source of truth for what the graph can contain.
_Avoid_: schema (too generic), data model (the broader design)

**Node**:
A typed entity that represents a GitLab object, such as Project, MergeRequest, File, or Definition. The **Ontology** defines each node type. Each type has its own table.
_Avoid_: vertex

**Relationship**:
A typed, directed connection between two **Nodes**, such as `AUTHORED` or `IN_PROJECT`. The **Ontology** defines the valid source and target types.
_Avoid_: interaction, link

**Derived Entity**:
An **Ontology** entry with **Pipelines** but no node table. A Rust transform turns the extracted rows into **Relationships**. Use it when one SQL row cannot express the graph shape, for example, SystemNote (ADR 015).
_Avoid_: storageless node, derived node

**Pipeline**:
The **Ontology** ETL unit, with `extract` and `transform` sections. **Nodes**, **Relationships**, and **Derived Entities** all use it.
_Avoid_: old-style `etl` block

**WorkItem**:
The one **Node** type for all units of work, such as issues, epics, tasks, incidents, and tickets. The `work_item_type` property tells them apart. There are no Issue or Epic node types.
_Avoid_: Issue, Epic as node types

### Graph partitions

**SDLC Data**:
The sub-graph of GitLab platform entities, such as projects, groups, merge requests, work items, pipelines, and users. Indexed from the **Datalake**.
_Avoid_: namespace graph

**Code Graph**:
The sub-graph of source code structure: branches, directories, files, definitions, imports, and their connections. Built from repository contents through Gitaly.
_Avoid_: call graph (only invocation edges)

**YAML Document Type**:
A config that tells the **Code Graph** which YAML files it claims, by filename, directory, or top-level keys. It also names the keys that become definitions or imports. Unclaimed YAML keeps only its `File` node, anchors, and aliases.
_Avoid_: YAML dialect, YAML schema

### Authorization

**Traversal Path**:
The slash-delimited ancestor **Namespace** IDs of an entity, for example `"42/100/1000/"`. The first segment is the **Organization**. A query returns a row only when the user can access the row's path or one of its ancestors.
_Avoid_: traversal ID, traversal_ids (the array form of the same data)

**Namespace**:
A GitLab group at any level. The top-level group is the unit of indexing dispatch and deletion. The graph stores it as a Group **Node**.
_Avoid_: tenant

**Organization**:
The top-level tenant boundary. Storage keeps the data of each organization apart.
_Avoid_: org, tenant

**Redaction**:
Authorization after the query. The service asks GitLab Rails to check each resource and removes the rows that the user cannot access. It covers cases that **Traversal Path** filters cannot, such as confidential issues and role-gated entities.
_Avoid_: filtering (too generic), content masking (rows are removed, not masked)

### Data pipeline

**CDC (Change Data Capture)**:
Capture of row-level changes from a source database as a stream of events. In Orbit, changes flow from GitLab PostgreSQL through **Siphon** into the **Datalake**.
_Avoid_: replication

**Siphon**:
The GitLab CDC service. It publishes PostgreSQL replication events to NATS JetStream. It is not part of Orbit.
_Avoid_: CDC bridge, producer

**Datalake**:
The ClickHouse database of raw CDC rows from **Siphon**. It is the source for the ETL into graph tables.
_Avoid_: data lake, raw data tables, lake

**Dispatch ID**:
A UUID on each indexing request. It groups the requests of one dispatch, for example, one namespace in one cycle.
_Avoid_: request ID, trace ID

**Campaign**:
The correlation ID above **Dispatch ID** for one full re-index, such as a schema migration. When no re-index runs, it has no value. Use it to add up the cost of one re-index across pipelines.
_Avoid_: batch, job

**Migration ledger**:
The list of schema version bumps. Each entry says how much of the graph that version invalidates: all, SDLC, code, or none. A schema migration reads it to decide what to clone and what to rebuild. `mise schema:bump` writes the entry.
_Avoid_: changelog, migration script

### Query system

**GQL**:
The read-only graph query language of **GitLab Orbit**, similar to OpenCypher. Clients write all queries in GQL.
_Avoid_: Orbit Query

**Query DSL**:
The JSON query language that GQL replaced. GitLab 19.6 removed it. Only older instances accept it.
_Avoid_: intermediate query language, JSON query language

**Named Query**:
A query defined in YAML and called by name. CI checks each definition against the **Ontology**.
_Avoid_: preset query, query template

**Hop**:
One **Relationship** traversal. A query can have at most 3 hops.
_Avoid_: depth

**Denormalized Join**:
An **Ontology** setting that pre-joins a chain of tables into one table. The compiler then answers the **Hops** along that chain with one scan. ClickHouse materialized views keep it current.
_Avoid_: materialized table, projection

**Hydration**:
Fetching **Node** properties found during the query, when the result node types are not known before the query runs.
_Avoid_: enrichment, decoration

**TOON (Token-Oriented Object Notation)**:
The compact text encoding for every `format=llm` response. It replaced GOON (ADR 012).
_Avoid_: GOON, LLM format

**GQL Response Format**:
An OpenCypher-like result table for `format=gql` responses. Columns are node aliases, and cells are node literals (ADR 019).
_Avoid_: query output table
