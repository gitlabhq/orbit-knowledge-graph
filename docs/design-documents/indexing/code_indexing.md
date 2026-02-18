---
title: Code Indexing Architecture
description: High-level guide to the GitLab Knowledge Graph code ETL pipeline and how the call graph lands in the database.
---

### Code Indexing ETL

This document outlines the approach of building the ETL pipeline for the Code Indexing in the `gkg-indexer`. The difference between code and SDLC entities is that we have to take into account that code versions can exist in parallel to each other via different branches.

If we want the Knowledge Graph to be able to ask questions about code, it needs to be able to understand the relationships at any given branch and at different commits.

The ETL pipeline will be responsible for:

- Reading the code from the GitLab repositories using Gitaly RPC calls
- Transforming the code into the desired format, including the call graph and the filesystem hierarchy
- Writing the entities and relationships to the Knowledge Graph ClickHouse database

### What is a call graph?

Here is a simple example of a call graph between two files:

```typescript
// fileA.ts
export class Foo {
  hello() {
    const bar = new Bar();
    bar.myMethod();
  }
}
```

```typescript
// fileB.ts
export class Bar {
  myMethod() {
    console.log("Hello from Bar.myMethod()");
  }
}
```

**Explanation:**

- `fileA.ts` defines `class Foo` with a method `hello()`.
- Inside `hello()`, it creates an instance of `Bar` and calls `myMethod()`.
- `fileB.ts` defines `class Bar` with the `myMethod()` implementation.

We create a call graph between the two files, and the result looks like this:

```mermaid
graph LR
    %% Define subgraphs
    subgraph DIR["my_directory -"]
        subgraph FILEA["fileA.ts"]
            A1["class Foo"]
            A2["hello()"]
        end

        subgraph FILEB["fileB.ts"]
            B1["class Bar"]
            B2["myMethod()"]
        end
    end

    %% Edges
    FILEA --> A1
    FILEB --> B1
    A1 -->|"method"| A2
    B1 -->|"method"| B2
    A2 -.->|"calls"| B2

    %% Styling
    %% Lighter directory background
    style DIR fill:#fafafa,stroke:#bbb,stroke-width:1px,color:#111,font-weight:bold
    style FILEA fill:#dce9ff,stroke:#90c2ff,stroke-width:1px,color:#111,font-weight:bold
    style FILEB fill:#d9f7d9,stroke:#90e090,stroke-width:1px,color:#111,font-weight:bold

    %% Class and method nodes (colored distinctly)
    style A1 fill:#cce0ff,stroke:#4080ff,stroke-width:1px,color:#003366
    style A2 fill:#dde9ff,stroke:#4080ff,stroke-width:1px,color:#003366
    style B1 fill:#ccffcc,stroke:#33aa33,stroke-width:1px,color:#004400
    style B2 fill:#ddffdd,stroke:#33aa33,stroke-width:1px,color:#004400

    %% Edge styling
    linkStyle 0,1,2,3 stroke:#444,stroke-width:1.5px
    linkStyle 4 stroke:#5b32ff,stroke-width:2.5px,stroke-dasharray: 4 2,background-color:#f0f0ff

```

### Core components

- `gkg-indexer`: The ETL pipeline for GitLab's code.
- `gkg-webserver`: The REST + MCP interface to query the Knowledge Graph.
- `NATS JetStream`: The message broker for the Knowledge Graph.
- `NATS KV`: The key-value store for the Knowledge Graph.
- `ClickHouse`: The OLAP database for the Knowledge Graph.
- `Gitaly`: Handles all Git repository RPC file access for GitLab

### Data storage

The Knowledge Graph code data is going to be stored in separate ClickHouse database.

- For `.com` this will probably be in a separate instance.
- For small dedicated environments and self-hosted instances, this can be done in the same instance as the main ClickHouse database. This choice ultimately depends on what the operators think is best for their environment.

### Some numbers

As of November 2025, the [GitLab monolith](https://gitlab.com/gitlab-org/gitlab) has over 4000 branches considered "active" (committed to within the last 3 months) and even more that are considered "stale" (last committed more than 3 months ago).

Locally, with the limited support for Ruby. We currently index about 300,000 definitions and over 1,000,000 relationships.

For simplicity's sake, let's say we want to keep an active code index for branches that are considered "active". This would require us to index (300,000 definitions *4000 branches) = 1.2 billion definitions and (1,000,000 relationships* 4000 branches) = 4 billion relationships just for the GitLab monolith. This is simply not feasible if we extrapolate this to all the repositories in `.com`.

### Use cases

To come up with a solution for the scale issue it's probably best to outline some use cases where the Knowledge Graph can be used to answer questions about code.

- Does this merge request change the behavior of the existing code in unexpected ways?
- What is the impact of this merge request on the existing code?
- Perform code exploration to help understand the codebase and reveal architectural patterns.
- Provide guidance when a user wants to refactor the code or add a new feature.
- Identify the potential risks of a vulnerability in the codebase.
- Create queryable APIs for code exploration and analysis.
- Documentation generation for the codebase.

Of course, there are many more use cases that can be thought of, but these seem to be the most common ones. This raises the question: do we need to index all the code for **every active branch** for every repository? The answer is **probably not**.

### Indexing the main branch

Let's first focus on indexing the main branch for every repository. This should cover most of the use cases for the Knowledge Graph and then let's think of a strategy to index the active branches if the need arises.

#### Extract

The extract phase involves listening to events from NATS and leveraging ClickHouse as both the data store and the mechanism for deriving project hierarchies and full paths.

Push events from GitLab's PostgreSQL database are published to NATS JetStream subjects like:

- `gkg_siphon_stream.events`
- `gkg_siphon_stream.push_event_payloads`

The indexing service subscribes to these NATS subjects and correlates events across tables:

- Events table: Contains `event_id`, `project_id`, `author_id`, and push action.
- Push payloads table: Contains `event_id`, `ref` (branch name), `commit_to` (SHA) and ref type.

The indexer receives the events and confirms it's a push to the `main` branch before proceeding with the indexing process. Then, the service acquires a lock on the project + branch + ref combination. This is to prevent other workers or pods from indexing the same branch at the same time.

Example NATS KV:

- Key: `/gkg-indexer/indexing/{project_id}/{branch_name}/{ref_type}/lock`
- Value: `{ "worker_id": String, "started_at": Instant }`
- TTL: 1 hour (estimated based on the amount of resources)

Once the service acquires the lock, it will make a direct RPC call to Gitaly to download the files temporarily to disk. The service will query ClickHouse as needed to build the namespace hierarchy and gather additional metadata to enrich both the project's code graph and NATS locking.

#### Transform (Call Graph Construction)

The next phase is documented at https://gitlab.com/gitlab-org/rust/knowledge-graph, however, here's a brief overview of the process:

- `FileProcessor` chooses a language parser by extension and feeds the content through the analyzers to the [GitLab Code Parser](https://gitlab.com/gitlab-org/rust/gitlab-code-parser). Supported languages include:
  - Ruby
  - Python
  - Kotlin
  - Java
  - C#
  - TypeScript/JavaScript
  - Rust
- The parser output is normalized into `FileProcessingResult` objects that contain definitions, imports, and references with precise byte offsets.
- For each file, the `AnalysisService` turns those results into a graph:
  - `FileSystemAnalyzer` builds directory and file nodes plus containment edges.
  - Language-specific analyzers emit definition and import nodes using consistent structs.
  - References are stitched into call-graph edges (definition → definition) and dependency edges (definition → imported symbol, imported symbol → definition/file).

#### Load

Once the graph is constructed, the `WriterService` converts nodes and edges into Arrow batches and writes Parquet files (`directory.parquet`, `file.parquet`, `definition.parquet`, `imported_symbol.parquet`, plus one Parquet file per relationship table defined in `RELATIONSHIP_TABLES`).

The `RepositoryIndexer::load_into_database` method will then leverage the ClickHouse client to stream the parquet data into the appropriate node and edge tables. Loading the data should be done using upsert operations to prevent duplicate data. The nodes and edges that have been deleted should be soft-deleted so a subsequent cleaning job can remove them.

Once the process is complete, the finished database is ready for query services, MCP adapters, and AI tooling.

#### Flow visual representation

```mermaid
graph TB
    subgraph "Data Sources"
        GL[GitLab PostgreSQL]
    end

    subgraph "Event Streaming Layer"
        NATS[NATS JetStream]
        E1[gkg_siphon_stream.events]
        E2[gkg_siphon_stream.push_event_payloads]
        GL --> NATS
        NATS --> E1
        NATS --> E2
    end

    subgraph "Extract Phase"
        IS[Indexing Service]
        E1 --> IS
        E2 --> IS

        LOCK{Acquire Lock?}
        IS --> LOCK

        KV[(NATS KV Store<br/>TTL: 1 hour)]
        LOCK -->|Lock Key| KV

        GITALY[Gitaly RPC<br/>Download Files]
        LOCK -->|Success| GITALY
    end

    subgraph "Transform Phase - Call Graph Construction"
        FP[FileProcessor<br/>Choose Parser by Extension]
        GITALY --> FP

        GCP[GitLab Code Parser]
        FP --> GCP

        LANGS[Supported Languages:<br/>Ruby, Python, Kotlin<br/>Java, C#, TS/JS, Rust]
        GCP -.-> LANGS

        FPR[FileProcessingResult<br/>definitions, imports, references]
        GCP --> FPR

        AS[AnalysisService]
        FPR --> AS

        FSA[FileSystemAnalyzer<br/>directories, files, containment]
        AS --> FSA

        LA[Language Analyzers<br/>definitions, imports]
        AS --> LA

        CG[Call Graph Construction<br/>def→def edges<br/>def→import edges<br/>import→def/file edges]
        FSA --> CG
        LA --> CG
    end

    subgraph "Load Phase"
        WS[WriterService]
        CG --> WS

        ARROW[Convert to Arrow Batches]
        WS --> ARROW

        PQ[Write Parquet Files:<br/>directory.parquet<br/>file.parquet<br/>definition.parquet<br/>imported_symbol.parquet<br/>+ relationship tables]
        ARROW --> PQ

        UPSERT[Stream to ClickHouse<br/>Upsert Operations<br/>Soft Delete Removed Nodes]
        PQ --> UPSERT

        CHDB[(ClickHouse Database<br/>Node & Edge Tables)]
        UPSERT --> CHDB
    end

    subgraph "Consumers"
        QS[Query Services]
        MCP[MCP Adapters]
        AI[AI Tooling]

        CHDB --> QS
        CHDB --> MCP
        CHDB --> AI
    end

    style IS fill:#e1f5ff
    style FP fill:#fff4e1
    style AS fill:#fff4e1
    style WS fill:#e8f5e9
    style CHDB fill:#f3e5f5
```

### Indexing the active branches

#### The problem

As discussed in the previous section, the main branch is the most common branch to index. However, it still feels relevant to document a strategy to index the active branches if the need arises. Let's also not forget that the Knowledge Graph includes a local version that customers can use to query code against their local repository at any version.

To reiterate, the issue with indexing active branches is the sheer volume of data that would need to be indexed. We're talking about billions of definitions and relationships for each GitLab-like repositories. This is a complex problem that takes effort away from releasing a first version of the Knowledge Graph service without providing clear value.

#### A future strategy

Once we deploy the initial version, if our metrics and customer feedback show that the ability to explore codebases at any version is valuable, we can then explore our options.

As stated above GitLab has the concept of a branch being "active" or "stale". An active branch is one that has been committed to within the last 3 months. A stale branch is one that has not been committed to in the last 3 months.

For the amount of data and un-even query distribution (some branches are never going to be queried), it's best we don't keep the data against the main branches in the same database since that would result in a lot of wasted storage and compute resources.

Ideally, we would re-use the same indexing strategy as the main branch where we can index the active branches by listening to push events from NATS, but instead of loading the data into ClickHouse, we would store the data in cold storage (like S3 or GCS).

On request, we would load the data into ClickHouse from cold storage in materialized tables. This would allow us to then query the data in ClickHouse during the current session and then unload the data from ClickHouse after the session is complete (based on a variable TTL).

#### Flow visual representation

```mermaid
graph TB
    subgraph "Active Branch Processing"
        J[Push Event] --> K[NATS Event Stream]
        K --> L[Index Data]
        L --> M[Cold Storage<br/>S3/GCS]
    end

    subgraph "On-Demand Loading"
        N[User Request] --> O{Data in ClickHouse?}
        O -->|No| P[Load from Cold Storage]
        O -->|Yes| Q[Query Data]
        P --> R[Materialized<br/>in ClickHouse]
        R --> Q
        Q --> S[Session Complete]
        S --> T[Unload Branch Data After TTL]
        T --> M
    end

    style M fill:#2196F3
    style P fill:#2196F3
```

#### Cleaning up

Once the branch either becomes stale or is deleted, we should clean up the data in our cold storage. This would be done by a separate job that would run periodically and clean up the data based on the latest state of the branches.

#### Alternative approach

An alternative approach if the time to first response is not critical is to index the active branches and then index the stale branches on demand. Depending on the indexing speed on the servers, this would allow us to save the temporary data in ClickHouse and then dispose of it after the session is complete or at a later time. This would eliminate the need to manage cold storage and the associated costs.

#### Indexing the stale branches

Stale branches are in most cases branches that have been abandoned by the original author. They are not actively being worked. If we were to index them, we could follow the same strategy as the [alternative approach](#alternative-approach) described for active branches.

#### Zero-Downtime Schema Changes

Code Indexing is going to follow the same schema migration strategy as the main branch as described in [Zero-Downtime Schema Changes](./sdlc_indexing.md#zero-downtime-schema-changes).

### How Code Querying Works Today

- **Purpose-built MCP tools**
  - The Knowledge Graph team originally built dedicated MCP tools, which include code-specific tools implemented under `crates/mcp/tools`. Each tool wraps a focused workflow on top of the indexed call graph. Reference documentation lives at [`docs/mcp/tools`](https://gitlab-org.gitlab.io/rust/knowledge-graph/docs/mcp/tools/).
- **What the tools currently do**
  - [`list_projects`](https://gitlab-org.gitlab.io/rust/knowledge-graph/docs/mcp/tools/#list_projects) enumerates indexed repositories for agent discovery.
  - [`search_codebase_definitions`](https://gitlab-org.gitlab.io/rust/knowledge-graph/docs/mcp/tools/#search_codebase_definitions) searches Definition nodes by name, FQN, or partial match and streams back signatures plus context.
  - [`get_definition`](https://gitlab-org.gitlab.io/rust/knowledge-graph/docs/mcp/tools/#get_definition) resolves a usage line to its Definition or ImportedSymbol node by leveraging call graph edges such as `CALLS` and `AMBIGUOUSLY_CALLS`.
  - [`get_references`](https://gitlab-org.gitlab.io/rust/knowledge-graph/docs/mcp/tools/#get_references) pivots the other way through relationships like `DEFINES_IMPORTED_SYMBOL` and `FILE_IMPORTS` to list every referencing definition with contextual snippets.
  - [`read_definitions`](https://gitlab-org.gitlab.io/rust/knowledge-graph/docs/mcp/tools/#read_definitions) batches definition bodies so agents can retrieve implementations efficiently.
  - [`repo_map`](https://gitlab-org.gitlab.io/rust/knowledge-graph/docs/mcp/tools/#repo_map) walks the directory nodes and summarizes contained definitions, using the graph to stay `.gitignore`-aware.
  - [`index_project`](https://gitlab-org.gitlab.io/rust/knowledge-graph/docs/mcp/tools/#index_project) invokes the repository indexer inside the MCP process, wiring the reindexing flow described earlier into an on-demand tool call.
- **How they execute queries**
  - Tools rely on `database::querying::QueryLibrary` (for example, `search_codebase_definitions` delegates to `QueryingService` via the shared query library) and on the same database connections managed by `crates/database`. This keeps query plans consistent with the schema imported during indexing.
  - Many tools supplement database hits with filesystem reads (see `file_reader_utils`) so responses include code snippets, respecting byte offsets captured in the graph.
- **Other consumers**
  - The HTTP/GQL surfaces continue to use the shared schema metadata published at [`docs/reference/schema`](https://gitlab-org.gitlab.io/rust/knowledge-graph/docs/reference/schema/); the MCP tools simply package the most common graph traversals for AI agents and IDE features while reusing the same underlying query service.

> **Important Note:** We intend to replace the above tools, where it makes sense, with our Graph Query Engine technology to enable agents and analytics to traverse the graph using tools that will be shared with SDLC querying. Agents will never write or execute raw queries themselves. They can only interact with the graph through these exposed, parameterized tools, which enforce security and access controls.
