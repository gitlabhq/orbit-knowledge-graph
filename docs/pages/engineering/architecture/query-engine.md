---
title: Graph query engine
description: How the engine turns a graph query into parameterized SQL.
authors:
  - michaelusa
---

## What the engine does

```mermaid
%%{init: {"flowchart": {"curve": "linear", "nodeSpacing": 24, "rankSpacing": 64, "padding": 14}}}%%
flowchart LR
    accTitle: Query languages and backends of the graph query engine
    accDescr: A user query arrives in GQL or the JSON Query DSL, with SQL and GLQL planned. The graph query compiler turns it into SQL for ClickHouse or DuckDB, with PostgreSQL planned.

    user(["User query"])

    subgraph languages["Query languages"]
        gql["GQL"]
        json["JSON Query DSL"]
        sql["SQL"]
        glql["GLQL"]
    end

    compiler[["Graph query compiler"]]

    subgraph backends["Backends"]
        clickhouse[("ClickHouse")]
        duckdb[("DuckDB")]
        postgres[("PostgreSQL")]
    end

    user --> gql
    user --> json
    user -.-> sql
    user -.-> glql
    gql --> compiler
    json --> compiler
    sql -.-> compiler
    glql -.-> compiler
    compiler --> clickhouse
    compiler --> duckdb
    compiler -.-> postgres

    classDef language fill:#FEF3C7,stroke:#B45309,color:#78350F
    classDef engine fill:#DCFCE7,stroke:#15803D,color:#14532D
    classDef backend fill:#DBEAFE,stroke:#2563EB,color:#1E3A8A
    classDef future fill:#F9FAFB,stroke:#9CA3AF,color:#6B7280,stroke-dasharray:5 4

    class user,gql,json language
    class compiler engine
    class clickhouse,duckdb backend
    class sql,glql,postgres future

    style languages fill:#FFFBEB,stroke:#FCD34D
    style backends fill:#EFF6FF,stroke:#93C5FD
```

Dashed boxes are planned and not yet supported.

The graph query engine is a multi-pass compiler. It turns a JSON Query DSL or
GQL query into one parameterized SQL statement over the ontology's node and
edge tables. Every statement it emits is bounded, carries a traversal-path
predicate on every namespaced read, and resolves each row to its latest
version. ClickHouse is the production backend. The same passes target DuckDB
for the local graph schema and the code-graph test suites.

The engine compiles four query types. Each row shows a GQL query and the core
of the SQL that it produces.

> [!note]
> These SQL fragments show only the core of each query.
> The full SQL also has authorization filters, page limits, and query settings.

<!-- markdownlint-disable MD044 -->

| Query type | Example (GQL) | Meaning | SQL output fragment |
| --- | --- | --- | --- |
| **Traversal** | <pre><code class="language-cypher">MATCH (u:User)-[:AUTHORED]-&gt;(mr:MergeRequest {state: 'merged'})&#10;RETURN u.username, mr.title</code></pre> | Merged merge requests and the users who wrote them | <pre><code class="language-sql">FROM gl_edge AS e JOIN gl_merge_request AS mr ON mr.id = e.target_id&#10;WHERE e.relationship_kind = ? AND mr.state = ?</code></pre> |
| **Aggregation** | <pre><code class="language-cypher">MATCH (mr:MergeRequest)-[:IN_PROJECT]-&gt;(p:Project {id: 278964})&#10;RETURN mr.state, count(mr) AS mrs</code></pre> | How many merge requests one project has in each state | <pre><code class="language-sql">SELECT mr.state, COUNT(mr.id) AS mrs ... GROUP BY mr.state</code></pre> |
| **Path finding** | <pre><code class="language-cypher">MATCH path = ANY SHORTEST&#10;  (g:Group {id: 9970})-[:CONTAINS*1..2]-&gt;(p:Project {id: 278964})&#10;RETURN path</code></pre> | The shortest path from a group to a project | <pre><code class="language-sql">WITH forward AS (...), backward AS (...)&#10;SELECT ... FROM forward JOIN backward ON ...</code></pre> |
| **Neighbors** | <pre><code class="language-cypher">MATCH (mr:MergeRequest {id: 42})--(n)&#10;RETURN n</code></pre> | Everything directly connected to one merge request, in either direction | <pre><code class="language-sql">SELECT ... FROM gl_edge WHERE source_id = ?&#10;UNION ALL&#10;SELECT ... FROM gl_edge WHERE target_id = ?</code></pre> |

<!-- markdownlint-enable MD044 -->

## How it works

```mermaid
%%{init: {"flowchart": {"curve": "linear", "nodeSpacing": 40, "rankSpacing": 70, "padding": 16}}}%%
flowchart LR
    accTitle: How the graph query engine compiles a query
    accDescr: A JSON or GQL query passes through four stages. The frontend parses and lowers it into the intermediate representation. Check validates, normalizes, and restricts it. Plan picks access paths, latest-state reads, and traversal-path bounds. Emit lowers, secures, verifies, and renders it as parameterized SQL with a hydration plan. The data model feeds check, plan, and emit, and the security context feeds check and emit.

    model[("<b>Data model</b><br/>graph, tables, policy")]
    front["<b>Frontend</b><br/>JSON or GQL query<br/>parse and lower<br/>intermediate representation"]
    context["<b>Security context</b><br/>caller paths and roles"]
    check["<b>Check</b><br/>validate and normalize<br/>restrict"]
    plan["<b>Plan</b><br/>access paths<br/>latest-state reads<br/>traversal-path bounds"]
    emit["<b>Emit</b><br/>lower and shape<br/>secure and verify<br/>code generation"]
    sql["<b>Output</b><br/>parameterized SQL<br/>hydration plan"]

    front --> check --> plan --> emit --> sql

    model -. "names and types" .-> check
    model -. "tables and routes" .-> plan
    model -. "role floors" .-> emit
    context -- "paths and roles" --> check
    context -- "eligible paths" --> emit

    classDef data fill:#DBEAFE,stroke:#2563EB,color:#1E3A8A
    classDef input fill:#FEF3C7,stroke:#B45309,color:#78350F
    classDef checking fill:#DCFCE7,stroke:#15803D,color:#14532D
    classDef planner fill:#EDE9FE,stroke:#6D28D9,color:#4C1D95
    classDef emitter fill:#FCE7F3,stroke:#BE185D,color:#831843
    classDef output fill:#F3F4F6,stroke:#6B7280,color:#111827,stroke-dasharray:4 3

    class model data
    class front,context input
    class check checking
    class plan planner
    class emit emitter
    class sql output

    linkStyle 4,5,6 stroke:#2563EB,stroke-width:2px
    linkStyle 7,8 stroke:#B45309,stroke-width:2px
```

The engine compiles in passes, and each pass has one job. A pass declares the
state it reads and the state it writes. That state is the IR, the query plan,
the SQL AST, the result context, or the hydration plan. A macro turns those
declarations into a typed context for each pipeline, so a pass sees only the
state it declared. A pass that needs state an earlier pass hasn't produced
fails with an invariant error, never on partial input.

A pipeline is an ordered list of passes for one frontend and one backend. The
JSON and GQL pipelines for ClickHouse share every pass after their frontend.
The hydration pipeline reuses the remote passes, minus parsing, pagination,
and hydration planning. The DuckDB pipelines reuse the frontends,
normalization, lowering, pagination, and the result contract. They swap in a
DuckDB planner and code generator, and drop restriction, bounds, and security,
because a local graph has a single user.

Every pass reads one immutable data model for the active schema version. The
model is the only source of table names, columns, edge routes, and
authorization rules. No identifier from the query reaches SQL unless the model
resolves it. Each request compiles against the schema snapshot that was active
when it started, so a schema switch never changes a query mid-compilation.

The output is one SQL statement with bound parameters, plus a plan for the
hydration statement.

Every statement that the engine emits has these guarantees:

| Guarantee | Meaning |
| --- | --- |
| **Parameterized SQL** | Values are bound parameters, and names must match the ontology. |
| **Fail-closed authorization** | Every namespaced read has an authorization filter, or the query is rejected. |
| **Bounded queries** | Hard limits on query shape, result size, time, and memory. |
| **Current state** | Every read sees the latest version of each row. |
| **Stable results** | The plan changes how data is read, never what is returned. |
| **Read-only access** | The webserver can only run `SELECT`. |

## Walkthrough: GQL query to secure SQL

The steps below follow one GQL query through the remote ClickHouse pipeline,
from the data model it compiles against to the statement that ClickHouse runs.

Each step's diagram traces this query through that step, and the italic text
in each box shows what happens to it. The colors mean the same thing in every
step. Green is a compiler pass, and purple is the query as it moves between
passes. Amber comes from the caller, blue is the data model, and gray is
outside the compiled statement.

```gql orbit-query
MATCH (u:User)-[:AUTHORED]->(mr:MergeRequest {state: 'merged'})
RETURN u.username, mr.title
```

### 1. The query data model

```mermaid
flowchart LR
    accTitle: How the ontology becomes a query data model
    accDescr: The ontology splits into the logical graph, backend facts, and authorization facts. They combine into one query data model per backend, which the engine reads to plan queries and render SQL.
    O["Ontology"]

    O --> G["<b>Logical graph</b><br/>• Entities and properties<br/>• Relationships and variants"]
    O --> B["<b>Backend facts</b><br/>• Tables and columns<br/>• Sort keys and foreign keys"]
    O --> A["<b>Authorization facts</b><br/>• Role requirements<br/>• Redaction and scope rules"]

    G & B & A --> M["Query data model"]

    M --> CH["ClickHouse + GitLab authorization"]
    M --> DB["DuckDB + trusted local"]

    CH --> C["Engine"]
    DB --> C

    C --> P["Query plan"]
    P --> S["SQL"]
```

The server builds one data model each time it loads a schema version. The
model combines three parts. The graph catalog holds entities, properties,
relationships, and relationship variants under typed IDs. The layout pairs the
physical schema with a mapping from graph identities to tables, columns, sort
keys, foreign keys, variant routes, and sorted copies. The policy holds role
floors, redaction metadata, and admin-only and hidden fields. Passes only read
the model. Schema management derives the same physical schema for DDL,
migrations, and cleanup, so the queries and the stored data cannot drift
apart.

### 2. Intermediate representation

```mermaid
%%{init: {"flowchart": {"curve": "linear", "nodeSpacing": 28, "rankSpacing": 48, "padding": 14}}}%%
flowchart LR
    accTitle: Frontends and the intermediate representation, with the example query
    accDescr: The JSON and GQL frontends both produce the intermediate representation. The example GQL statement becomes a traversal with two node selectors, one AUTHORED relationship, and the default limit.
    JSON["<b>JSON object</b>"]
    JF["<b>JSON frontend</b><br/>schema check, deserialize"]
    GQL["<b>GQL statement</b><hr/><i>MATCH (u:User)-[:AUTHORED]-><br/>(mr:MergeRequest {state: 'merged'})<br/>RETURN u.username, mr.title</i>"]
    GF["<b>GQL frontend</b><br/>parse, typed tree, lower"]
    IR["<b>Intermediate representation</b><hr/><i>• a traversal<br/>• u is a User<br/>• mr is a MergeRequest where state = 'merged'<br/>• u AUTHORED mr<br/>• limit 30, the default</i>"]
    Schema["<b>Ontology metadata</b>"]
    JSON --> JF --> IR
    GQL --> GF --> IR
    GF -.->|"CALL db.schema"| Schema

    classDef pass fill:#DCFCE7,stroke:#15803D,color:#14532D
    classDef state fill:#EDE9FE,stroke:#6D28D9,color:#4C1D95
    classDef caller fill:#FEF3C7,stroke:#B45309,color:#78350F
    classDef data fill:#DBEAFE,stroke:#2563EB,color:#1E3A8A
    classDef external fill:#F3F4F6,stroke:#6B7280,color:#111827
    classDef reject fill:#FEE2E2,stroke:#B91C1C,color:#7F1D1D
    class JSON,GQL caller
    class JF,GF pass
    class IR state
    class Schema external
```

Every pass after the frontend works on one intermediate representation (IR):
the language-neutral compiler input. It holds node selectors, relationship
selectors, filters, aggregations, ordering, and limits. Each query language
has one frontend that produces the IR, and that frontend is the first pass of
its pipelines. The JSON frontend validates the payload against the base and
ontology-derived JSON schemas, then deserializes it. The GQL frontend parses
against an openCypher 9 grammar and builds a typed syntax tree. Lowering then
applies statement-wide checks: one connected pattern, unique variables, and an
inferred query type. Both produce the same IR, so no later pass knows which
language the query came from.

The GQL frontend also answers schema calls. The server parses a statement once
and routes on the result. A `MATCH` statement continues into the compiler, and
`CALL db.schema()` returns ontology metadata without running any further pass.
The [query languages](query-languages.md) page covers both frontends in
detail.

### 3. Validation and normalization

```mermaid
%%{init: {"flowchart": {"curve": "linear", "nodeSpacing": 28, "rankSpacing": 48, "padding": 14}}}%%
flowchart LR
    accTitle: Validation and normalization, with the example query
    accDescr: Validation checks shape, limits, references, and types. Normalization resolves tables and coerces values. For the example, every name exists, AUTHORED runs from User to MergeRequest, and both entities resolve to their tables.
    Input["<b>Intermediate representation</b>"]
    Validate["<b>Validate</b><br/>shape, limits, references, types<hr/><i>• all names exist<br/>• AUTHORED goes from User to MergeRequest</i>"]
    Normalize["<b>Normalize</b><br/>tables, coerced values, expanded columns<hr/><i>• User → gl_user<br/>• MergeRequest → gl_merge_request<br/>• 'merged' cast to the type of state</i>"]
    Input --> Validate --> Normalize

    classDef pass fill:#DCFCE7,stroke:#15803D,color:#14532D
    classDef state fill:#EDE9FE,stroke:#6D28D9,color:#4C1D95
    classDef caller fill:#FEF3C7,stroke:#B45309,color:#78350F
    classDef data fill:#DBEAFE,stroke:#2563EB,color:#1E3A8A
    classDef external fill:#F3F4F6,stroke:#6B7280,color:#111827
    classDef reject fill:#FEE2E2,stroke:#B91C1C,color:#7F1D1D
    class Input state
    class Validate,Normalize pass
```

Validation checks the input's shape and bounds, resolves every reference
against the data model, and checks that filter values fit their property
types. Normalization resolves entities to tables, coerces filter values, and
expands wildcard projections. It also puts predicates in a canonical order, so
equal queries produce byte-identical SQL and parameters.

### 4. Restriction

```mermaid
%%{init: {"flowchart": {"curve": "linear", "nodeSpacing": 28, "rankSpacing": 48, "padding": 14}}}%%
flowchart LR
    accTitle: Restriction, with the example query
    accDescr: Restriction reads the security context and the intermediate representation. The example needs no raised role and asks for no admin-only column, so it passes unchanged.
    Input["<b>Intermediate representation</b>"]
    Context["<b>Security context</b><br/>paths and access levels<hr/><i>for example, Reporter on 1/9970/</i>"]
    Restrict["<b>Restrict</b><br/>role floors, admin-only fields,<br/>path filters, aggregation reach<hr/><i>• no special role needed<br/>• no admin-only fields</i>"]
    Restricted["<b>Restricted input</b><hr/><i>unchanged</i>"]
    Input --> Restrict
    Context --> Restrict
    Restrict --> Restricted

    classDef pass fill:#DCFCE7,stroke:#15803D,color:#14532D
    classDef state fill:#EDE9FE,stroke:#6D28D9,color:#4C1D95
    classDef caller fill:#FEF3C7,stroke:#B45309,color:#78350F
    classDef data fill:#DBEAFE,stroke:#2563EB,color:#1E3A8A
    classDef external fill:#F3F4F6,stroke:#6B7280,color:#111827
    classDef reject fill:#FEE2E2,stroke:#B91C1C,color:#7F1D1D
    class Input,Restricted state
    class Context caller
    class Restrict pass
```

Restriction is the first pass that reads the security context. It rejects
queries on entities whose role floor the caller meets on no authorized path.
It strips admin-only columns, and rejects filters, sorts, and aggregates on
admin-only fields. It checks explicit traversal-path filters against the
authorized set. It also rejects aggregations in which no traversal-path-scoped
node is reachable, because aggregates bypass row-level redaction.

### 5. Planning and latest-state reads

```mermaid
%%{init: {"flowchart": {"curve": "linear", "nodeSpacing": 28, "rankSpacing": 48, "padding": 14}}}%%
flowchart LR
    accTitle: Planning, with the example query
    accDescr: The planner reads the restricted input and the data model and produces a relation graph. For the example, it reads gl_edge, joins gl_merge_request and gl_user, and uses FINAL on both node reads.
    Input["<b>Restricted input</b>"]
    Model[("<b>Data model</b>")]
    Planner["<b>Planner</b><br/>relations, routes, joins,<br/>latest-state reads"]
    Plan["<b>Relation graph</b><hr/><i>• read AUTHORED edges from gl_edge<br/>• join gl_merge_request for state and title<br/>• join gl_user for username<br/>• read the latest row versions</i>"]
    Input --> Planner
    Model --> Planner
    Planner --> Plan

    classDef pass fill:#DCFCE7,stroke:#15803D,color:#14532D
    classDef state fill:#EDE9FE,stroke:#6D28D9,color:#4C1D95
    classDef caller fill:#FEF3C7,stroke:#B45309,color:#78350F
    classDef data fill:#DBEAFE,stroke:#2563EB,color:#1E3A8A
    classDef external fill:#F3F4F6,stroke:#6B7280,color:#111827
    classDef reject fill:#FEE2E2,stroke:#B91C1C,color:#7F1D1D
    class Input,Plan state
    class Model data
    class Planner pass
```

The planner builds a relation graph, with one relation per relationship
occurrence, joined through shared node bindings. Reads are edge-first. A node
table is joined only for node data that the query consumes: a filter, a
projection, a group key, or a sort key. Variant routing picks the edge table
for each relationship kind and endpoint pair. A relationship that spans
several tables becomes a `UNION ALL` with a uniform column set. Selective
chains pass distinct-endpoint frontiers from hop to hop, and path finding runs
a bidirectional frontier search. Foreign-key relationships join node to node
with no edge read. A relation that is referenced more than once becomes a CTE,
unless it is a point lookup. Planning changes access paths, never results.

Node and edge tables are `ReplacingMergeTree(_version, _deleted)`, so a read
must resolve each key to its latest version. The default is `FINAL`, a
streaming merge with bounded memory. For exact IDs, narrow ranges, frontiers,
and filters that the ontology marks as selective, the planner uses a two-step
read instead. It finds candidate keys without `FINAL`, then reads every
version of only those keys, and keeps the latest with `LIMIT 1 BY` on the
replacement key. Filters on sort-key and immutable columns run before the
pick, because tombstones carry those columns. Filters on mutable columns, and
`_deleted = false`, run after it.

### 6. Traversal-path bounds

A traversal-path bound limits a read to one namespace when the query names
that namespace. It is an extra `startsWith` filter on top of the authorization
filter, and it lets ClickHouse skip most of the table.

The example query names no project or group, so it gets no bound, and it reads
every namespace that the caller can see. Here is the same query pinned to one
project:

```gql orbit-query
MATCH (u:User)-[:AUTHORED]->(mr:MergeRequest {state: 'merged'})-[:IN_PROJECT]->(p:Project {id: 278964})
RETURN u.username, mr.title
```

```mermaid
%%{init: {"flowchart": {"curve": "linear", "nodeSpacing": 28, "rankSpacing": 64, "padding": 14}}}%%
flowchart LR
    accTitle: Traversal-path bounds for the example query pinned to one project
    accDescr: The project has a known ID, so it is the anchor. Its path bounds the merge request through IN_PROJECT, and the AUTHORED edge read through the merge request. The user is global and gets no bound.
    user["<b>u: User</b><hr/><i>global, so no bound</i>"]
    mr["<b>mr: MergeRequest</b><hr/><i>bound: the project's path</i>"]
    project["<b>p: Project {id: 278964}</b><hr/><i>• the anchor<br/>• its path is read in the same statement</i>"]
    user -->|"AUTHORED<br/>edge read bounded like mr"| mr
    mr -->|"IN_PROJECT<br/>the bound spreads to mr"| project

    classDef state fill:#EDE9FE,stroke:#6D28D9,color:#4C1D95
    classDef caller fill:#FEF3C7,stroke:#B45309,color:#78350F
    classDef external fill:#F3F4F6,stroke:#6B7280,color:#111827
    class user,mr,project state
```

The project has a known ID, so it is the anchor. The engine reads the
project's current path in a subquery of the same statement, so a moved project
is bounded by its new path. `IN_PROJECT` keeps a merge request in its
project's namespace, so the bound spreads to `mr`. `AUTHORED` edges are stored
with their merge request, so the edge read gets the same bound. `User` is
global and has no path, so `u` gets no bound.

Some relationships, like `CLOSES`, can cross namespaces. A bound never spreads
across them, and never from a child namespace up to its parent. A bound only
narrows a read, and the authorization filter still applies.

### 7. Lowering and result shaping

```mermaid
%%{init: {"flowchart": {"curve": "linear", "nodeSpacing": 28, "rankSpacing": 48, "padding": 14}}}%%
flowchart LR
    accTitle: Lowering and result shaping, with the example query
    accDescr: Lowering builds the SQL AST, then pagination, the response policy, the result contract, hydration planning, and settings shape it. For the example, the AST joins gl_edge to gl_merge_request and gl_user, the page is LIMIT 31, and the result contract adds ID and type columns for u and mr.
    Plan["<b>Relation graph</b>"]
    AST["<b>SQL AST</b><hr/><i>• gl_edge joined to gl_merge_request on target_id<br/>• gl_edge joined to gl_user on source_id</i>"]
    Page["<b>Pagination window</b><hr/><i>LIMIT 31: 30 rows, plus 1 to check for more</i>"]
    Shape["<b>Response policy and result contract</b><hr/><i>adds ID and type columns for u and mr</i>"]
    Extras["<b>Hydration plan and settings</b><hr/><i>username and title are already in the main query</i>"]
    Plan --> AST --> Page --> Shape --> Extras

    classDef pass fill:#DCFCE7,stroke:#15803D,color:#14532D
    classDef state fill:#EDE9FE,stroke:#6D28D9,color:#4C1D95
    classDef caller fill:#FEF3C7,stroke:#B45309,color:#78350F
    classDef data fill:#DBEAFE,stroke:#2563EB,color:#1E3A8A
    classDef external fill:#F3F4F6,stroke:#6B7280,color:#111827
    classDef reject fill:#FEE2E2,stroke:#B91C1C,color:#7F1D1D
    class Plan,AST state
    class Page,Shape,Extras pass
```

Lowering turns the plan into a SQL AST with physical result bindings, and
attaches the bounds to the reads they own. Pagination sets a limit of the page
size plus one probe row, lowers `SKIP` to an `OFFSET`, and adds tie-breakers
so that windows are stable. The response policy caps large text projections so
that a page fits within the Workhorse gRPC message limit. The result contract
adds redaction ID and type columns for every returned node and edge, and
records the result context that redaction and formatting read. The pass also
builds the hydration plan and resolves the ClickHouse settings for the query
type.

### 8. Security, verification, and code generation

```mermaid
%%{init: {"flowchart": {"curve": "linear", "nodeSpacing": 28, "rankSpacing": 48, "padding": 14}}}%%
flowchart LR
    accTitle: Security, verification, and code generation, with the example query
    accDescr: Security injects traversal-path predicates, verification checks every namespaced read, and code generation renders the SQL. For the example, gl_edge and gl_merge_request get predicates at the Reporter floor, gl_user gets none, and every value is a bound parameter.
    AST["<b>SQL AST</b>"]
    Security["<b>Inject traversal-path predicates</b><hr/><i>• path filter on gl_edge and gl_merge_request<br/>• gl_user is global, so no filter</i>"]
    Verify["<b>Verify</b><hr/><i>both filtered, so it passes</i>"]
    Codegen["<b>Code generation</b><hr/><i>AUTHORED, 'merged', and the paths are sent as parameters</i>"]
    Reject["<b>Reject</b>"]
    AST --> Security --> Verify
    Verify -- every namespaced read covered --> Codegen
    Verify -. any read uncovered .-> Reject

    classDef pass fill:#DCFCE7,stroke:#15803D,color:#14532D
    classDef state fill:#EDE9FE,stroke:#6D28D9,color:#4C1D95
    classDef caller fill:#FEF3C7,stroke:#B45309,color:#78350F
    classDef data fill:#DBEAFE,stroke:#2563EB,color:#1E3A8A
    classDef external fill:#F3F4F6,stroke:#6B7280,color:#111827
    classDef reject fill:#FEE2E2,stroke:#B91C1C,color:#7F1D1D
    class AST state
    class Security,Verify,Codegen pass
    class Reject reject
```

Security runs on the finished AST, after every pass that can add a read. It
adds `startsWith(traversal_path, ?)` to every namespaced node and edge alias,
using only the paths where the caller meets the entity's role floor. With no
eligible path, the predicate is a constant false. Verification then walks the
final AST, and rejects the statement if any namespaced alias lacks a
role-eligible predicate or a fail-closed false. Code generation renders the
AST in the backend's dialect with bound parameters. The AST has a closed
function vocabulary, and a backend rejects any operation it can't express.

## When things go wrong

- **An unresolved reference.** Validation rejects the query before planning.
- **A namespaced read without a predicate.** Verification rejects the
  statement. It never reaches ClickHouse.
- **An anchor that isn't indexed or is deleted.** The bound subquery returns a
  sentinel path, and the read falls back to the authorization predicate alone.
- **Too many anchors.** Past eight anchors on one node, the node keeps only
  its authorization predicate.
- **An operation the backend can't express,** such as token search on DuckDB.
  Code generation rejects it, and never substitutes an approximation.
- **An invariant violation between passes.** The pipeline returns an internal
  error, not degraded SQL.

## Why we built it this way

Declaring each pass's state makes ordering explicit, and stops one pass from
reading state that another hasn't produced yet. Pipelines can then reuse runs
of passes. Hydration and local compilation share the remote passes instead of
copying them, and running security last guarantees that verification sees the
final AST.

`FINAL` stays the default for latest-state reads. An `argMax` aggregation over
IDs can be faster on small results, but its memory grows with result
cardinality, which is unknown at compile time. On settled parts, `FINAL` was
as fast or faster in every measured case, with bounded memory. A slow `FINAL`
points to part fragmentation, and the fix belongs in storage.

Bounds resolve the anchor path inside the statement, not in a lookup before
it. That avoids a cache and its staleness window, and production `EXPLAIN`
output shows the same granule pruning as a literal prefix.

## Decisions

- [ADR 000: ClickHouse as graph storage](../adr/000_clickhouse_graph_storage.md)
- [ADR 016: Column statistics for join ordering](../adr/016_column_statistics_for_join_ordering.md)
