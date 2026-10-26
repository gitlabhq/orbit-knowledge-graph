---
title: Orbit Query Engine
description: How the Orbit Query Engine turns a graph query into parameterized SQL.
authors:
  - michaelusa
---

## What the engine does

```mermaid
%%{init: {"flowchart": {"curve": "linear", "nodeSpacing": 24, "rankSpacing": 64, "padding": 14, "subGraphTitleMargin": {"top": 8, "bottom": 18}}}}%%
flowchart LR
    accTitle: Query languages and backends of the Orbit Query Engine
    accDescr: A user query arrives in GQL, with SQL and GLQL planned. The Orbit Query Engine turns it into SQL for ClickHouse or DuckDB, with PostgreSQL planned.

    user(["User Query"])

    subgraph languages["Query languages"]
        gql["GQL"]
        sql["SQL"]
        glql["GLQL"]
    end

    compiler[["Orbit Query Engine"]]

    subgraph backends["Backends"]
        clickhouse[("ClickHouse")]
        duckdb[("DuckDB")]
        postgres[("PostgreSQL")]
    end

    user --> gql
    user -.-> sql
    user -.-> glql
    gql --> compiler
    sql -.-> compiler
    glql -.-> compiler
    compiler --> clickhouse
    compiler --> duckdb
    compiler -.-> postgres

    classDef language fill:#F59E0B26,stroke:#F59E0B,stroke-width:1.5px
    classDef engine fill:#22C55E26,stroke:#22C55E,stroke-width:1.5px
    classDef backend fill:#3B82F626,stroke:#3B82F6,stroke-width:1.5px
    classDef future fill:#9CA3AF26,stroke:#9CA3AF,stroke-width:1.5px,stroke-dasharray:5 4

    class user,gql language
    class compiler engine
    class clickhouse,duckdb backend
    class sql,glql,postgres future

    style languages fill:#F59E0B12,stroke:#F59E0B
    style backends fill:#3B82F612,stroke:#3B82F6
```

The Orbit Query Engine is a multi-pass compiler. It turns a JSON Query DSL or
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

| Query type | Example (GQL) | SQL output fragment |
| --- | --- | --- |
| **Traversal**<br>Merged merge requests and the users who wrote them | <pre><code class="language-cypher">MATCH (u:User)-[:AUTHORED]-&gt;&#10;      (mr:MergeRequest {state: 'merged'})&#10;RETURN u.username, mr.title</code></pre> | <pre><code class="language-sql">FROM gl_edge AS e JOIN gl_merge_request mr&#10;  ON mr.id = e.target_id&#10;WHERE e.relationship_kind = ?&#10;  AND mr.state = ?</code></pre> |
| **Aggregation**<br>How many merge requests one project has in each state | <pre><code class="language-cypher">MATCH (mr:MergeRequest)-[:IN_PROJECT]-&gt;&#10;      (p:Project {id: 278964})&#10;RETURN mr.state, count(mr) AS mrs</code></pre> | <pre><code class="language-sql">SELECT mr.state, COUNT(mr.id) AS mrs&#10;... GROUP BY mr.state</code></pre> |
| **Path finding**<br>The shortest path from a group to a project | <pre><code class="language-cypher">MATCH path = ANY SHORTEST&#10;  (g:Group {id: 9970})-[:CONTAINS*1..2]-&gt;&#10;  (p:Project {id: 278964})&#10;RETURN path</code></pre> | <pre><code class="language-sql">WITH forward AS (...),&#10;     backward AS (...)&#10;SELECT ... FROM forward&#10;JOIN backward ON ...</code></pre> |
| **Neighbors**<br>Everything directly connected to one merge request, in either direction | <pre><code class="language-cypher">MATCH (mr:MergeRequest {id: 42})--(n)&#10;RETURN n</code></pre> | <pre><code class="language-sql">SELECT ... FROM gl_edge WHERE source_id = ?&#10;UNION ALL&#10;SELECT ... FROM gl_edge WHERE target_id = ?</code></pre> |

<!-- markdownlint-enable MD044 -->

## How it works

```mermaid
%%{init: {"flowchart": {"curve": "linear", "nodeSpacing": 40, "rankSpacing": 70, "padding": 16}}}%%
flowchart LR
    accTitle: How the Orbit Query Engine compiles a query
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

    classDef data fill:#3B82F626,stroke:#3B82F6,stroke-width:1.5px
    classDef input fill:#F59E0B26,stroke:#F59E0B,stroke-width:1.5px
    classDef checking fill:#22C55E26,stroke:#22C55E,stroke-width:1.5px
    classDef planner fill:#8B5CF626,stroke:#8B5CF6,stroke-width:1.5px
    classDef emitter fill:#EC489926,stroke:#EC4899,stroke-width:1.5px
    classDef output fill:#9CA3AF26,stroke:#9CA3AF,stroke-width:1.5px,stroke-dasharray:4 3

    class model data
    class front,context input
    class check checking
    class plan planner
    class emit emitter
    class sql output

    linkStyle 4,5,6 stroke:#3B82F6,stroke-width:2px
    linkStyle 7,8 stroke:#F59E0B,stroke-width:2px
```

The engine compiles in small passes, and each pass declares the state it reads
and writes, so a pass sees only what earlier passes produced. Pipelines reuse
these passes per frontend and backend: hydration skips parsing and pagination,
and DuckDB drops the security passes. Every pass reads one immutable data
model for the schema version that was active when the request started. That
model is the only source of tables, columns, and authorization rules, so
nothing from the query reaches SQL unless the model resolves it. The output is
one parameterized SQL statement, plus a plan for the hydration statement.

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

### 1. Load the data model for the active schema version

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

When the server loads a schema version, it builds one data model from that
version's ontology. The graph catalog holds entities, properties,
relationships, and their variants. The layout maps each of them to ClickHouse
tables, columns, sort keys, and edge routes. The policy holds role floors,
redaction rules, and admin-only fields. Every pass reads the model and never
changes it.

### 2. Turn a user query into an intermediate representation

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

    classDef pass fill:#22C55E26,stroke:#22C55E,stroke-width:1.5px
    classDef state fill:#8B5CF626,stroke:#8B5CF6,stroke-width:1.5px
    classDef caller fill:#F59E0B26,stroke:#F59E0B,stroke-width:1.5px
    classDef data fill:#3B82F626,stroke:#3B82F6,stroke-width:1.5px
    classDef external fill:#9CA3AF26,stroke:#9CA3AF,stroke-width:1.5px
    classDef reject fill:#EF444426,stroke:#EF4444,stroke-width:1.5px
    class JSON,GQL caller
    class JF,GF pass
    class IR state
    class Schema external
```

Every pass after the frontend works on one intermediate representation (IR):
node selectors, relationship selectors, filters, aggregations, ordering, and
limits. The JSON frontend checks the payload against its JSON schemas, then
deserializes it. The GQL frontend parses the statement against an openCypher 9
grammar, then lowers it and checks for one connected pattern and unique
variables. Both produce the same IR, so no later pass knows which language the
query came from. A `CALL db.schema()` statement stops here and returns
ontology metadata, and [Query languages](query-languages.md) covers both
frontends.

### 3. Validate and normalize the query

```mermaid
%%{init: {"flowchart": {"curve": "linear", "nodeSpacing": 28, "rankSpacing": 48, "padding": 14}}}%%
flowchart LR
    accTitle: Validation and normalization, with the example query
    accDescr: Validation checks shape, limits, references, and types. Normalization resolves tables and coerces values. For the example, every name exists, AUTHORED runs from User to MergeRequest, and both entities resolve to their tables.
    Input["<b>Intermediate representation</b>"]
    Validate["<b>Validate</b><br/>shape, limits, references, types<hr/><i>• all names exist<br/>• AUTHORED goes from User to MergeRequest</i>"]
    Normalize["<b>Normalize</b><br/>tables, coerced values, expanded columns<hr/><i>• User → gl_user<br/>• MergeRequest → gl_merge_request<br/>• 'merged' cast to the type of state</i>"]
    Input --> Validate --> Normalize

    classDef pass fill:#22C55E26,stroke:#22C55E,stroke-width:1.5px
    classDef state fill:#8B5CF626,stroke:#8B5CF6,stroke-width:1.5px
    classDef caller fill:#F59E0B26,stroke:#F59E0B,stroke-width:1.5px
    classDef data fill:#3B82F626,stroke:#3B82F6,stroke-width:1.5px
    classDef external fill:#9CA3AF26,stroke:#9CA3AF,stroke-width:1.5px
    classDef reject fill:#EF444426,stroke:#EF4444,stroke-width:1.5px
    class Input state
    class Validate,Normalize pass
```

Validation checks the IR's shape and limits, resolves every name against the
data model, and checks that filter values fit their property types. A query
that fails a check is rejected with an error that names the problem.
Normalization then resolves entities to tables, coerces filter values, and
expands wildcard columns. It also sorts predicates into a fixed order, so the
same query always produces the same SQL.

### 4. Apply the caller's access rules

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

    classDef pass fill:#22C55E26,stroke:#22C55E,stroke-width:1.5px
    classDef state fill:#8B5CF626,stroke:#8B5CF6,stroke-width:1.5px
    classDef caller fill:#F59E0B26,stroke:#F59E0B,stroke-width:1.5px
    classDef data fill:#3B82F626,stroke:#3B82F6,stroke-width:1.5px
    classDef external fill:#9CA3AF26,stroke:#9CA3AF,stroke-width:1.5px
    classDef reject fill:#EF444426,stroke:#EF4444,stroke-width:1.5px
    class Input,Restricted state
    class Context caller
    class Restrict pass
```

Restriction is the first pass that reads the caller's security context: their
traversal paths, and their role on each one. It rejects a query on an entity
whose required role the caller holds on no path. It removes admin-only
columns, and rejects filters, sorts, and aggregates on admin-only fields. It
checks any traversal-path filter in the query against the caller's paths. It
also rejects aggregations that no namespaced node can reach, because
aggregates skip row-by-row redaction.

### 5. Plan the reads and pick the latest row versions

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

    classDef pass fill:#22C55E26,stroke:#22C55E,stroke-width:1.5px
    classDef state fill:#8B5CF626,stroke:#8B5CF6,stroke-width:1.5px
    classDef caller fill:#F59E0B26,stroke:#F59E0B,stroke-width:1.5px
    classDef data fill:#3B82F626,stroke:#3B82F6,stroke-width:1.5px
    classDef external fill:#9CA3AF26,stroke:#9CA3AF,stroke-width:1.5px
    classDef reject fill:#EF444426,stroke:#EF4444,stroke-width:1.5px
    class Input,Plan state
    class Model data
    class Planner pass
```

The planner turns the IR into a relation graph, with one read per
relationship, joined through shared nodes. It reads edge tables first, and
joins a node table only when the query needs that node's data. A relationship
stored in several edge tables becomes a `UNION ALL`, and a selective chain
passes each hop's results to the next hop. Tables keep old row versions until
ClickHouse merges them, so every read must pick the latest one. The planner
uses `FINAL` by default, and a cheaper two-step read for exact IDs and
selective filters.

### 6. Narrow reads to the namespaces the query names

A traversal-path bound limits a read to one namespace when the query names it,
so ClickHouse can skip most of the table. The example names no project or
group, so it gets no bound, and here is the same query pinned to one project:

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

    classDef state fill:#8B5CF626,stroke:#8B5CF6,stroke-width:1.5px
    classDef caller fill:#F59E0B26,stroke:#F59E0B,stroke-width:1.5px
    classDef external fill:#9CA3AF26,stroke:#9CA3AF,stroke-width:1.5px
    class user,mr,project state
```

The project is the anchor, and the engine reads its current path inside the
same statement. The bound spreads to `mr` through `IN_PROJECT`, and the
`AUTHORED` edge read gets it too, but the global `User` gets none. A bound
never crosses relationships like `CLOSES` that can leave the namespace, and
the authorization filter always still applies.

### 7. Build the SQL tree and shape the result

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

    classDef pass fill:#22C55E26,stroke:#22C55E,stroke-width:1.5px
    classDef state fill:#8B5CF626,stroke:#8B5CF6,stroke-width:1.5px
    classDef caller fill:#F59E0B26,stroke:#F59E0B,stroke-width:1.5px
    classDef data fill:#3B82F626,stroke:#3B82F6,stroke-width:1.5px
    classDef external fill:#9CA3AF26,stroke:#9CA3AF,stroke-width:1.5px
    classDef reject fill:#EF444426,stroke:#EF4444,stroke-width:1.5px
    class Plan,AST state
    class Page,Shape,Extras pass
```

Lowering turns the relation graph into a SQL syntax tree, and attaches the
bounds to each read. Pagination asks for one row past the page, so the server
can tell whether more rows exist. The response policy caps long text values,
so a page fits in one response message. The result contract adds the ID and
type columns that Rails needs for redaction. This step also plans the
hydration statement, and sets time and memory limits for the query type.

### 8. Add authorization filters, verify them, and render SQL

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

    classDef pass fill:#22C55E26,stroke:#22C55E,stroke-width:1.5px
    classDef state fill:#8B5CF626,stroke:#8B5CF6,stroke-width:1.5px
    classDef caller fill:#F59E0B26,stroke:#F59E0B,stroke-width:1.5px
    classDef data fill:#3B82F626,stroke:#3B82F6,stroke-width:1.5px
    classDef external fill:#9CA3AF26,stroke:#9CA3AF,stroke-width:1.5px
    classDef reject fill:#EF444426,stroke:#EF4444,stroke-width:1.5px
    class AST state
    class Security,Verify,Codegen pass
    class Reject reject
```

Security runs last, after every pass that can add a read to the tree. It adds
a `startsWith(traversal_path, ?)` filter to every namespaced read, using only
the paths where the caller meets the entity's role floor. Verification then
walks the finished tree, and rejects the query if any namespaced read lacks a
filter. Code generation renders the tree as SQL for one backend, with every
value as a bound parameter. A backend that can't express an operation rejects
the query instead of approximating it.

## Decisions

- [ADR 000: ClickHouse as graph storage](../adr/000_clickhouse_graph_storage.md)
- [ADR 016: Column statistics for join ordering](../adr/016_column_statistics_for_join_ordering.md)
