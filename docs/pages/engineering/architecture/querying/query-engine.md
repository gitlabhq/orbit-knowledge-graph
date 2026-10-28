---
title: Orbit Query Engine
description: How the Orbit Query Engine turns a graph query into parameterized SQL.
order: 2
authors:
  - michaelusa
---

## What the engine does

The Orbit Query Engine compiles a graph query into one parameterized SQL
statement. We built it so that neither end is fixed. A frontend turns a query
language into one intermediate representation, and a code generator turns that
representation into SQL for one backend. Today GQL is the only language, and
ClickHouse and DuckDB are the backends.

![The Orbit Query Engine turns GQL, and later SQL or GLQL, into SQL for ClickHouse, DuckDB, and later PostgreSQL](../../../../assets/architecture/query-engine.svg)

The engine compiles four query types. Each row shows a GQL query and the core
of the SQL that it produces.

> [!note] > These SQL fragments show only the core of each query. > The full
SQL also has authorization filters, page limits, and query settings.

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
    accDescr: A user query passes through four stages. The frontend parses and lowers it into the intermediate representation. Check validates, normalizes, and restricts it. Plan picks access paths, latest-state reads, and traversal-path bounds. Emit lowers, secures, verifies, and renders it as parameterized SQL with a hydration plan. The data model feeds check, plan, and emit, and the security context feeds check and emit.

    model[("<b>Data model</b><br/>graph, tables, policy")]
    front["<b>Frontend</b><br/>User Query<br/>parse and lower<br/>intermediate representation"]
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

The steps below follow one GQL query through the ClickHouse pipeline of the
Orbit API. They start at the data model the query compiles against and end at
the statement that ClickHouse runs.

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

When Orbit loads a schema version, it builds one data model from that
version's ontology. The model holds the graph, the ClickHouse tables that
store it, and the rules for who may read what. Every step reads the model and
never changes it. Each request compiles against the schema version that was
active when it started, so a schema switch never changes a query halfway
through.

### 2. Turn a user query into an intermediate representation

```mermaid
%%{init: {"flowchart": {"curve": "linear", "nodeSpacing": 28, "rankSpacing": 48, "padding": 14}}}%%
flowchart LR
    accTitle: Frontends and the intermediate representation, with the example query
    accDescr: The GQL frontend turns the example statement into the intermediate representation. A future language would add its own frontend that produces the same representation.
    GQL["<b>GQL statement</b><hr/><i>MATCH (u:User)-[:AUTHORED]-><br/>(mr:MergeRequest {state: 'merged'})<br/>RETURN u.username, mr.title</i>"]
    GF["<b>GQL frontend</b><br/>parse, typed tree, lower"]
    Future["<b>Future language</b><hr/><i>SQL, GLQL, ...</i>"]
    FF["<b>Its frontend</b>"]
    IR["<b>Intermediate representation</b><hr/><i>• a traversal<br/>• u is a User<br/>• mr is a MergeRequest where state = 'merged'<br/>• u AUTHORED mr<br/>• limit 30, the default</i>"]
    Schema["<b>Ontology metadata</b>"]
    GQL --> GF --> IR
    Future -.-> FF -.-> IR
    GF -.->|"CALL db.schema"| Schema

    classDef pass fill:#22C55E26,stroke:#22C55E,stroke-width:1.5px
    classDef state fill:#8B5CF626,stroke:#8B5CF6,stroke-width:1.5px
    classDef caller fill:#F59E0B26,stroke:#F59E0B,stroke-width:1.5px
    classDef data fill:#3B82F626,stroke:#3B82F6,stroke-width:1.5px
    classDef external fill:#9CA3AF26,stroke:#9CA3AF,stroke-width:1.5px
    classDef reject fill:#EF444426,stroke:#EF4444,stroke-width:1.5px
    classDef future fill:#9CA3AF26,stroke:#9CA3AF,stroke-dasharray:5 4
    class GQL caller
    class GF pass
    class IR state
    class Schema external
    class Future,FF future
```

Every step after the frontend works on one intermediate representation: the
nodes, relationships, filters, aggregations, ordering, and limits of the
query. The GQL frontend parses the statement against an openCypher 9 grammar,
checks that the pattern is connected, and lowers it. Our example becomes a
traversal with two nodes, one relationship, a filter on the merge request
state, and the default page size. Another language would need only its own
frontend, and no later step would change.

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

Validation checks the query's shape and limits, and checks every name against
the data model. A query that fails a check is rejected with an error that
names the problem. Normalization then maps each entity to its table, converts
filter values to their property types, and puts the filters in a fixed order.
The same query always produces the same SQL.

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

This is the first step that knows who is asking. It rejects a query on an
entity whose role the caller holds on no namespace. It removes admin-only
fields for callers who aren't admins. It also rejects an aggregation that no
namespaced node can reach, because counts can't be redacted row by row. Our
example asks for nothing restricted, so it passes unchanged.

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

The planner decides how to read each node and relationship. It reads the edge
table first, and joins a node table only when the query needs that node's
data. For our example, it reads the authored edges, joins merge requests for
the state and title, and joins users for the username. Tables keep old
versions of a row until ClickHouse merges them away, so every read keeps only
the latest version.

### 6. Narrow reads to the namespaces the query names

When a query names a project or group, the engine filters each read on that
namespace path, so ClickHouse can skip most of the table. Our example names
neither, so it reads every namespace the caller can see. Here is the same
query pinned to one project:

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

The engine reads the project's current path inside the same statement. The
path spreads to the merge requests in that project, and to the authored edges
stored with them. Users are global, so they get no path filter. A path never
spreads across a relationship that can leave the namespace.

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

The plan becomes a SQL syntax tree. The engine asks for one row past the page,
so it can tell whether more rows exist. It caps long text values, so a page
fits in one response, and adds the ID and type columns that GitLab needs for
redaction. Then it plans a second, small query for any properties that the
main query didn't fetch. It also sets time and memory limits for the query
type.

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

Security runs last, on the finished tree. It adds a namespace filter to every
namespaced read, using only the namespaces where the caller holds the role
that the entity needs. Verification then checks every read, and rejects the
query if any read lacks its filter. Last, the engine renders the tree as SQL
for one backend, with every value as a bound parameter.

## When things go wrong

- **A name doesn't match the ontology.** Validation rejects the query before
  planning, with an error that names the problem.
- **The caller lacks the role for an entity on every namespace.** The engine
  rejects the query.
- **A read has no authorization filter.** Verification rejects the statement,
  and it never reaches ClickHouse.
- **The project or group that a query names isn't indexed yet.** The read
  falls back to the caller's authorization filter, so indexed rows still come
  back.
- **A backend can't express an operation.** The engine rejects the query
  instead of approximating it.
- **A step is missing input that an earlier step should have produced.** The
  engine returns an internal error, never partial SQL.

## Why we built it this way

We put every rule for turning a query into SQL in one compiler, so there's one
place to review and test them. Each step declares what it reads and writes,
which lets us reuse steps. Hydration and local graph queries run the same steps
as Orbit API queries. Security always runs last, so verification sees the finished
statement. We read a project's path inside the same statement instead of
looking it up first. So there's no cache that can go stale when a project
moves.

## Trade-offs and what's next

- **New languages are a plan.** SQL and GLQL have no frontend yet. The
  representation is ready for them, but each needs its own grammar and checks.
- **New backends are a plan.** PostgreSQL has no code generator yet. It would
  also need its own data model layout and its own way to read the latest row
  version.
- **Join order uses fixed rules.** The planner orders joins from hints in the
  ontology. [ADR 016](../../adr/016_column_statistics_for_join_ordering.md)
  proposes column statistics to make that choice from data.

## Decisions

- [ADR 000: ClickHouse as graph storage](../../adr/000_clickhouse_graph_storage.md)
- [ADR 016: Column statistics for join ordering](../../adr/016_column_statistics_for_join_ordering.md)
