<!-- markdownlint-disable MD041 -->
<div align="center">

<img src="../../assets/orbit-logo.png" width="120" height="120" alt="GitLab Orbit logo">

# GitLab Orbit: Graph Query Engine

**From a graph query to parameterized SQL**

[Overview](README.md) · [Query languages](querying.md) · [Graph query engine](engine.md) · [Query language reference](../../source/remote/queries/query-language.md)

</div>

GitLab Orbit's query engine turns a graph query into parameterized SQL over typed node tables and adjacency-ordered edge tables.

Data flows through the engine in these steps:

1. A frontend like JSON or GQL parses a graph query into a shared input.
   The [Query languages](querying.md) doc describes the frontends.
1. The engine validates the shared input against Orbit's GitLab ontology, GitLab permissions, and general querying limits.
1. The query planner attempts to pick the most efficient way to execute a graph query.
1. The selected query plan is turned into a SQL syntax tree.
1. Additional engine passes add authorization filters, response formatting, and query settings.
1. A final validation pass is executed to ensure all authorization filters and redaction metadata are present.
1. Code generation renders the syntax tree as SQL for a particular backend, like ClickHouse.

ClickHouse gives Orbit fast aggregations for analytical queries.
The engine adds property-graph semantics on top: traversals, paths, neighborhoods, and grouped counts.
Orbit needs no separate graph database.
[ADR 000](../decisions/000_clickhouse_graph_storage.md) records that decision.

The engine supports four query types: traversal, aggregation, path finding, and neighbors.
This table shows a GQL example of each one, and the SQL that it produces.

> [!note]
> These SQL fragments show only the core of each query.
> The full SQL also has authorization filters, page limits, and query settings.

| Query type | Example (GQL) | Meaning | SQL output fragment |
| --- | --- | --- | --- |
| **Traversal** | `MATCH (u:User)-[:AUTHORED]->(mr:MergeRequest {state: 'merged'}) RETURN u.username, mr.title` | Merged merge requests and the users who wrote them | `FROM gl_edge AS e JOIN gl_merge_request AS mr ON mr.id = e.target_id WHERE e.relationship_kind = ? AND mr.state = ?` |
| **Aggregation** | `MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project {id: 278964}) RETURN mr.state, count(mr) AS mrs` | How many merge requests one project has in each state | `SELECT mr.state, COUNT(mr.id) AS mrs ... GROUP BY mr.state` |
| **Path finding** | `MATCH path = ANY SHORTEST (g:Group {id: 9970})-[:CONTAINS*1..2]->(p:Project {id: 278964}) RETURN path` | The shortest path from a group to a project | `WITH forward AS (...), backward AS (...) SELECT ... FROM forward JOIN backward ON ...` |
| **Neighbors** | `MATCH (mr:MergeRequest {id: 42})--(n) RETURN n` | Everything directly connected to one merge request, in either direction | `SELECT ... FROM gl_edge WHERE source_id = ? UNION ALL SELECT ... FROM gl_edge WHERE target_id = ?` |

---

## Walkthrough

This section maps the parts of the engine, in the order that the rest of this document describes them.
It starts with where the data lives and how the engine reads it.
Then it follows one query through each phase, from text to SQL.

| Theme | Component | What it does |Query Engine Phases |
| --- | --- | --- | --- |
| Data | [Storage model](#storage-model) | Typed node tables and shared edge tables in ClickHouse, all declared by the ontology | None |
| Data | [Query data model](#query-data-model) | The engine's read-only view of one schema version: the graph, its tables, and its authorization rules | None |
| Structure | [Pipelines](#pipelines) | Ordered lists of phases, built from reusable segments, one per frontend and backend | All |
| Querying | [Frontends](querying.md) | Parse GQL or JSON into the shared input, in their own doc | `gql_parse`, `json_dsl_parse` |
| Querying | [Validation](#validation) | Rejects bad queries early, then normalizes names and types | `validate`, `validate_relationships`, `normalize` |
| Querying | [Restriction and scope](#restriction-and-scope) | Applies who is asking, and narrows the query to the namespaces it touches | `restrict` |
| Querying | [Planning](#planning) | Picks how to read each node and relationship | `plan` |
| Querying | [Current-state reads](#current-state-reads) | Makes sure every read sees the latest version of each row | `plan` |
| Querying | [Lowering and code generation](#lowering-and-code-generation) | Turns the plan into a SQL syntax tree, then into SQL for one backend | `lower`, `response_policy`, `codegen` |
| Querying | [Authorization](#authorization) | Adds authorization filters and redaction columns, then proves they are all there | `enforce`, `security`, `check` |
| Querying | [Hydration](#hydration) | Plans a second, small query for the properties the main query didn't fetch | `hydrate_plan` |
| Querying | [Pagination](#pagination) | Pages through results without keeping state between requests | `paginate` |
| Querying | [Settings](#settings) | Picks time and memory limits for each query type | `settings` |
| Summary | [Guarantees](#guarantees) | What the engine promises for every query | All |

This diagram connects the components:

```mermaid
flowchart TB
    accTitle: How the graph query engine fits together
    accDescr: Once per schema version, the ontology produces the ClickHouse storage model and the query data model. For each query, a frontend parses the text, then validation, restriction, planning, lowering, authorization, and the finishing phases turn it into parameterized SQL. ClickHouse runs the SQL, Rails redacts the rows, and hydration fetches the remaining properties before the response goes back.

    subgraph schema_time["Once per schema version"]
        ontology["Ontology"]
        storage[("Storage model<br/>node and edge tables in ClickHouse")]
        model["Query data model<br/>graph, layout, and policy"]
        ontology --> storage
        ontology --> model
    end

    subgraph query_time["Once per query: one pipeline"]
        query_text["GQL or JSON query"]
        frontend["Frontend<br/>parse into the shared input"]
        validation["Validation<br/>shape, limits, names, types"]
        restriction["Restriction and scope<br/>admin-only fields, paths, anchors"]
        planning["Planning<br/>access paths and current-state reads"]
        lowering["Lowering<br/>SQL syntax tree"]
        authorization["Authorization<br/>enforce, security, check"]
        finishing["Pagination, hydration plan,<br/>and settings"]
        codegen["Code generation<br/>ClickHouse or DuckDB SQL"]
        query_text --> frontend --> validation --> restriction --> planning --> lowering --> authorization --> finishing --> codegen
    end

    context["Security context<br/>caller paths and roles"]
    context --> restriction
    context --> authorization

    model -. "names and types" .-> validation
    model -. "tables, routes, and keys" .-> planning
    model -. "role floors and redaction rules" .-> authorization

    codegen --> sql["Parameterized SQL"]
    sql --> storage
    storage --> rows["Rows"]
    rows --> redaction["Rails redaction<br/>outside the engine"]
    redaction --> hydration["Hydration query<br/>through ch_hydration"]
    hydration -.-> storage
    hydration --> response["Response"]
```

---

## Storage model

The ontology defines every table, sort key, and index.

- Each entity has its own node table.
  Namespaced tables lead their sort key with the traversal path, so authorization filters prune the primary index.
- Relationships share edge tables.
  The ontology routes each relationship to a table, with `gl_edge` as the default.
  Edge tables sort by traversal path, relationship kind, and source, so one-hop lookups read a narrow range.
- Every row has a version and a deleted flag.
  The indexer appends new versions, and ClickHouse merges old ones away in the background.
- The ontology can declare a denormalized join: a pre-joined chain of tables that answers several hops in one scan.
  Every table in the chain keeps its own traversal path, and each one is filtered on its own.

[Data model](../data_model.md) describes the entities and relationships.
[Schema management](../schema_management.md) describes how tables change between versions.

---

## Query data model

The query engine never reads the ontology directly.
Instead, each schema version of Orbit derives one immutable query data model from its ontology, and every engine phase reads from that model.

The model splits the ontology into three kinds of facts:

- The **logical graph**: entities, properties, relationships, and relationship variants.
  A variant is one relationship between one pair of entity types, like `(User)-[:AUTHORED]->(MergeRequest)`.
- The **backend facts**: tables, columns, sort keys, foreign keys, and which edge table each relationship variant lives in.
- The **authorization facts**: role requirements, redaction rules, and scope rules.

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

### How a model is put together

Every model has the same three parts, and each part can be swapped on its own:

- The **graph catalog** describes the logical graph.
  It is shared by every backend.
- The **layout** pairs a physical schema with a mapping.
  The schema describes the tables a backend actually has.
  The mapping connects each graph identity, like an entity or a property, to the table and column that stores it.
- The **policy** decides what a caller may see.

```mermaid
flowchart TB
    accTitle: The parts of a query data model
    accDescr: A data model owns a graph catalog, a layout, and a policy. The layout pairs a relational schema for one backend with a mapping from graph identities to columns.
    Model["DataModel&lt;Schema, Mapping, Policy&gt;"]
    Model -->|graph| Graph["Graph catalog"]
    Model -->|layout| Store["Layout&lt;Schema, Mapping&gt;"]
    Model -->|authorization| Policy["Policy"]
    Store -->|schema| Schema["Relational schema<br/>for one backend"]
    Store -->|mapping| Mapping["Mapping from graph<br/>identities to columns"]
    Schema --> Tables["Tables, columns,<br/>and backend options"]
```

The model builds its mapping from the graph and the schema it is given, so a mapping can never be paired with the wrong schema.
A missing property in the mapping means that the backend cannot supply it.

Orbit has two models:

| Model | Schema | Policy | Used by |
| --- | --- | --- | --- |
| `ClickHouseDataModel` | ClickHouse tables, engines, indexes, and views | GitLab authorization | Orbit Remote |
| `DuckDbDataModel` | The local DuckDB tables | Trusted local, since a local graph has one user | The local graph schema and the code-graph test suites |

A new backend, like a document store, supplies its own schema and mapping.
It does not have to look relational, and the graph catalog stays the same.

### Who reads the model

The engine reads the model through the mapping.
The planner uses the mapping to find the edge table for each relationship variant and the column behind each property.
It also uses the mapping to find sorted copies of a table for a faster lookup.

Schema management reads the physical schema directly.
DDL, migrations, cleanup, and view lifecycle all come from the same ClickHouse schema that the engine queries, so the two cannot drift apart.

```mermaid
flowchart LR
    accTitle: Who reads the ClickHouse schema
    accDescr: DDL rendering, migrations, cleanup, view lifecycle, and the engine all read the same ClickHouse schema.
    S["ClickHouse schema"] --> D["DDL rendering"]
    S --> M["Migration inventory"]
    S --> C["Cleanup inventory"]
    S --> V["View lifecycle"]
    S --> E["Engine, through the mapping"]
```

The code lives in `crates/query-data-model/`.

---

## Pipelines

A pipeline is an ordered list of phases.
`crates/query-engine/compiler/src/config.rs` declares every phase, the state it reads and writes, and every pipeline that uses it.
Because each phase declares its inputs and outputs, the same run of phases can show up in several pipelines.

A segment is a run of phases that several pipelines share:

| Segment | Phases |
| --- | --- |
| GQL front | `gql_parse` → `validate` → `validate_relationships` → `normalize` |
| JSON front | `json_dsl_parse` → `validate` → `normalize` |
| Remote core | `restrict` → `plan` → `lower` → `response_policy` → `enforce` → `security` |
| Remote finish | `paginate` → `check` → `hydrate_plan` → `settings` → `codegen` |
| Local tail | `plan` → `lower` → `enforce_local` → `paginate` → `duckdb_codegen` |

This diagram shows how each pipeline combines segments.
Boxes with the same color are the same segment:

```plantuml
@startuml
left to right direction
skinparam componentStyle rectangle

component "GQL front" as gql_a #DCFCE7
component "Remote core" as core_a #DBEAFE
component "Remote finish" as finish_a #FEF3C7
gql_a --> core_a
core_a --> finish_a
note top of gql_a : clickhouse_gql

component "JSON front" as json_b #D1FAE5
component "Remote core" as core_b #DBEAFE
component "Remote finish" as finish_b #FEF3C7
json_b --> core_b
core_b --> finish_b
note top of json_b : clickhouse_json_dsl

component "validate_conditions" as cond_c
component "Remote core" as core_c #DBEAFE
component "check, settings,\ncodegen" as finish_c
cond_c --> core_c
core_c --> finish_c
note top of cond_c : ch_hydration

component "GQL front\nwith validate_local" as gql_d #DCFCE7
component "Local tail" as local_d #FCE7F3
gql_d --> local_d
note top of gql_d : duckdb_gql

component "GQL front" as gql_e #DCFCE7
note top of gql_e : validate_normalize_gql
@enduml
```

Segment reuse has these effects:

- **Both remote languages share everything after parsing.** `clickhouse_gql` and `clickhouse_json_dsl` differ only in their front segment.
- **Hydration reuses the remote core.** It starts from an input the server builds, so it skips parsing, but it runs the same restrict, plan, and security phases.
  Hydration queries can't bypass authorization.
- **Local queries reuse the front.** The DuckDB pipelines parse and normalize the same way.
  Then they swap in a local tail with no security phases, because a local graph has a single user.
- **A front segment can run alone.** When a new schema version loads, the server runs every named query through `validate_normalize_gql` and `validate_normalize`, to prove each one still fits the new ontology.

| Pipeline | Frontend | Backend | Used by |
| --- | --- | --- | --- |
| `clickhouse_gql` | GQL | ClickHouse | Orbit Remote |
| `clickhouse_json_dsl` | JSON Query DSL | ClickHouse | Orbit Remote |
| `ch_hydration` | None | ClickHouse | Hydration queries |
| `duckdb_gql`, `duckdb_json_dsl` | Either | DuckDB | The local graph schema and the code-graph test suites |
| `validate_normalize_gql`, `validate_normalize` | Either | None | Named-query checks when a schema version loads |

---

## Validation

Validation is the first phase after parsing.
It rejects an invalid query before planning starts.
Each check rejects a different kind of mistake, and every rejection is a client-safe error.

```plantuml
@startuml
left to right direction
skinparam componentStyle rectangle
component "Query text" as text
component "Parse" as parse
component "Shape and\nlimits" as shape
component "Ontology names\nand types" as names
component "Relationship\ndirection" as direction
component "Normalize" as normalize
component "Shared input" as input
text --> parse
parse --> shape
shape --> names
names --> direction
direction --> normalize
normalize --> input

note bottom of parse
  MATCH (u:User RETURN u
  rejected: unexpected token,
  line 1, column 15
end note

note bottom of shape
  -[:CONTAINS*1..5]->
  rejected: hops cap at 3
end note

note bottom of names
  MATCH (mr:MergeRequest)
  RETURN mr.titel
  rejected: unknown property
end note

note bottom of direction
  (mr:MergeRequest)-[:AUTHORED]->(u:User)
  rejected: AUTHORED goes
  from User to MergeRequest
end note

note bottom of normalize
  MergeRequest becomes gl_merge_request,
  '42' becomes 42,
  columns * becomes a list
end note
@enduml
```

Validation checks these categories:

- **Syntax.** GQL must match the grammar.
  JSON must match the JSON schema.
  Syntax errors report the line and column, and never echo the query text.
- **Shape.** A query has one to five nodes.
  A traversal has exactly one fewer relationship than nodes.
  A GQL pattern must be one connected tree with unique variables and no cycles.
- **Limits.** Hops and path depth cap at 3.
  `LIMIT` caps at 1,000, and `SKIP` at 10,000.
  A query has at most 16 predicates, 500 node IDs per node, 100 values per `IN` list, and 10 relationship types.
  GQL text caps at 32 KiB and 32 levels of nesting.
- **Ontology names.** Every entity, relationship, and property must exist, and each property must belong to its entity.
- **Types.** Filter values must match the property type.
  `sum` and `avg` need numeric properties.
  Group keys must be stored, filterable properties, and date buckets need date properties.
- **Relationship direction.** In GQL, a typed relationship between two labeled nodes must match the ontology.
  A reversed arrow is rejected with a hint that names the right direction.
- **Normalization.** Once a query is valid, `normalize` resolves entity names to tables, coerces filter values to their property types, and expands wildcard columns.
  It also orders filters into stable predicate lists, so the same query always produces the same SQL.

The limits live as Rust constants, and a build script checks that the JSON schema states the same ones.
The [query language reference](../../source/remote/queries/query-language.md#validation-limits) lists them all.

---

## Restriction and scope

Restriction is the first phase that reads the caller's security context: their organization, plus each traversal path they can read and their role on it.

```plantuml
@startuml
left to right direction
skinparam componentStyle rectangle
component "Shared input" as input
component "Security context\n1/9970/ as Reporter\n1/22/ as Developer" as context
component "restrict" as restrict
component "Restricted input\nand scope" as output
input --> restrict
context --> restrict
restrict --> output

note bottom of restrict
  u.email is admin-only,
  so it is removed for non-admins.
  A requested path outside
  1/9970/ and 1/22/ is rejected.
  Project {id: 278964} is an anchor,
  so the query gets a scope.
end note
@enduml
```

Restriction does three things:

- It removes properties that the ontology marks as admin-only, like `User.email`, unless the caller is an admin.
- It rejects any traversal path the caller asks for that isn't inside their authorized paths.
- It derives the query's scope, so a project-scoped query reads only that project's rows.

### How scope works

A scope starts at an anchor: a node with a known ID or path, like a project, group, merge request, or file.
The anchor's traversal path becomes a `startsWith` filter, which lets ClickHouse prune most of the primary index.

```plantuml
@startuml
left to right direction
skinparam componentStyle rectangle
component "Group 9970" as group
component "Project 278964\n(anchor)" as project #DBEAFE
component "MergeRequest" as mr #DBEAFE
component "WorkItem" as work_item
group --> project : CONTAINS
mr --> project : IN_PROJECT
mr --> work_item : CLOSES

note bottom of project
  scope: the project's own path,
  read in a subquery of the same statement
end note
note bottom of mr
  IN_PROJECT stays in one namespace,
  so the scope spreads here
end note
note bottom of work_item
  CLOSES can cross namespaces,
  so the scope stops
end note
note top of group
  scope never spreads
  from a child to its parent
end note
@enduml
```

The rules for scope:

- The anchor's current path is read in a subquery inside the same statement.
  There is no lookup before the query and no cache.
  A transferred project scopes to its new path as soon as its rows are indexed.
- ClickHouse evaluates that subquery before index analysis, so it prunes like a literal prefix.
- The scope spreads along relationships that the ontology marks as staying in one namespace, like `IN_PROJECT` and `CONTAINS`.
- It never spreads across relationships that can cross namespaces, like `CLOSES`, and never from a child namespace up to its parent.
- A node with several anchors gets one `startsWith` per anchor, combined with `OR`.
  Past eight anchors, the node keeps only the authorization filter.
- A missing or deleted anchor falls back to the authorization filter alone, so the query still returns the rows that are indexed.
- Scope only narrows a query.
  It never widens what the caller can read.

---

## Planning

The planner decides how to read each node and relationship.
It works from facts in the query data model, not from the shape of the query text.
These are some of its strategies.

**Read edges first, and join nodes only when needed.** Edge tables already hold both endpoint IDs, so a query that only needs IDs never touches a node table.

```plantuml
@startuml
left to right direction
skinparam componentStyle rectangle
component "gl_edge\nAUTHORED" as edge_only #DBEAFE
note bottom of edge_only
  RETURN u, mr
  IDs only: one edge read
end note

component "gl_edge\nAUTHORED" as edge_join #DBEAFE
component "gl_merge_request" as node_join #FEF3C7
edge_join --> node_join : target_id = id
note bottom of node_join
  WHERE mr.state = 'merged'
  needs node data: join the node table
end note
@enduml
```

**Union the edge tables a relationship lives in.** The ontology can route each relationship variant to its own edge table.
When a query's relationship spans more than one table, every table becomes one arm of a `UNION ALL`, and every arm returns the same columns.

```plantuml
@startuml
left to right direction
skinparam componentStyle rectangle
component "(u:User {id: 1})-[:AUTHORED]->(x)" as query
component "gl_edge\nAUTHORED to MergeRequest,\nNote, and WorkItem" as default_edge #DBEAFE
component "gl_sec_edge\nAUTHORED to Vulnerability" as security_edge #DBEAFE
component "UNION ALL\nsame columns in every arm" as union
query --> default_edge
query --> security_edge
default_edge --> union
security_edge --> union
@enduml
```

**Narrow a chain as it goes.** In a selective chain, the distinct endpoints of one hop bound the scan of the next hop.

```plantuml
@startuml
left to right direction
skinparam componentStyle rectangle
component "Hop 1\nAUTHORED where source_id = 1" as hop1 #DBEAFE
component "Frontier\ndistinct merge request IDs" as frontier #FEF3C7
component "Hop 2\nHAS_NOTE where source_id IN frontier" as hop2 #DBEAFE
hop1 --> frontier
frontier --> hop2
note bottom of frontier
  (u:User {id: 1})-[:AUTHORED]->(mr)-[:HAS_NOTE]->(n)
  hop 2 reads only the merge requests hop 1 found
end note
@enduml
```

The full set of planning decisions:

- **Relations.** Each relationship in the query becomes one relation.
  Shared nodes connect the relations into a chain or a branch.
- **Node joins.** The planner joins a node table only for node data: a filter, a returned property, a group key, or a sort key.
- **Edge filters.** A node filter can run on the edge row itself when every selected relationship variant carries that property.
- **Routing.** Each relationship variant reads the edge table the ontology routes it to.
  A relationship that spans several tables becomes a union.
- **Foreign keys.** A relationship stored as a foreign key joins node to node on that column, with no edge table.
- **Direction.** A reverse hop filters on the target column, which has its own index.
  It can also read a sorted copy of the table when the ontology declares one.
- **Frontiers.** Selective chains narrow hop by hop, and path finding searches from both ends at once.
- **Reuse.** A relation used more than once becomes a CTE.
  A relation that is only a point lookup, like one ID, is cheaper to repeat inline.
- **Aggregations.** Filters run before grouping, and groups run on the smallest set of rows possible.
  A property group keeps its node table-backed, so security and current-row checks run before the aggregate.
- **Current state.** The planner picks `FINAL` or a selective current-row read for each relation.
  See [Current-state reads](#current-state-reads).
- **Hydration.** The planner decides which properties the main query returns, and which ones hydration fetches later.

The planner may change how a query reads data, but never what it returns.
Plan-shape fixtures pin these choices for the common query shapes.
A planner change that picks a different plan for one of them fails a test.

---

## Current-state reads

Node and edge tables keep several versions of a row until ClickHouse merges them.
A correct query has to ask "does the current version match?", not "did any version ever match?".
Those two questions give different answers for any column that can change back.

```plantuml
@startuml
left to right direction
skinparam componentStyle rectangle
component "Job 1001, version 1\nstatus: failed" as v1
component "Job 1001, version 2\nstatus: success (latest)" as v2 #DCFCE7
v1 --> v2 : retried
note bottom of v1
  any version matches status = 'failed':
  wrongly counted as failed
end note
note bottom of v2
  current version checked:
  correctly not counted
end note
@enduml
```

The engine reads current state in one of two ways:

```plantuml
@startuml
left to right direction
skinparam componentStyle rectangle
component "FINAL\nstream-merge sorted parts" as final #DBEAFE
component "Latest version\nof every row" as final_rows
final --> final_rows
note bottom of final
  the default
  memory stays bounded
end note

component "1. Find candidate keys\nwithout FINAL" as candidates #FEF3C7
component "2. Read every version\nof those keys only" as versions #FEF3C7
component "3. Keep the latest,\nthen filter again" as latest #FEF3C7
candidates --> versions
versions --> latest
note bottom of candidates
  selective reads only:
  exact IDs, narrow ranges,
  frontiers, selective filters
end note
@enduml
```

Where a filter runs depends on whether its column can change:

- **Sort-key and unchanging columns** filter before the latest version is picked.
  Every tombstone carries these columns, so a tombstone passes the filter and hides the deleted row.
- **Changing columns**, like `state` or `status`, filter after the pick, so an old version can't match.
- **The deleted flag** always filters last.

Some more details:

- The selective read is safe for changing columns because finding candidates never decides the result.
  It only limits which keys the second read checks.
- Negated filters, `!=`, `IS NOT NULL`, wide ID ranges, and low-selectivity fields keep `FINAL`.
- Edge-only reads don't join node tables, so the indexer deletes a node's edges in the same batch as the node.
- Hydration reads a tiny, fixed set of IDs, so it picks the latest version without `FINAL` and keeps column pruning.
- Orbit doesn't use `argMax`-style aggregations to resolve the latest version.
  Their memory grows with the number of result rows, which is unknown when the query compiles.
- On settled ClickHouse parts, `FINAL` was faster or equal in every measured case, and stayed bounded.
  A slow `FINAL` aggregation is a sign of fragmented parts, and the fix belongs in storage, not in the query.

---

## Lowering and code generation

Lowering turns the plan into a SQL syntax tree.
Code generation then renders that tree for one backend.
Built-in functions are a closed vocabulary in the tree, so each backend spells them its own way.

```plantuml
@startuml
left to right direction
skinparam componentStyle rectangle
component "Plan\nread gl_merge_request\nwithin path ?" as plan
component "SQL tree\nFilter(StartsWith(mr.traversal_path, p0))" as tree
component "ClickHouse\nstartsWith(mr.traversal_path, {p0:String})" as clickhouse #DBEAFE
component "DuckDB\nstarts_with(mr.traversal_path, ?)" as duckdb #FCE7F3
plan --> tree
tree --> clickhouse
tree --> duckdb
@enduml
```

- Planning decides what to read.
  Lowering decides how to express it as SQL.
  Code generation decides the dialect.
- Every query value stays a bound parameter, all the way to the database driver.
- `response_policy` caps the size of large projected values, like long text, before the result leaves ClickHouse.
- Date buckets keep their units until code generation picks the backend expression.
- When a backend can't support an operation, it rejects the query.
  DuckDB, for example, has no equivalent for ClickHouse's token search.

---

## Authorization

Three phases protect every remote query.
`enforce` adds what redaction needs, `security` adds the authorization filters, and `check` verifies that every read has a filter.

```plantuml
@startuml
left to right direction
skinparam componentStyle rectangle
component "SQL tree\nreads gl_edge, gl_merge_request,\ngl_vulnerability, gl_user" as tree
component "enforce" as enforce
component "security" as security
component "check" as check
component "Safe SQL tree" as safe
tree --> enforce
enforce --> security
security --> check
check --> safe

note bottom of enforce
  adds an ID and type column
  per node, so Rails can redact rows
end note

note bottom of security
  gl_merge_request: startsWith on
  1/9970/ and 1/22/ (Reporter floor)
  gl_vulnerability: only paths where
  the caller holds the higher role
  gl_user: global, no path filter
end note

note bottom of check
  every namespaced read has a filter?
  yes: pass
  no: reject the query
end note
@enduml
```

- Every namespaced node and edge read gets a `startsWith(traversal_path, ?)` filter at the role floor of its entity.
  Most entities need Reporter, and security entities like vulnerabilities need a higher role.
- When the caller holds no path at the right role, the read gets a filter that is always false.
  Some queries are refused outright instead.
  In both cases, the caller gets no rows for that entity.
- Global tables, like `gl_user`, have no traversal path.
  Their rows rely on Rails redaction, and joins through namespaced relationships keep their path filters.
- `check` walks the final SQL tree.
  If any namespaced read lacks a valid filter, the engine rejects the query instead of running it.
- `enforce` adds the columns that redaction needs, so Rails can check rows that SQL can't decide on its own, like confidential items.

The [Security](../security.md) design document describes the full authorization model and the redaction layer.

---

## Hydration

The main query returns identities, plus the properties it already joined.
Hydration fetches the rest in one more small query.

```plantuml
@startuml
left to right direction
skinparam componentStyle rectangle
component "Main query\n(User 1)-[:MEMBER_OF]->(Group 22)\nIDs only" as main
component "Hydration plan\nUser: [1]\nGroup: [22]" as hydration_plan
component "Hydration query\ngl_user WHERE id IN (1)\nUNION ALL\ngl_group WHERE id IN (22)" as hydration_query #DBEAFE
component "Response\nnodes with properties" as response
main --> hydration_plan
hydration_plan --> hydration_query
hydration_query --> response
note bottom of hydration_query
  compiled through ch_hydration,
  so it carries the same
  authorization filters
end note
@enduml
```

- Path finding and neighbors find node types only at run time, so they usually return IDs alone and rely on hydration.
- Nodes that the main query already joined, for a sort or a group key, need no second query.
- The hydration query compiles through `ch_hydration`, so it runs the same restrict and security phases as the main query.
- The caller picks how many columns dynamic nodes return: the ontology defaults, or all columns.

---

## Pagination

The engine pages without keeping any state between requests.
Every page is a new, bounded query.

```plantuml
@startuml
left to right direction
skinparam componentStyle rectangle
component "SKIP 20 LIMIT 20" as request
component "SQL\nORDER BY mr.created_at DESC, mr.id\nLIMIT 21 OFFSET 20" as sql #DBEAFE
component "21 rows back\nkeep 20\nhas_more: true" as trim
component "Redaction drops 2\n18 rows\nhas_more: true" as redacted
request --> sql
sql --> trim
trim --> redacted
note bottom of sql
  one probe row past the page,
  plus tie-breakers for a stable order
end note
@enduml
```

- The engine asks for one row past the page.
  If that row exists, `has_more` is true.
- `SKIP` becomes an `OFFSET` over a stable order.
  Tie-breakers keep pages from overlapping or dropping rows: node IDs for traversals, group keys for aggregations, and path arrays for path finding.
- Redaction runs after SQL, so a page can hold fewer rows than its limit while `has_more` is still true.
  Callers stop when `has_more` is false.

---

## Settings

The `settings` phase picks ClickHouse settings for each query type: time limits, memory limits, and caching.
Path finding has hard ceilings of 15 seconds and 15 GiB.
Server configuration can tighten these limits, but never raise them past the ceilings.

---

## Guarantees

The engine guarantees these properties for every query:

- **One path to SQL.** The engine is the only path from a query to SQL.
  Every safety rule lives in one place.
- **Values never become SQL.** Query values are bound parameters, and names must match the ontology.
  The SQL tree accepts only a closed set of functions.
- **Authorization fails closed.** Every namespaced node and edge read carries an authorization filter at the role floor of its entity.
  The `check` phase rejects any query with a read that lacks one.
- **Every query is bounded.** Shape limits cap nodes, relationships, hops, path depth, predicates, ID lists, and result size.
  The `settings` phase adds a time and memory limit for each query type.
- **Reads see current state.** A query reads the latest version of each row.
  A faster plan never returns stale rows.
- **The plan does not change the result.** The planner may change how a query reads data, but never what it returns.
- **Phases declare their state.** A phase reads and writes only the state that it declares.
- **The query tier is read-only.** The webserver connects to ClickHouse as a user that can only run `SELECT`.

---

## Appendix

### Glossary

- An **anchor** is a node with a known ID or path, like a project, that a query's scope starts from.
- A **frontier** is the set of distinct endpoints that one hop hands to the next.
- A **phase** is one engine step, such as `validate` or `plan`.
- A **pipeline** is an ordered list of phases for one frontend and one backend.
- The **query data model** is the engine's view of one schema version: the logical graph, its physical layout for one backend, and its authorization policy.
- A **relation** is one read of a node or edge table in the plan.
- A **scope** is a narrow traversal path prefix that the engine derives from the query, such as the path of one project.
- A **segment** is a run of phases that several pipelines share.
- A **tombstone** is the row version that marks a row as deleted.

### Observability

Each query reports the time of each phase, the rows it returned, and the rows, bytes, and memory that ClickHouse used.
Authorized callers can see the compiled SQL and its parameters.
[Observability](../observability.md) lists the metrics.

### Testing

Query scenarios check results in both languages.
Plan-shape fixtures check the plan the engine picks.
Fuzz targets check the frontends.
[Testing](../testing.md) describes each layer and its CI job.

### Related documents

- [Querying overview](README.md)
- [Query languages](querying.md)
- [Security](../security.md)
- [Data model](../data_model.md)
- [Schema management](../schema_management.md)
- [ADR 000: ClickHouse as graph storage](../decisions/000_clickhouse_graph_storage.md)
