# Graph engine

## Overview

We will use ClickHouse as the primary database for graph queries in deployed environments. The query tier compiles high‑level graph operations into ClickHouse SQL and executes them directly on adjacency‑ordered edge tables and typed node tables. This pairs OLAP throughput with property‑graph semantics (paths, traversals, pattern matching) without introducing another datastore.

This plan draws on the [team's research](https://gitlab.com/gitlab-org/rust/knowledge-graph/-/issues/267). The approach keeps the service stateless and focuses on schema design, SQL generation, and guardrails.

## Storage Model in ClickHouse

The ontology defines the ClickHouse storage model, including table routing, sort keys,
primary keys, and secondary indexes.

- Each node type has a dedicated `ReplacingMergeTree` table. Namespaced node tables
  usually lead their sort key with `traversal_path`; individual ontology declarations
  can add entity-specific columns such as project, branch, or local identifiers.
- Relationship types share physical edge tables. `settings.edge_tables` declares those
  tables, `settings.default_edge_table` selects `gl_edge` as the default, and an edge YAML
  can use `table:` to route a relationship elsewhere. The current routes include
  dedicated tables for code, CI/CD, security, and merge request diff relationships.
- `traversal_path` is the slash-delimited namespace hierarchy for a namespaced row. The
  query engine applies `startsWith` predicates to namespaced node and edge scans, using
  exactly the paths Rails authorized. See [Security Architecture](../security.md).
- Code tables can also carry project and branch columns. Their ontology declarations put
  these columns into the physical sort and primary keys where required. See
  [Code Indexing](../indexing/code_indexing.md).

### Edge table schema

Each physical edge table has ontology-defined columns and storage settings. For example,
the default `gl_edge` table uses this key and these ID indexes in the generated DDL:

```sql
PRIMARY KEY (traversal_path, relationship_kind, source_id)
ORDER BY (
  traversal_path, relationship_kind, source_id, target_id, source_kind, target_kind
)
INDEX idx_source_id source_id TYPE bloom_filter(0.0001) GRANULARITY 1
INDEX idx_target_id target_id TYPE bloom_filter(0.0001) GRANULARITY 1
```

Leading with `traversal_path` lets authorization and namespace scope prune the primary
index. `relationship_kind` then groups the relationship types routed to the same table.
Bloom filter indexes support source and target ID lookups. The current ontology-backed
edge DDL does not define source- or target-ordered projections.

## Query Engine Design

The compiler supports two query frontends:

1. The JSON Query DSL describes traversal, neighbors, path-finding, and aggregation queries. Remote requests through MCP, HTTP, and gRPC default to this frontend for compatibility.
2. The [Orbit Query Frontend](orbit_query_frontend.md) accepts a restricted, read-only language based on openCypher 9 syntax. Rails selects the compiler preset with the default-off `orbit_gql_queries` feature flag. The protobuf `QueryLanguage` enum is independent of the raw or named query kind. Both modes use the same endpoint and authorization pipeline, but each accepts only its own query shape. This is not a Neo4j-compatible driver.

### Compiler pass pipeline

Both frontends compile to parameterized ClickHouse SQL through shared passes.
`crates/query-engine/compiler/src/config.rs` defines the `clickhouse_json_dsl` and `clickhouse_gql` presets for graph queries.
`compiler::gql::prepare` parses once: MATCH enters the shared graph passes, while `CALL db.schema(...)` resolves ontology metadata inside the GQL frontend without SQL.
Schema calls have no state in the shared compiler contexts. `compiler::compile` remains query-only for both frontends.

Each active schema snapshot derives one immutable query data model from its loaded ontology.
The data model assigns typed IDs to entities, properties, relationships, and relationship variants.
Its backend catalog resolves tables, columns, edge routes, foreign keys, sort keys, and denormalized properties.
Its authorization catalog resolves GitLab redaction and scope metadata.
Each stored property realization contains its physical query column. An absent realization means that the backend cannot supply that property.
Foreign-key facts identify the source or target endpoint that holds the key, its property, and the referenced ID property.
The endpoint remains unambiguous for self-relationships and incoming traversals.
Planning resolves the referenced property's column for FK joins and filtering subqueries. Graph IDs remain separate; direct ID substitution requires a reference to the graph ID column.
The current ontology files, archives, DDL, and indexing declarations remain unchanged.
Planning resolves backend facts into execution requirements. Lowering translates those requirements into the SQL AST and physical result bindings.

Compiler input carries ordinary filters in node and relationship maps, and condition trees with `AND` and `NOT` in `Input.predicates`.
Only the GQL frontend builds trees; the JSON Query DSL does not expose them.
A tree holds cross-node comparisons and the `NOT` conditions that have no exact complement.
Validation and restriction check both sides of every leaf, and normalization coerces enum values inside each tree.
Planning binds a tree that reads one node to that node's scan. It applies other trees after the joins, where a node ID reads the edge column that binds the node.
Nodes and relationships that a tree reads keep their table scans: foreign-key elision and FK chains skip them, and aggregation scope proofs exclude them.
Trees never make a node selective, so a node whose only condition is `NOT` gets no candidate or filter keys.
Queries that keep a `NOT` node after normalization set `transform_null_in = 0`, so a NULL value under `IN` stays NULL at every depth of negation. A complemented `NOT` is an ordinary filter and does not set it.
All later passes continue to use that AST. Planning does not construct SQL expressions, query blocks, function calls, or casts.
Pure catalog and filter-value helpers live under planning; SQL construction helpers live under lowering.
The SQL AST records aggregate functions, optional arguments, distinctness, and conditions as structured values.
Built-in calls use a closed `Function` vocabulary rather than SQL names. Each renderer owns function spelling, argument placement, and dialect syntax.
The emitted explain view prints exact `Function` variant names, such as `StartsWith`. ClickHouse codegen renders `startsWith`; DuckDB renders `starts_with`.
Security checks recognize the prefix operation directly. String byte length is distinct from character substring operations.
Shared parameter types live in `orbit-utils::query_types`. They describe scalar and array values, dates, and timestamps with precision and timezone intent.
ClickHouse type spelling belongs to its adapter. DuckDB renders temporal casts and binds string values without ClickHouse-specific literal syntax.
Codegen renders ClickHouse combinators or DuckDB aggregate filters. Time buckets retain their units until codegen selects the backend expression and result type.
Week buckets start on Sunday. Day and larger buckets return dates; minute and hour buckets retain timestamp output.
Collection aggregates omit null inputs and return an empty array for empty input. Other aggregates retain their backend result conventions, including empty sums and averages.
Token search records single, all, or any matching. ClickHouse renders its native token functions; DuckDB rejects these operations because equivalent tokenizer semantics are not available.
CTE bodies use the complete query renderer, including nested definitions. DuckDB omits the outer limit of a recursive CTE body.
UNION arms also retain their local CTE definitions. Empty path arrays carry field types instead of relying on dummy values in lowering.

`mise test:plan-shape` checks YAML fixtures with `query.json` and `query.gql` arms.
The shared runner and structural matcher live in `integration-testkit::plan_shape`.
Logical assertions inspect parsed, normalized Input before scope preparation.
They include node and relationship filters, groups, measures, ordering, and limits.
Physical assertions select `planned` or `emitted` under each backend, keeping the selected plan separate from the SQL AST before enforcement.
The explain view renders incoming relationships in source-to-target order so both frontends can share assertions.
Patterns use the operator grammar from !2590: `(_)` matches one subtree and `(...)` matches zero or more children.
`...` permits additional Filter, Project, or Aggregate items; a trailing head `...` matches a token prefix.
These assertions check plan structure; data-correctness scenarios check execution results.

### Plan fixture assertions

Each assertion block supports `expect`, `reject`, `bind`, `occurrences`, `ctes`, and `exact`.
Use multiline patterns for nested operations. Expressions use readable column references and preserve explicit grouping and quoted literals.

```yaml
physical:
  clickhouse:
    planned:
      bind:
        - (CTE $projects (Project p.id AS id (_)))
      expect:
        - (Filter p.id IN $projects.id, ... (_))
      occurrences:
        - pattern: (CTE $projects (_))
          count: 1
      ctes:
        exact_order: [$projects]
    emitted:
      expect:
        - (Scan Table(gl_project) AS p)
```

`bind` must match exactly one subtree and introduce a named capture. Later assertions may reference it but cannot introduce new captures.
Captures are local to one assertion block and one frontend/backend run. They capture syntax, not scoped SQL relation identities.
`?` matches one expression token or parenthesized group; `$name` captures that same unit and requires later uses to agree.
Quoted `'?'`, `'$name'`, and `'...'` are literal strings. Failed matches do not change capture bindings.

Item lists match without order, preserve duplicate counts, and require `...` to admit extra items.
Child order remains significant. Expressions retain token order and grouping; the matcher does not infer algebraic equivalence.
`exact` compares the whole ordered tree, including projection order, and rejects wildcards and captures.
`ctes.exact_order` checks the complete top-level CTE sequence, including captured names.
Use `ctes.absent: true` to assert that no top-level CTEs exist. Omit `ctes` to leave definitions unchecked.
The two forms are exclusive. Empty lists, empty blocks, and `absent: false` are rejected.
Failures report the fixture path, frontend, backend, phase, and assertion index. Missing-pattern errors include the first subtree with the requested operator.

The planned Project view lists requested outputs; it does not assert a closed projection schema for later compiler phases.
Scope-aware relation captures remain deferred to the query-local SQL identity work.
Family explain views include neighbor access and routes, path frontier depths and endpoints, and hydration projections, paths, and dedup keys.
Hydration fixtures use ordinary JSON/GQL node selectors plus an internal `hydration` setup block with `dynamic`, `path_segment_budget`, and alias-keyed `paths`.
The harness applies that setup after normalization; hydration remains unavailable as a user query type.

Each edge-chain emitter builds node bindings as it emits scans and joins.
Each binding contains the graph identity, visible table alias, and hydration path expression when available.
Lowering resolves elided endpoints through their emitted holders or pinned literals.
Filtering CTEs do not make a node table visible in the result query block.
Result enforcement consumes these bindings instead of searching the SQL tree for aliases.
It still adds required authorization scans and projects redaction columns.
Neighbors emits direction-specific redaction columns in its own query arms.
Its projected binding carries only an identity needed for an additional role scan, when required.
Property comparisons and cursor identities reuse the resolved node bindings.
Lowering rejects identities that have neither a visible source nor an elided pinned value.
FK hop elision retains elevated-role targets for authorization scans.
It also retains multi-ID targets when projection, grouping, property aggregation, or ordering needs their values.
Ungrouped aggregations can still elide targets used only as filters.

| # | Pass | Responsibility |
|---|---|---|
| 1 | `json_dsl_parse` or `gql_parse` | Lowers raw graph-query text to `Input`; GQL preparation supplies parsed Input instead. The JSON frontend also validates the JSON schemas and computes the cursor query hash |
| 2 | `validate` | Checks native `Input` shape, bounds, ontology membership, and cross-references |
| 3 | `normalize` | Resolves entity names to table names, coerces filter types, and expands wildcard columns |
| 4 | `restrict` | Strips `admin_only` fields, validates user-supplied paths, and prepares query scope, including scope-only container removal ([Security](../security.md)) |
| 5 | `plan` | Chooses performance-equivalent access paths, join order, hydration, and dedup strategies |
| 6 | `lower` | Emits the SQL AST and physical result bindings from the query plan |
| 7 | `response_policy` | Applies transport-size policy to result projections |
| 8 | `enforce` | Adds role-gated scans and redaction columns, then builds the result context |
| 9 | `scope_requirements` | Applies scope predicates inside each scan's query block and guards for removed scope anchors |
| 10 | `security` | Injects `startsWith(traversal_path, ?)` predicates on all namespaced node and edge scans, with per-entity role scoping ([Security](../security.md)) |
| 11 | `cursor` | Applies keyset pagination (stable order, probe limit, seek predicate, and readback columns) |
| 12 | `check` | Verifies every namespaced graph-table alias carries a valid `startsWith` predicate traceable to the `SecurityContext` ([Security](../security.md)) |
| 13 | `hydrate_plan` | Builds the hydration plan for entity properties the base query does not already project; nodes joined inline (FK shapes, sort and group targets) need no second query |
| 14 | `settings` | Resolves ClickHouse query-level settings (timeouts, memory limits, cache) for the query type |
| 15 | `codegen` | Serializes the AST into parameterized ClickHouse SQL |

The planner emits ClickHouse SQL similar to these patterns:

- One‑hop neighbors: equality filter on the edge table’s leading keys, `WHERE startsWith(traversal_path, ?) AND branch = ? AND src_id IN (...)` (for code) or `WHERE startsWith(traversal_path, ?) AND src_id IN (...)` (for SDLC), producing O(degree) scans per source.
- Multi-selector traversals: chained JOINs/CTEs with DISTINCT frontiers between selectors to avoid blow-ups. A traversal can contain up to five node selectors and therefore four relationship selectors. Each relationship selector can independently use an inclusive `hops` range whose upper bound is 3.
- Path finding: bounded expansion over the routed edge tables, with `path.max_depth` capped independently at 3.
- Reverse hops: filter the edge table on `target_id`, supported by its target ID bloom filter.
- Alternate relationship types: a query's relationship types may span multiple physical edge tables, or use a wildcard. In that case the compiler emits a `UNION ALL` across the relevant tables. Each arm selects the standard edge columns so downstream passes see a uniform schema.
- Aggregations: push filters early; perform groupings on the smallest necessary sets; avoid post‑filtering of large results. Top-level `group_by` supports node groups and scalar property groups. Property groups keep the grouped alias table-backed, so security filters and latest-row checks apply before aggregation.
- HAVING filters: `GROUP BY ... HAVING aggregate_expr > threshold` for post‑aggregation filtering.
- Derived‑table subqueries: `(SELECT ... FROM table FINAL WHERE ...) AS alias` in FROM/JOIN positions. This applies when a latest-row node scan has filters or narrowing predicates that should be applied inside the `FINAL` read. FK-star center scans and joined node scans use this shape.
- Narrowing CTEs: edge-derived narrowing CTEs use `SELECT DISTINCT` for ID frontiers so high fan-out relationships do not feed millions of duplicate values into an `IN` set.
- Cross-node comparisons resolve IDs through the plan's edge or FK columns. Other properties require node joins before comparison. FK hop removal keeps relationships needed by these comparisons.
- FK candidate prefilters: joined FK plans may add `SELECT DISTINCT id FROM table WHERE ...` CTEs without `FINAL`. They then constrain the outer `FINAL` scan with `id IN (...)` and re-apply every predicate after latest-row resolution. Center candidate CTEs are only emitted when they include target-derived predicates. The compiler does not build a same-table center candidate that only repeats the center node's own filters.
- Row deduplication: `ReplacingMergeTree` does not guarantee merge-time dedup between queries, so the compiler injects query-time dedup (see [Row deduplication](#row-deduplication) below).

These choices preserve factorization. Each hop operates on a compact frontier and prunes the next edge scan via semi‑joins. This mirrors Kùzu’s accumulate → semijoin → probe execution.

### Row deduplication

Node and edge tables use `ReplacingMergeTree(_version, _deleted)`. Between background merges, queries can see stale row versions and soft-deleted rows. The ClickHouse compiler ensures query-time correctness for node table reads, mostly via `FINAL`. Hydration arms and narrowed node joins instead dedup with `LIMIT 1 BY <sort_key>`. This preserves the same latest-non-deleted semantics while keeping column pruning and projections (see the Hydration row below):

| Scan type | Strategy | Rationale |
|---|---|---|
| Single-node traversal | Node table scan with `FINAL` | Applies `ReplacingMergeTree` latest-row semantics before filters and limits |
| Node filter CTEs | Node table scan with `FINAL` | Ensures ID frontiers are derived from latest rows, not stale matching versions |
| FK candidate CTEs | Non-`FINAL` ID or FK prefilter plus a latest-row recheck | Candidates can include stale matches. The target scan selects the latest version before rechecking filters. |
| Edge narrowing CTEs | Non-`FINAL` edge-endpoint ID frontier | Narrows joined node scans to IDs the edges reach |
| Redaction joins for filtered non-default auth IDs | Filtered node table subquery with `FINAL` | Lets enforcement joins for entities such as code definitions apply property filters inside the latest-row read |
| Hydration (UNION ALL arms) | Non-`FINAL` scan with `LIMIT 1 BY <sort_key> ORDER BY <sort_key>, _version DESC`, outer `_deleted = false` | Hydration reads a tiny pinned `id IN (...)` set; dropping `FINAL` lets column pruning and projections apply (`FINAL` reconstructs full rows, defeating both). Dedup identity is the table's full sort key, matching `FINAL`'s per-ORDER-BY-key semantics. Falls back to `FINAL` when a table has no sort key. |
| Main query node scans and broad node joins | Node table scan with `FINAL` | Keeps traversal, FK center, aggregation, and single-node lookup semantics consistent |
| Narrowed node joins | Candidate-ID and sort-key filters before `LIMIT 1 BY <sort_key> ORDER BY <sort_key>, _version DESC`; other node filters and `_deleted = false` outside | Sort-key values are the same for every version of a row, so those filters keep primary-key pruning. Mutable filters and deleted checks run after deduplication. |
| Edge scans | `_deleted = false` in WHERE | Full-tuple ORDER BY makes RMT merge effective; only soft-delete filtering needed |

Filter placement rules for node `FINAL` scans:

- **Structural filters** (`traversal_path`, `id`, `project_id`, `branch`) are emitted on the `FINAL` scan so ClickHouse can still use primary-key pruning where supported.
- **Mutable filters** (`state`, `status`, `draft`) also evaluate against the `FINAL` scan, preventing stale row versions from matching.
- **`_deleted = false`** is always applied after latest-row resolution, either on the `FINAL` scan or outside a wrapping subquery.
- **Candidate CTEs** may over-select because they are only a performance prefilter. The target scan resolves the latest row before re-applying node filters and `_deleted = false`.
- **Pinned FK target IDs** are pushed into the FK center `FINAL` subquery when the FK column lives on the center table.

Edge-only traversals do not join node tables for non-group-by nodes, so they cannot filter out deleted nodes at the query layer. In production this is handled by the SDLC indexer, which soft-deletes FK edge rows in the same ETL batch as their parent node (`crates/indexer/src/modules/sdlc/pipeline.rs`). Cross-entity FK cleanup relies on PostgreSQL's referential integrity propagating through Siphon CDC.

### Denormalized joins

A denormalized join pre-joins a linear chain of tables into one `gl_denorm_<name>` table. The compiler can then answer the matching hops with a single scan. It is declared as a chain of edge variants. Each variant is realized either through its edge table, or, with `via: fk`, directly node to node on the variant's FK column:

```yaml
denormalized_joins:
  - name: reviewer_project
    hops:
      - {relationship: REVIEWER,   from: User,         to: MergeRequest}
      - {relationship: IN_PROJECT, from: MergeRequest, to: Project, via: fk}
```

That resolves to the table chain `gl_user, gl_edge, gl_merge_request, gl_project`.
Adjacent tables join on the ID or edge ID that links them.
Every scoped table keeps its own `traversal_path` in the row, exactly as each scan alias keeps its own in an ordinary join.
Every other column of every table is copied under a `t{i}_` prefix.
The first scoped table's `traversal_path` is the row's unprefixed one and leads the sort key.
The DDL generator composes the table from the source tables' already-generated definitions (columns, codecs, indexes, settings, partitioning).
It emits one `TO` materialized view per table, joined outward from the trigger with `FINAL`.
The security pass filters each path column in the row against the authorized set (see `Ontology::traversal_path_columns`), each at its own table's role floor.
So a hop may cross namespaces just as it may in an edge chain.
A row is returned only when the caller is authorized for every namespace it touches.
The loader only requires that at least one table in the chain is scoped.

Before declaring a join in `schema.yaml`, trial it as an ontology overlay under `config/seeds/overlays/<name>/`. That directory mirrors `config/ontology/` and is merged over it. Run the data correctness suite against it with `mise test:integration:overlay <name>`. The suite creates the table and its views from the seed. It checks the table holds exactly the rows the live source join produces. It runs the YAML query scenarios against the overlaid ontology.

### Scope rewrite (traversal_path prefix injection)

Project- and group-scoped `traversal` and `aggregation` queries add a tight `startsWith(traversal_path, <prefix>)` predicate so the primary key prunes the scan.

**When a node is scoped**

- It carries one of two things. Either a single `id`, `full_path`, or up to eight `node_ids` for an anchor entity. Or a single equality filter on a `namespace_anchor` FK column such as `project_id`.
- Anchors and FK columns come from the ontology's `traversal_path_lookup` declarations and edge scope annotations (`Ontology::is_anchor`, `Ontology::anchor_fk_mappings`).
- Anchors: `Project`, `Group`, `MergeRequest`, `Definition`, `File`, `Directory`. The code entities let "find callers" traversals scope to the symbol's own project.

**How the prefix is produced** (`scope::derive_scope_prefixes`, `ScopePrefix`)

- No pre-query lookup. The compiler emits a scalar subquery in the same statement: `(SELECT coalesce(if(argMaxOrNull(_deleted, _version), NULL, argMaxOrNull(traversal_path, _version)), '0/') FROM <anchor table> AS _scope WHERE _scope.<key> = ?)`.
- ClickHouse evaluates it once before index analysis, so pruning equals a literal prefix (production `EXPLAIN`: 273 of 39 350 granules for both forms).
- ID lookups use an aggregate over all matching versions. Full-path lookups use `FINAL` before filtering the mutable path, so an old name cannot resolve a renamed anchor.
- A missing or deleted anchor yields `0/`. The predicate then falls back to the authorization filter alone (`startsWith(...) OR <lookup> = '0/'`). So rows whose anchor row is not indexed yet still return, as with the old resolver.
- When scope preparation removes a container anchor, scope application adds `<lookup> != '0/'` to the query. A missing anchor then yields no rows, instead of counting the whole authorized scope.
- Containment elision preserves the requested direction and constrains the target's traversal-path depth. The compiler retains the target's table scan even when the query does not return its properties.
- Several anchors on one node give one `startsWith` per anchor, OR-ed. Above eight the node keeps only the authorization filter.
- The lookup reads the anchor's current row, so a transferred project scopes to its new location as soon as its rows are indexed. No cache, no staleness window.

**Where it lands**

- Scope preparation runs after restriction. It removes a scope-only container only when the target's scope and hop depth exactly replace the relationship. It stores the bounded target proof in scope state and gives planning only the target alias that needs a table scan.
- Planning and lowering do not consume scope proofs. Edge scans retain their input relationship index, including scans inside SIP producers and bounded-hop arms.
- Scope application walks the emitted AST after result enforcement. It uses scan provenance to add predicates inside each scan's query block, including dedup subqueries and CTEs.
- Removed containers retain a resolved-anchor guard. Security injection then adds caller authorization filters beside the scope filters.
- Scope application, security injection, and final checks share callback-based query walkers. The walkers cover queries nested in expressions as well as derived tables and CTEs.

**Propagation** (`Ontology::propagate_scope_prefixes`)

- Edge variants declare `scope`: `namespace_anchor`, `same_namespace`, or omitted for cross-namespace.
- Group containment propagates a parent's prefix to its descendants, never a descendant's prefix to its parent. This follows the stored edge direction, including incoming query selectors. A project's path cannot constrain its ancestor group or that group's membership edges.
- Other scope-preserving variants propagate prefixes in both directions. A two-pass taint walk resolves the exact variant and refuses aliases reachable through a cross-namespace edge.
- Cross-namespace relationships such as `CLOSES` do not propagate, so multi-edge traversals stay correct. This is what lets a 2+ edge project-scoped traversal seek the project's PK range instead of scanning the org-wide edge table (#601941).

## Request Flow (Deployed)

1. Client (MCP or REST) submits a tool call or Cypher.
2. Adapter validates/normalizes input pursuant to the currently deployed schema. For SDLC queries it computes the user's `traversal_path` prefixes, which encode the organization ID as their first segment. For code queries it selects the active `branch`.
3. Planner compiles to ClickHouse SQL (CTEs, recursive CTEs, unions, joins) with bound parameters.
4. ClickHouse executes; the server returns rows plus the generated SQL for audit.

### Unified Response Format

The server fetches one probe row beyond the requested window, trims it, and derives honest pagination metadata (`has_more`, `truncated`, `next_cursor`). Keyset cursors (`{ page_size, after }`) lower into seek predicates in SQL, so each page is a fresh bounded query. There is no offset slicing and no cross-page result cache. The formatting stage then transforms the trimmed `QueryResult` into the output payload. [ADR 004](../decisions/004_unified_response_schema.md) defines the format: a unified `{ format_version, query_type, nodes, edges, columns?, group_columns?, rows?, pagination? }` shape for all four query types (traversal, aggregation, path_finding, neighbors) with deduplicated nodes and instance-level edges. `format_version` (semver) lets consumers detect breaking changes.
Aggregation queries include `columns`, `group_columns`, and `rows` for table-shaped analytics output.
A `GraphFormatter` handles the transformation, and a JSON Schema defines the response contract between server and frontend.

Namespace graph updates arrive via an ETL worker, described in [SDLC Indexing](../indexing/sdlc_indexing.md). The indexer publishes a small state record (namespace → active state). The web tier caches namespace metadata and injects appropriate filters into queries; no file swapping is required.

Direct projections and hydration apply ontology-derived [text excerpts](../../source/remote/queries/query-language.md#text-excerpts) in SQL before serialization, without changing filters, joins, authorization, grouping, sorting, or cursor keys.

## Authorization and Safety

- Hard filters in SQL: every query carries `startsWith(traversal_path, ?)` predicates scoped to the caller's authorized namespaces. Organization isolation is implicit, because the org ID is the first path segment.
- Redaction layer: final pass to drop rows the upstream filters could not precisely exclude (e.g., confidential flags). Avoid redaction for aggregates; either pre‑filter or block the query shape.
- All queries will be parameterized.
- Depth caps and relationship allow‑lists to prevent runaway traversals; row and time limits per request.
- Grammar‑based query validation: the planner validates generated SQL against a strict subset of the ClickHouse grammar. The validator walks the AST to verify that required predicates (e.g., `startsWith(traversal_path, ?)`) are present in `WHERE` clauses and rejects queries that omit them. This "fail closed" approach ensures malformed or overly broad queries are blocked before execution rather than relying solely on downstream filtering.

## Observability

- Per-phase timings (parse/plan/render/execute) and row counts.
- Emitted SQL and parameter map for debugging.
- Per-query ClickHouse resource stats (`read_rows`, `read_bytes`, `memory_usage`) extracted from the `X-ClickHouse-Summary` response header on every query. When profiling is enabled, these are enriched from `system.query_log`.
- Query result cache metrics: lookups (hit/miss/error), stores (success/error/too_large), evictions (per_user_limit).

## Integration with Indexing

The indexer writes denormalized, typed node and edge tables in ClickHouse via ETL rather than synchronous materialized views. The exact mechanisms for this are covered in [SDLC Indexing](../indexing/sdlc_indexing.md) and [Schema Management](../schema_management.md). Materialized views would require filtered license checks on every inserted row, reducing ingestion efficiency. ETL decouples transformation from ingestion, allowing the indexer to batch writes and maintain control over schema evolution without impacting ClickHouse insert performance. Materialized views are reserved for precomputing stable summaries (e.g., group closure) that change infrequently and do not require per-row filtering. But these are optional enhancements for performance and may be subject to change.
Edge lookups use the ontology-declared sort keys, primary keys, and bloom filter indexes rather than per-table projections.

## Security and performance testing

Security query scenarios, plan-shape fixtures, performance scenarios, and local fuzz targets check the query engine. For each test layer and its CI job, see [Testing](../testing.md). For the authorization model, see [Security Architecture](../security.md).

## References

- graphhouse experiments and benchmarks (multi‑table schema, adjacency ordering, recursive CTEs)
- ClickGraph engine (Cypher → ClickHouse SQL, recursive CTEs, path functions, Bolt/HTTP front ends)
