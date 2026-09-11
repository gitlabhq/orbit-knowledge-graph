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

1. The JSON Query DSL describes traversal, neighbors, path-finding, and aggregation queries. Remote requests through MCP, HTTP, and gRPC use this frontend.
2. The [Orbit Query Frontend](orbit_query_frontend.md) accepts a restricted, read-only language based on openCypher 9 syntax. It is a compiler preset, not a remote endpoint or Neo4j-compatible driver.

### Compiler pass pipeline

Both frontends compile to parameterized ClickHouse SQL through shared passes.
`crates/query-engine/compiler/src/config.rs` defines the `clickhouse_json_dsl` and `clickhouse_gql` presets, which differ only in the first phase:

| # | Pass | Responsibility |
|---|---|---|
| 1 | `json_dsl_parse` or `gql_parse` | Lowers raw text to `Input`; the JSON frontend also validates the JSON schemas and computes the cursor query hash |
| 2 | `validate` | Checks native `Input` shape, bounds, ontology membership, and cross-references |
| 3 | `normalize` | Resolves entity names to table names, coerces filter types, and expands wildcard columns |
| 4 | `restrict` | Strips `admin_only` fields and validates user-supplied `traversal_path` filters against the JWT-granted scope ([Security](../security.md)) |
| 5 | `plan` | Translates validated input into a query plan (hop chain, join strategy, FK shape) |
| 6 | `lower` | Emits the SQL AST from the query plan (edge-chain-first, nodes lazy) |
| 7 | `enforce` | Injects ID and type columns required for redaction; builds the result context |
| 8 | `security` | Injects `startsWith(traversal_path, ?)` predicates on all namespaced node and edge scans, with per-entity role scoping ([Security](../security.md)) |
| 9 | `cursor` | Applies keyset pagination (seek predicate and readback columns) |
| 10 | `check` | Verifies every namespaced graph-table alias carries a valid `startsWith` predicate traceable to the `SecurityContext` ([Security](../security.md)) |
| 11 | `hydrate_plan` | Builds the hydration plan for fetching entity properties after the base query |
| 12 | `settings` | Resolves ClickHouse query-level settings (timeouts, memory limits, cache) for the query type |
| 13 | `codegen` | Serializes the AST into parameterized ClickHouse SQL |

The planner emits ClickHouse SQL similar to these patterns:

- One‑hop neighbors: equality filter on the edge table’s leading keys, `WHERE startsWith(traversal_path, ?) AND branch = ? AND src_id IN (...)` (for code) or `WHERE startsWith(traversal_path, ?) AND src_id IN (...)` (for SDLC), producing O(degree) scans per source.
- Multi-selector traversals: chained JOINs/CTEs with DISTINCT frontiers between selectors to avoid blow-ups. A traversal can contain up to five node selectors and therefore four relationship selectors; each relationship selector can independently use an inclusive `hops` range whose upper bound is 3.
- Path finding: bounded expansion over the routed edge tables, with `path.max_depth` capped independently at 3.
- Reverse hops: filter the edge table on `target_id`, supported by its target ID bloom filter.
- Alternate relationship types: when a query's relationship types span multiple physical edge tables (or use a wildcard), the compiler emits a `UNION ALL` across the relevant tables. Each arm selects the standard edge columns so downstream passes see a uniform schema.
- Aggregations: push filters early; perform groupings on the smallest necessary sets; avoid post‑filtering of large results. Top-level `group_by` supports node groups and scalar property groups, and property groups keep the grouped alias table-backed so security filters and latest-row checks apply before aggregation.
- HAVING filters: `GROUP BY ... HAVING aggregate_expr > threshold` for post‑aggregation filtering.
- Derived‑table subqueries: `(SELECT ... FROM table FINAL WHERE ...) AS alias` in FROM/JOIN positions when a latest-row node scan has filters or narrowing predicates that should be applied inside the `FINAL` read. FK-star center scans and joined node scans use this shape.
- Narrowing CTEs: edge-derived narrowing CTEs use `SELECT DISTINCT` for ID frontiers so high fan-out relationships do not feed millions of duplicate values into an `IN` set.
- FK candidate prefilters: joined FK plans may add `SELECT DISTINCT id FROM table WHERE ...` CTEs without `FINAL`, then constrain the outer `FINAL` scan with `id IN (...)` and re-apply every predicate after latest-row resolution. Center candidate CTEs are only emitted when they include target-derived predicates; the compiler does not build a same-table center candidate that only repeats the center node's own filters.
- Row deduplication: `ReplacingMergeTree` does not guarantee merge-time dedup between queries, so the compiler injects query-time dedup (see [Row deduplication](#row-deduplication) below).

These choices preserve factorization: each hop operates on a compact frontier and prunes the next edge scan via semi‑joins, mirroring Kùzu’s accumulate → semijoin → probe execution.

### Row deduplication

Node and edge tables use `ReplacingMergeTree(_version, _deleted)`. Between background merges, queries can see stale row versions and soft-deleted rows. The ClickHouse compiler ensures query-time correctness for node table reads, mostly via `FINAL` (hydration arms instead dedup with `LIMIT 1 BY <sort_key>`, which preserves the same latest-non-deleted semantics while keeping column pruning and projections; see the Hydration row below):

| Scan type | Strategy | Rationale |
|---|---|---|
| Single-node traversal | Node table scan with `FINAL` | Applies `ReplacingMergeTree` latest-row semantics before filters and limits |
| Node filter CTEs | Node table scan with `FINAL` | Ensures ID frontiers are derived from latest rows, not stale matching versions |
| FK candidate CTEs | Non-`FINAL` `SELECT DISTINCT id` or FK values plus outer `FINAL` recheck | Lets ClickHouse use selective filters before the expensive latest-row scan while preserving correctness through the outer recheck |
| Edge narrowing CTEs | Non-`FINAL` `SELECT DISTINCT edge_id` frontier | Narrows joined node `FINAL` scans while avoiding duplicate-heavy `IN` sets from fan-out edges |
| Redaction joins for filtered non-default auth IDs | Filtered node table subquery with `FINAL` | Lets enforcement joins for entities such as code definitions apply property filters inside the latest-row read |
| Hydration (UNION ALL arms) | Non-`FINAL` scan with `LIMIT 1 BY <sort_key> ORDER BY <sort_key>, _version DESC`, outer `_deleted = false` | Hydration reads a tiny pinned `id IN (...)` set; dropping `FINAL` lets column pruning and projections apply (`FINAL` reconstructs full rows, defeating both). Dedup identity is the table's full sort key, matching `FINAL`'s per-ORDER-BY-key semantics. Falls back to `FINAL` when a table has no sort key. |
| Main query node scans | Node table scan with `FINAL` | Keeps traversal, FK, aggregation, and single-node lookup semantics consistent |
| Edge scans | `_deleted = false` in WHERE | Full-tuple ORDER BY makes RMT merge effective; only soft-delete filtering needed |

Filter placement rules for node `FINAL` scans:

- **Structural filters** (`traversal_path`, `id`, `project_id`, `branch`) are emitted on the `FINAL` scan so ClickHouse can still use primary-key pruning where supported.
- **Mutable filters** (`state`, `status`, `draft`) also evaluate against the `FINAL` scan, preventing stale row versions from matching.
- **`_deleted = false`** is always applied after latest-row resolution, either on the `FINAL` scan or outside a wrapping subquery.
- **Candidate CTEs** are allowed to over-select because they are only a performance prefilter. The outer `FINAL` scan always re-applies the filters and `_deleted = false` before rows can affect traversal or aggregation results.
- **Pinned FK target IDs** are pushed into the FK center `FINAL` subquery when the FK column lives on the center table.

Edge-only traversals do not join node tables for non-group-by nodes, so they cannot filter out deleted nodes at the query layer. In production this is handled by the SDLC indexer, which soft-deletes FK edge rows in the same ETL batch as their parent node (`crates/indexer/src/modules/sdlc/pipeline.rs`). Cross-entity FK cleanup relies on PostgreSQL's referential integrity propagating through Siphon CDC.

### Denormalized joins

A denormalized join pre-joins a linear chain of tables into one `gl_denorm_<name>` table so the compiler can answer the matching hops with a single scan. It is declared as a chain of edge variants, each realized either through its edge table or, with `via: fk`, directly node to node on the variant's FK column:

```yaml
denormalized_joins:
  - name: reviewer_project
    hops:
      - {relationship: REVIEWER,   from: User,         to: MergeRequest}
      - {relationship: IN_PROJECT, from: MergeRequest, to: Project, via: fk}
```

That resolves to the table chain `gl_user, gl_edge, gl_merge_request, gl_project`. Adjacent tables join on the id or edge id that links them, and every scoped table keeps its own `traversal_path` in the row, exactly as each scan alias keeps its own in an ordinary join. Every other column of every table is copied under a `t{i}_` prefix. The first scoped table's `traversal_path` is the row's unprefixed one and leads the sort key. The DDL generator composes the table from the source tables' already-generated definitions (columns, codecs, indexes, settings, partitioning) and emits one `TO` materialized view per table, joined outward from the trigger with `FINAL`. The security pass filters each path column in the row against the authorized set (see `Ontology::traversal_path_columns`), each at its own table's role floor, so a hop may cross namespaces just as it may in an edge chain: a row is returned only when the caller is authorized for every namespace it touches. The loader only requires that at least one table in the chain is scoped.

Before declaring a join in `schema.yaml`, trial it as an ontology overlay under `config/seeds/overlays/<name>/` (a directory mirroring `config/ontology/`, merged over it) and run the data correctness suite against it with `mise test:integration:overlay <name>`. The suite creates the table and its views from the seed, checks the table holds exactly the rows the live source join produces, and compiles every query it knows against the overlaid ontology.

### Scope rewrite (traversal_path prefix injection)

Project- and group-scoped queries (`traversal` and `aggregation`) are rewritten to add a tight `startsWith(traversal_path, '<prefix>')` predicate, so the leading primary-key segment prunes the scan rather than a structural-column filter alone. A node pins a scope when it carries a single `id`/`full_path`/`node_ids` for an anchor entity, **or** a single equality filter on a `namespace_anchor` FK column (e.g. `project_id`/`group_id`) — the anchor and its FK columns are read from the ontology's per-property `traversal_path_lookup` declarations and edge scope annotations via `Ontology::is_anchor` / `Ontology::anchor_fk_mappings`, not a hardcoded list. Anchors are `Project` and `Group` (resolved through a ClickHouse `CACHE` dictionary over `gl_project`/`gl_group`), plus `MergeRequest` and the code entities `Definition`/`File`/`Directory`, which have no dictionary and resolve through an `argMax(traversal_path, _version)` lookup on their own table by `id` (`PathResolver`, backed by a short-lived in-process cache; see `crates/orbit-server/src/pipeline/path_resolver.rs`). The code-entity lookups let code-intelligence "find callers/references/callees" traversals — anchored on a single `Definition` node id rather than a project filter — scope to the symbol's own project. A resolution failure — a dictionary miss for a not-yet-indexed id, or the `'0/'` sentinel — yields no injection, so the query falls back to the plain filter.

**Propagation to reachable edges and payload nodes.** Edge variants are annotated in the ontology YAML with a `scope` (`namespace_anchor`, `same_namespace`, or omitted = cross-namespace; see the scope-annotation MR). Because an edge row's `traversal_path` is its source entity's, and a scope-preserving edge keeps both endpoints in one namespace subtree, a resolved prefix floods across scope-preserving relationships to every reachable node and edge via `Ontology::propagate_scope_prefixes` — a two-pass taint walk that resolves the *exact* variant (`is_scope_preserving_triple`, so mixed-variant edges like `CONTAINS` are handled correctly) and refuses to enter any alias reachable through a cross-namespace edge. The compiler maps each `InputRelationship` into an `ontology::ScopeEdge` (`scope::scope_edges`) for the walk. The webserver attaches the flooded node prefixes to `SecurityContext.scope_prefixes` so their node-table scans inherit the prefix; the compiler's `restrict` pass stamps each edge whose endpoints share a prefix, and the lowerer emits the `startsWith` on the edge scan. Cross-namespace relationships (e.g. `CLOSES` an issue in another project) do not propagate, so multi-edge traversals stay correct — an unannotated relationship confines the prefix conservatively rather than over-pruning. This is what makes a 2+ edge project-scoped traversal seek the project's PK range instead of scanning the org-wide edge table (the cause of the #601941 timeout).

The prefix is validated within authorized scope before use: the path resolver only attaches it when it is a descendant of one of the caller's authorized traversal paths (`is_descendant`). For a node-table scan whose prefix is also within the entity's role-eligible paths, the `SecurityPass` injects it **as that alias's authorization filter**, in place of the broad per-namespace `startsWith` set — the tight prefix already confines the scan to authorized rows, so the broad set is redundant (and its long OR-chain is what made the unscoped scan slow). Below the entity's role floor the role-filtered broad set is kept instead (possibly `Bool(false)`). Either way it only narrows within already-authorized scope; it never widens access.

**Bounded staleness on namespace moves.** The prefix is resolved from a cache (dictionary `LIFETIME` plus the in-process TTL) over `gl_project`/`gl_group`, which the graph itself derives from PostgreSQL via CDC and re-indexing. When a project or group is transferred, its rows are re-stamped with the new `traversal_path`, but the cache can briefly keep resolving the pre-transfer prefix. During that window a scoped query can under-prune — return fewer rows than it should — because the stale `startsWith` no longer matches the re-stamped rows. The window self-heals once the cache refreshes; it only ever under-prunes (the surviving `id`/`full_path` filter and the authorization prefix mean it never returns extra or cross-tenant rows); and `is_descendant` limits exposure to callers already authorized over both the old and new locations. It is a performance optimization layered on the graph's existing eventual consistency, not a new correctness or security boundary.

## Request Flow (Deployed)

1. Client (MCP or REST) submits a tool call or Cypher.
2. Adapter validates/normalizes input pursuant to the currently deployed schema, computes the user's `traversal_path` prefixes (which encode the organization ID as their first segment) for SDLC queries, and selects the active `branch` for code queries.
3. Planner compiles to ClickHouse SQL (CTEs, recursive CTEs, unions, joins) with bound parameters.
4. ClickHouse executes; the server returns rows plus the generated SQL for audit.

### Unified Response Format

The server fetches one probe row beyond the requested window, trims it, and derives honest pagination metadata (`has_more`, `truncated`, `next_cursor`). Keyset cursors (`{ page_size, after }`) lower into seek predicates in SQL, so each page is a fresh bounded query; there is no offset slicing and no cross-page result cache. The formatting stage then transforms the trimmed `QueryResult` into the output payload. [ADR 004](../decisions/004_unified_response_schema.md) defines the format: a unified `{ format_version, query_type, nodes, edges, columns?, group_columns?, rows?, pagination? }` shape for all four query types (traversal, aggregation, path_finding, neighbors) with deduplicated nodes and instance-level edges. `format_version` (semver) lets consumers detect breaking changes.
Aggregation queries include `columns`, `group_columns`, and `rows` for table-shaped analytics output.
A `GraphFormatter` handles the transformation, and a JSON Schema defines the response contract between server and frontend.

Namespace graph updates arrive via an ETL worker, described in [SDLC Indexing](../indexing/sdlc_indexing.md). The indexer publishes a small state record (namespace → active state). The web tier caches namespace metadata and injects appropriate filters into queries; no file swapping is required.

Direct projections and hydration apply ontology-derived [text excerpts](../../source/remote/queries/query-language.md#text-excerpts) in SQL before serialization, without changing filters, joins, authorization, grouping, sorting, or cursor keys.

## Authorization and Safety

- Hard filters in SQL: every query carries `startsWith(traversal_path, ?)` predicates scoped to the caller's authorized namespaces (organization isolation is implicit — the org ID is the first path segment).
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

The indexer writes denormalized, typed node and edge tables in ClickHouse via ETL rather than synchronous materialized views. The exact mechanisms for this are covered in [SDLC Indexing](../indexing/sdlc_indexing.md) and [Schema Management](../schema_management.md). Materialized views would require filtered license checks on every inserted row, reducing ingestion efficiency. ETL decouples transformation from ingestion, allowing the indexer to batch writes and maintain control over schema evolution without impacting ClickHouse insert performance. Materialized views are reserved for precomputing stable summaries (e.g., group closure) that change infrequently and do not require per-row filtering, but these are optional enhancements for performance and may be subject to change.
Edge lookups use the ontology-declared sort keys, primary keys, and bloom filter indexes rather than per-table projections.

## Unified Security and Performance Testing

Security testing and performance testing share the same underlying techniques for the query engine. We treat them as a single validation effort:

- **Fuzzing**: Automated generation of malformed, edge-case, and adversarial inputs to the JSON tool interface and (optionally) Cypher parser. The same fuzzer that finds performance regressions (e.g., queries that blow up in time or memory) will also surface authorization bypass attempts (e.g., queries missing required predicates).
- **Automated Query Generation**: Property-based testing that generates random valid query shapes and verifies:
  - All generated SQL includes `startsWith(traversal_path, ?)` predicates (security invariant).
  - Query execution time stays within bounds (performance invariant).
  - Result sets respect authorization constraints (correctness invariant).
- **Automated Penetration Testing**: Scripted scenarios that attempt common bypass techniques (SQL injection, predicate stripping, cross-tenant access). These run as part of CI and are informed by the threat model.

This unified approach ensures that security and performance are validated together—an authorization check that slows queries unacceptably is as much a bug as one that fails to block unauthorized access. Results from fuzzing and automated testing feed back into both the threat model and the grammar-based validation described in [Authorization and Safety](#authorization-and-safety).

In addition to the above, a formal threat model is being developed for the query engine. This will be tracked as an epic under the broader GKGaaS effort, with specific issues for high-risk components such as the query planner and JSON-to-SQL transformation pipeline. For the full authorization model (tenant segregation, traversal path filtering, JWT verification, and final redaction), see [Security Architecture](../security.md).

## References

- graphhouse experiments and benchmarks (multi‑table schema, adjacency ordering, recursive CTEs)
- ClickGraph engine (Cypher → ClickHouse SQL, recursive CTEs, path functions, Bolt/HTTP front ends)
