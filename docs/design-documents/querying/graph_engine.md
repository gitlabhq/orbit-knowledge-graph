# Graph engine

## Overview

We will use ClickHouse as the primary database for graph queries in deployed environments. The query tier compiles high‑level graph operations into ClickHouse SQL and executes them directly on adjacency‑ordered edge tables and typed node tables. This pairs OLAP throughput with property‑graph semantics (paths, traversals, pattern matching) without introducing another datastore.

This plan draws on the [team's research](https://gitlab.com/gitlab-org/rust/knowledge-graph/-/issues/267). The approach keeps the service stateless and focuses on schema design, SQL generation, and guardrails.

## Storage Model in ClickHouse

The storage model aims to replicate the CSR (Compressed Sparse Row) adjacency list index concepts from [KuzuDB’s whitepaper](https://www.cidrdb.org/cidr2023/papers/p48-jin.pdf).

- Nodes by type: separate ReplacingMergeTree tables per entity (e.g., `groups`, `projects`, `issues`, `merge_requests`, `files`, `symbols`).
  - Primary key and ORDER BY include `organization_id`, then `traversal_path` (for SDLC entities) or `branch` (for code node types), followed by the local identifier.
  - SDLC example: `(organization_id, traversal_path, issue_id)`
  - Code example: `(organization_id, branch, symbol_id)`

- Edges by relationship: separate MergeTree tables per relationship (e.g., `has_subgroup`, `has_project`, `mr_closes_issue`, `imports`, `calls`).
  - ORDER BY `(organization_id, traversal_path, src_id, dst_id)` for SDLC edges or `(organization_id, branch, src_id, dst_id)` for code edges to create contiguous adjacency lists per source. This is the on-disk adjacency index the query engine exploits for fast neighbor scans.
  - Optional projection per edge table ordered by destination for reverse traversals: `ORDER BY (organization_id, traversal_path, dst_id, src_id)` or `ORDER BY (organization_id, branch, dst_id, src_id)`.
  - Branched edges for code relationships; current-state edges for SDLC hierarchy unless historical analysis is required.

- `traversal_path`: SDLC entities (issues, merge requests, pipelines, etc.) are enriched with a `traversal_path` column during indexing. This slash-delimited string encodes the full ancestor hierarchy of the entity's parent namespace (e.g., `"42/100/1000/"` for a project inside a subgroup inside a top-level group, where `42` is the organization ID). The query engine uses `traversal_path` for prefix-based permission filtering via `startsWith` predicates, enabling efficient authorization checks without joining back to the namespace hierarchy. See [Security Architecture](../security.md) for details on how `traversal_path` filters are injected.

- `branch`: Code node types (`files`, `symbols`, `definitions`) include a `branch` column to track which Git branch the indexed code belongs to. SDLC entities do not use this column as it is not relevant to their current state. **Note**: we intend to only index the **current** state of a particular branch in the initial iteration, not the historical state. See [Code Indexing](../indexing/code_indexing.md) for more details.

- Multi‑tenancy and authorization are enforced in every query by prefix filtering on `organization_id` and `traversal_path` to keep scans local and permission-scoped.

### Edge table schema

Edge tables are declared in `settings.edge_tables` in `schema.yaml` (default: `gl_edge`). Each edge YAML can set `table:` to route that relationship type to a specific table. All edge tables share the same column schema and use an adjacency-optimized primary key:

```sql
PRIMARY KEY (source_id, relationship_kind, target_id)
ORDER BY (source_id, relationship_kind, target_id, traversal_path, source_kind, target_kind)
PROJECTION by_target (SELECT * ORDER BY (target_id, relationship_kind, target_kind, source_id, traversal_path))
```

The primary key serves as a forward adjacency index, giving O(log N) lookup for all
outgoing edges from a node. The `by_target` projection serves as the reverse adjacency
index for incoming edge lookups.

Sort key column order matters: `(id, relationship_kind, ...)` ensures ClickHouse can use
prefix-based primary index pruning for both the base table and projections. Placing
`source_kind`/`target_kind` before `relationship_kind` breaks prefix matching since
queries rarely filter on entity kind alone.

Bloom filter indexes on `source_id`/`target_id` are intentionally omitted. They compete
with projections in ClickHouse's cost optimizer: the optimizer counts granules and picks
the "cheaper" path, but bloom-filtered base table granules appear cheaper than projection
granules even though projection data is contiguous and bloom data is scattered. Removing
bloom filters lets projections be correctly selected.

## Query Engine Design

There are two intended ways to interact with the graph engine:

1. Intermediate JSON tools (MCP/HTTP): existing JSON schemas describe graph intents (`traversal`, `neighbors`, `path_finding`, `aggregation`). The server validates input and compiles to parameterized SQL.
2. Cypher reader (optional): Cypher to SQL translation à la ClickGraph for teams that prefer property‑graph syntax or need Neo4j driver compatibility.

### Compiler pass pipeline

The query compiler transforms a JSON DSL input into parameterized ClickHouse SQL through an ordered pipeline of passes. The canonical pass order is defined in `crates/query-engine/compiler/src/config.rs` (the `clickhouse` pipeline):

| # | Pass | Responsibility |
|---|---|---|
| 1 | `validate` | Schema and cross-reference validation of the JSON input against the ontology |
| 2 | `normalize` | Resolves entity names to table names, coerces filter types, and expands wildcard columns |
| 3 | `restrict` | Strips `admin_only` fields and validates user-supplied `traversal_path` filters against the JWT-granted scope ([Security](../security.md)) |
| 4 | `plan` | Translates validated input into a query plan (hop chain, join strategy, FK shape) |
| 5 | `lower` | Emits the SQL AST from the query plan (edge-chain-first, nodes lazy) |
| 6 | `enforce` | Injects ID and type columns required for redaction; builds the result context |
| 7 | `security` | Injects `startsWith(traversal_path, ?)` predicates on all node-table scans, with per-entity role scoping ([Security](../security.md)) |
| 8 | `check` | Verifies every node-table alias carries a valid `startsWith` predicate traceable to the `SecurityContext` ([Security](../security.md)) |
| 9 | `hydrate_plan` | Builds the hydration plan for fetching entity properties after the base query |
| 10 | `settings` | Resolves ClickHouse query-level settings (timeouts, memory limits, cache) for the query type |
| 11 | `codegen` | Serializes the AST into parameterized ClickHouse SQL |

The planner emits ClickHouse SQL similar to these patterns:

- One‑hop neighbors: equality filter on the edge table’s leading keys, `WHERE organization_id = ? AND branch = ? AND src_id IN (...)` (for code) or `WHERE organization_id = ? AND startsWith(traversal_path, ?) AND src_id IN (...)` (for SDLC), producing O(degree) scans per source.
- Multi‑hop fixed depth (2–3): chained JOINs/CTEs with DISTINCT frontiers between hops to avoid blow‑ups.
- Variable‑length paths: `WITH RECURSIVE` over the edge table(s) with a depth limit and optional accumulation of `nodes(path)` and `relationships(path)` as arrays.
- Reverse hops: use the destination‑ordered projection or a reversed view produced on the fly.
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

### Scope rewrite (traversal_path prefix injection)

Project- and group-scoped queries (`traversal` and `aggregation`) are rewritten to add a tight `startsWith(traversal_path, '<prefix>')` predicate, so the leading primary-key segment prunes the scan rather than a structural-column filter alone. A node pins a scope when it carries a single `id`/`full_path`/`node_ids` for `Project`/`Group`, **or** a single equality filter on a `namespace_anchor` FK column (e.g. `project_id`/`group_id`) — the anchor and its FK columns are read from the ontology's edge scope annotations via `Ontology::is_anchor` / `Ontology::anchor_fk_mappings`, not a hardcoded list. The prefix is the anchor entity's own `traversal_path`, resolved from its `id`/`full_path` through a ClickHouse `CACHE` dictionary over `gl_project`/`gl_group` (`PathResolver`, backed by a short-lived in-process cache; see `crates/gkg-server/src/pipeline/path_resolver.rs`). A resolution failure — a dictionary miss for a not-yet-indexed id, or the `'0/'` sentinel — yields no injection, so the query falls back to the plain filter.

**Propagation to reachable edges and payload nodes.** Edge variants are annotated in the ontology YAML with a `scope` (`namespace_anchor`, `same_namespace`, or omitted = cross-namespace; see the scope-annotation MR). Because an edge row's `traversal_path` is its source entity's, and a scope-preserving edge keeps both endpoints in one namespace subtree, a resolved prefix floods across scope-preserving relationships to every reachable node and edge via `Ontology::propagate_scope_prefixes` — a two-pass taint walk that resolves the *exact* variant (`is_scope_preserving_triple`, so mixed-variant edges like `CONTAINS` are handled correctly) and refuses to enter any alias reachable through a cross-namespace edge. The compiler maps each `InputRelationship` into an `ontology::ScopeEdge` (`scope::scope_edges`) for the walk. The webserver attaches the flooded node prefixes to `SecurityContext.scope_prefixes` so their node-table scans inherit the prefix; the compiler's `restrict` pass stamps each edge whose endpoints share a prefix, and the lowerer emits the `startsWith` on the edge scan. Cross-namespace relationships (e.g. `CLOSES` an issue in another project) do not propagate, so multi-edge traversals stay correct — an unannotated relationship confines the prefix conservatively rather than over-pruning. This is what makes a 2+ edge project-scoped traversal seek the project's PK range instead of scanning the org-wide edge table (the cause of the #601941 timeout).

The injected prefix is re-validated before use: it is only applied when it is a descendant of one of the caller's authorized traversal paths (`is_descendant`), and it is ANDed with the existing filters. It can therefore only narrow within already-authorized scope; it never widens access or replaces the authorization prefix.

**Bounded staleness on namespace moves.** The prefix is resolved from a cache (dictionary `LIFETIME` plus the in-process TTL) over `gl_project`/`gl_group`, which the graph itself derives from PostgreSQL via CDC and re-indexing. When a project or group is transferred, its rows are re-stamped with the new `traversal_path`, but the cache can briefly keep resolving the pre-transfer prefix. During that window a scoped query can under-prune — return fewer rows than it should — because the stale `startsWith` no longer matches the re-stamped rows. The window self-heals once the cache refreshes; it only ever under-prunes (the surviving `id`/`full_path` filter and the authorization prefix mean it never returns extra or cross-tenant rows); and `is_descendant` limits exposure to callers already authorized over both the old and new locations. It is a performance optimization layered on the graph's existing eventual consistency, not a new correctness or security boundary.

## Request Flow (Deployed)

1. Client (MCP or REST) submits a tool call or Cypher.
2. Adapter validates/normalizes input pursuant to the currently deployed schema, resolves `organization_id`, computes the user's `traversal_path` prefixes for SDLC queries, and selects the active `branch` for code queries.
3. Planner compiles to ClickHouse SQL (CTEs, recursive CTEs, unions, joins) with bound parameters.
4. ClickHouse executes; the server returns rows plus the generated SQL for audit.

### Unified Response Format

After ClickHouse returns rows and redaction completes, the server applies agent-driven cursor pagination (`{ offset, page_size }`) to slice the authorized result set. A query result cache (moka, 60s TTL) stores the full authorized result so subsequent pages skip ClickHouse, authorization, and redaction. The formatting stage then transforms the sliced `QueryResult` into the output payload. [ADR 004](../decisions/004_unified_response_schema.md) defines the format: a unified `{ format_version, query_type, nodes, edges, columns?, group_columns?, rows?, pagination? }` shape for all four query types (traversal, aggregation, path_finding, neighbors) with deduplicated nodes and instance-level edges. `format_version` (semver) lets consumers detect breaking changes.
Aggregation queries include `columns`, `group_columns`, and `rows` for table-shaped analytics output.
A `GraphFormatter` handles the transformation, and a JSON Schema defines the response contract between server and frontend.

Namespace graph updates arrive via an ETL worker, described in [SDLC Indexing](../indexing/sdlc_indexing.md). The indexer publishes a small state record (namespace → active state). The web tier caches namespace metadata and injects appropriate filters into queries; no file swapping is required.

## Authorization and Safety

- Hard filters in SQL: every query carries `organization_id` and an allow‑list of resource scopes (e.g., pre‑authorized projects).
- Redaction layer: final pass to drop rows the upstream filters could not precisely exclude (e.g., confidential flags). Avoid redaction for aggregates; either pre‑filter or block the query shape.
- All queries will be parameterized.
- Depth caps and relationship allow‑lists to prevent runaway traversals; row and time limits per request.
- Grammar‑based query validation: the planner validates generated SQL against a strict subset of the ClickHouse grammar. The validator walks the AST to verify that required predicates (e.g., `organization_id`, traversal path filters) are present in `WHERE` clauses and rejects queries that omit them. This "fail closed" approach ensures malformed or overly broad queries are blocked before execution rather than relying solely on downstream filtering.

## Observability

- Per-phase timings (parse/plan/render/execute) and row counts.
- Emitted SQL and parameter map for debugging.
- Per-query ClickHouse resource stats (`read_rows`, `read_bytes`, `memory_usage`) extracted from the `X-ClickHouse-Summary` response header on every query. When profiling is enabled, these are enriched from `system.query_log`.
- Query result cache metrics: lookups (hit/miss/error), stores (success/error/too_large), evictions (per_user_limit).

## Integration with Indexing

The indexer writes denormalized, typed node and edge tables in ClickHouse via ETL rather than synchronous materialized views. The exact mechanisms for this are covered in [SDLC Indexing](../indexing/sdlc_indexing.md) and [Schema Management](../schema_management.md). Materialized views would require filtered license checks on every inserted row, reducing ingestion efficiency. ETL decouples transformation from ingestion, allowing the indexer to batch writes and maintain control over schema evolution without impacting ClickHouse insert performance. Materialized views are reserved for precomputing stable summaries (e.g., group closure) that change infrequently and do not require per-row filtering, but these are optional enhancements for performance and may be subject to change.
For reverse access paths, projections are built per table.

## Unified Security and Performance Testing

Security testing and performance testing share the same underlying techniques for the query engine. We treat them as a single validation effort:

- **Fuzzing**: Automated generation of malformed, edge-case, and adversarial inputs to the JSON tool interface and (optionally) Cypher parser. The same fuzzer that finds performance regressions (e.g., queries that blow up in time or memory) will also surface authorization bypass attempts (e.g., queries missing required predicates).
- **Automated Query Generation**: Property-based testing that generates random valid query shapes and verifies:
  - All generated SQL includes `organization_id` and traversal path predicates (security invariant).
  - Query execution time stays within bounds (performance invariant).
  - Result sets respect authorization constraints (correctness invariant).
- **Automated Penetration Testing**: Scripted scenarios that attempt common bypass techniques (SQL injection, predicate stripping, cross-tenant access). These run as part of CI and are informed by the threat model.

This unified approach ensures that security and performance are validated together—an authorization check that slows queries unacceptably is as much a bug as one that fails to block unauthorized access. Results from fuzzing and automated testing feed back into both the threat model and the grammar-based validation described in [Authorization and Safety](#authorization-and-safety).

In addition to the above, a formal threat model is being developed for the query engine. This will be tracked as an epic under the broader GKGaaS effort, with specific issues for high-risk components such as the query planner and JSON-to-SQL transformation pipeline. For the full authorization model (tenant segregation, traversal path filtering, JWT verification, and final redaction), see [Security Architecture](../security.md).

## References

- graphhouse experiments and benchmarks (multi‑table schema, adjacency ordering, recursive CTEs)
- ClickGraph engine (Cypher → ClickHouse SQL, recursive CTEs, path functions, Bolt/HTTP front ends)
