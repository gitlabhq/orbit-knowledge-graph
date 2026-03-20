# ClickHouse Optimization Findings (2026-03-20)

Supplement to [clickhouse_optimization.md](clickhouse_optimization.md).

This memo synthesizes:

- The prior deep-research log at `/Users/angelo.rivera/.claude/projects/-Users-angelo-rivera-gitlab-orbit-knowledge-graph/6fee4b11-5f62-4efe-8edc-b1664638927f.jsonl`
- GKG schema, compiler, ontology, and indexing code
- ClickHouse source code and official documentation
- Local `clickhouse-local` experiments against reduced schemas
- Parallel reviews focused on join shape, workload reality, write-path
  feasibility, and ClickHouse planner/storage behavior
- A focused second pass over ClickHouse JOIN, query-optimization, and
  sparse-primary-index guidance

## Executive Summary

- The dominant GKG workload is not arbitrary graph traversal. The benchmark corpus in [sdlc_queries.yaml](../../fixtures/queries/sdlc_queries.yaml) is mostly one-hop, owner-scoped, filter-heavy, top-N analytics.
- The biggest remaining cost is not join algorithm choice. It is the generic `node -> gl_edge -> node` lowering path for queries whose relationship key often already exists on the node row.
- `gl_edge` still needs a real reverse access path, but the current base sort order is not fundamentally wrong for outgoing traversals. The current `by_target` projection is the real defect.
- A blanket move to node tables with base `ORDER BY id` is not yet justified. In local experiments, this helped join locality but could also lose `traversal_path` pruning on secured joins when ClickHouse chose the base table instead of a traversal-path projection.
- The ClickHouse guidance in the JOIN, query-optimization, and sparse-primary-index docs all points in the same direction: optimize schema and physical access paths before spending more time on join-algorithm tuning.
- Intermediary tables can help, but only selectively. Small hot-path bridge or fact tables are viable. Splitting the entire graph into many physical relationship tables is not.

## Workload Reality

The benchmark corpus in [sdlc_queries.yaml](../../fixtures/queries/sdlc_queries.yaml) is heavily skewed toward:

- One-hop traversals
- Security-scoped filters on `traversal_path`
- Additional filters on `state`, `status`, `severity`, and similar columns
- Top-N sorts on one timestamp or metric column
- Frequent `User` and `Project` lookups

It is not dominated by:

- Deep recursive traversal
- Path-finding
- Bidirectional neighborhood expansion
- Large fan-out multi-hop graph algorithms

The validator also caps both `max_hops` and `max_depth` at 3 in [validate.rs](../../crates/query-engine/compiler/src/validate.rs#L154), which further limits the practical value of investing first in multi-hop physical design.

## How ClickHouse Guidance Maps to GKG

The ClickHouse guidance from these docs is directly relevant:

- https://clickhouse.com/docs/best-practices/minimize-optimize-joins
- https://clickhouse.com/docs/optimize/query-optimization
- https://clickhouse.com/docs/guides/best-practices/sparse-primary-indexes

### JOIN guidance

The JOIN best-practice guidance reinforces four points that matter for GKG:

1. Denormalize when the access path is stable and latency matters.
2. Prefer direct key-value style joins for dimension tables where possible.
3. Reduce the size of the right-hand side before the join.
4. Treat materialized views or intermediary physical tables as valid tools when they shift repeated query work into ingest time.

For GKG, that means:

- `AUTHORED`, `MERGED_BY`, `TRIGGERED`, `IN_MILESTONE`, and `IN_GROUP` should be challenged first as denormalized foreign-key lookups, not as mandatory `gl_edge` traversals.
- `gl_user` is the best candidate for a direct-join optimization path because it already uses `ORDER BY (id)` in [graph.sql](../../config/graph.sql#L27) and is not traversal-path filtered like the other node tables.
- Dedicated intermediary tables are justified only when they remove repeated many-to-many expansion that cannot be handled by a scalar foreign key.

Local ClickHouse references for these points:

- `/Users/angelo.rivera/developer/ClickHouse/docs/en/sql-reference/statements/select/join.md:23`
- `/Users/angelo.rivera/developer/ClickHouse/docs/en/sql-reference/statements/select/join.md:471-479`
- `/Users/angelo.rivera/developer/ClickHouse/docs/en/engines/table-engines/special/join.md:50-61`
- `/Users/angelo.rivera/developer/ClickHouse/docs/en/sql-reference/statements/create/dictionary/layouts/direct.md:15-19`

### Query-optimization workflow guidance

The query-optimization guidance reinforces that tuning should follow a fixed order:

1. Observe real queries first.
2. Use `EXPLAIN` and query logs to identify where time and reads go.
3. Optimize schema and sort order before query-level micro-tuning.
4. Benchmark with disciplined cache handling and one change at a time.

For GKG, this means the benchmark loop should emphasize:

- Representative secured queries from [sdlc_queries.yaml](../../fixtures/queries/sdlc_queries.yaml)
- `EXPLAIN indexes = 1`
- `system.query_log` metrics for read rows, read bytes, and memory
- Query-condition-cache-disabled validation runs when comparing index usage
- Cold-cache and warm-cache comparisons
- Filesystem-cache-aware benchmarking discipline

Local ClickHouse references for this workflow:

- `/Users/angelo.rivera/developer/ClickHouse/docs/en/operations/system-tables/query_log.md:16-23`
- `/Users/angelo.rivera/developer/ClickHouse/docs/en/operations/system-tables/query_log.md:54-72`
- `/Users/angelo.rivera/developer/ClickHouse/docs/en/sql-reference/statements/explain.md:187-188`

### Sparse-primary-index guidance

The sparse-primary-index guidance matters because ClickHouse does not index every row. It indexes marks for granules, and the sort key determines how well filters can prune those granules.

For GKG, that means:

- `ORDER BY (traversal_path, id)` is good when the dominant access pattern is namespace pruning plus point-id lookup.
- It is not automatically good for `state/status/severity + time` workloads if those columns are not co-located in the sort order.
- If GKG truly needs two incompatible access patterns, ClickHouse guidance supports three tools: projections, materialized/secondary tables, or a second full table layout.
- Projections are the most transparent option, but a second physical table is sometimes the more deterministic option when the access path must be explicit.

The key sparse-index implication is that these options are all alternate
on-disk layouts, not merely logical accelerators. The official primary-index
guide is explicit that a second table, a materialized view, and a projection
all duplicate the data in order to change row order and primary-key behavior.

## Main Physical Design Finding

The biggest structural mismatch is not that `gl_edge` is keyed on `source_id`. It is that GKG still routes many one-hop relationship lookups through `gl_edge` even when the source row already carries the relevant foreign key.

Examples that should be challenged as generic edge traversals:

- `IN_PROJECT`
- `AUTHORED`
- `MERGED_BY`
- `TRIGGERED`
- `IN_MILESTONE`
- `IN_GROUP`
- `HAS_NOTE`

For these paths, the higher-value move is usually one of:

1. Add the scalar key directly to the node table and filter or join from there.
2. Defer wide payload access and hydrate later.
3. Use a narrow specialized bridge table only for the hottest many-to-many relationships.

## `gl_edge` Verdict

The current `gl_edge` layout in [graph.sql](../../config/graph.sql#L216) is still aligned with the default outgoing traversal direction used by the compiler in [input.rs](../../crates/query-engine/compiler/src/input.rs#L293).

The real defect is the reverse projection:

- The existing `by_target` projection omits `traversal_path`
- It does not use `SELECT *`
- It is therefore misaligned with the security filter injected by [security.rs](../../crates/query-engine/compiler/src/security.rs)

The best immediate DDL change is to replace it with a full-row reverse projection ordered like:

```sql
ALTER TABLE gl_edge DROP PROJECTION by_target;

ALTER TABLE gl_edge ADD PROJECTION reverse_lookup
(
    SELECT *
    ORDER BY
    (
        traversal_path,
        target_id,
        target_kind,
        relationship_kind,
        source_id,
        source_kind
    )
);

ALTER TABLE gl_edge MATERIALIZE PROJECTION reverse_lookup;
```

This preserves security pruning and fixes the missing incoming-edge access path without changing the compiler.

## Node Layout Verdict

The proposal to change SDLC node tables from `ORDER BY (traversal_path, id)` to base `ORDER BY id` plus a traversal-path projection is plausible, but it is not yet proven safe.

Local `clickhouse-local` experiments showed:

- Current node order preserves primary-key pruning for `startsWith(traversal_path, ...)`
- `id`-first order can reduce sort work before merge join
- Filter-only queries can use a traversal-path projection on an `id`-ordered base table
- Security-filtered JOIN queries did not reliably route through that projection in the tested shape
- In the JOIN case, ClickHouse read the base `id`-ordered table and lost traversal-path pruning

Conclusion:

- Do not promote `ORDER BY id` as the default node layout yet
- Treat it as a benchmark candidate for selected hot tables only
- Keep the security-vs-join-locality tradeoff explicit

## Sparse Primary Index and On-Disk Implications

The local MergeTree docs sharpen what a better layout means physically:

- `ORDER BY` is a clustering and read-pruning decision, not an OLTP-style
  uniqueness rule. MergeTree stores one primary-key mark per granule, not one
  index entry per row.
- Sparse indexes still overread. A selected key range can stream extra rows
  around the matching granules, so the key matters only when it lets
  ClickHouse skip long runs of marks.
- Each MergeTree part has one base lexicographic order. If one workload needs a
  different physical order, the real choices are another on-disk layout via a
  projection, another logical table, or a materialized view.
- Sort order affects both pruning and compression, so leading columns should be
  chosen for repeated filter patterns, not just join convenience.

Applied to GKG:

- `traversal_path` is doing real physical work, not just logical auth work.
  The compiler injects `startsWith(traversal_path, ...)` on secured scans, so a
  `traversal_path`-first base order is aligned with the one predicate almost
  every secured query has.
- `state`, `status`, `severity`, and hot time columns are underrepresented in
  the current SDLC physical orderings, which is why many hot queries still scan
  too much within a namespace even after auth pruning.
- The `traversal_path`-first versus `id`-first debate should be treated as
  base-layout versus alternate-layout, not as one universally correct key.
  Secured base tables should stay `traversal_path`-first unless benchmarks show
  otherwise for a specific family. `id`-first is a better fit for access paths
  like `gl_user` or selected projections, not for every namespaced table.
- If GKG needs another access path, ClickHouse's own sparse-primary-index
  guidance points to projections or secondary tables. Those are not planner
  tricks. They are real on-disk duplicate layouts with their own pruning
  behavior and storage cost.

This is also why the code tables in [graph.sql](../../config/graph.sql#L414)
already look like a useful precedent: they keep a security- and owner-aware
base order while exposing `id_lookup` projections for a second access path.

Version caveat:

- Projections are skipped for `FINAL` queries and `readsInOrder()` plans in
  current ClickHouse. That makes them useful as alternate filtering layouts,
  but not a universal replacement for every read pattern.

Local ClickHouse references for these storage claims:

- `/Users/angelo.rivera/developer/ClickHouse/docs/en/engines/table-engines/mergetree-family/mergetree.md:615-637`
- `https://clickhouse.com/docs/guides/best-practices/sparse-primary-indexes`

## Join Strategy Finding

The current optimization thread overweights merge-join ideas.

Based on ClickHouse source and local experiments:

- `full_sorting_merge` is not the main opportunity for GKG
- GKG SQL shape often includes conditions that make merge-join wins less compelling
- Current workload benefits more from avoiding joins than from picking a different join algorithm
- For spill-safe joins, `grace_hash` is more interesting than forcing merge joins
- Runtime join filters are now relevant and default-on in modern ClickHouse, which makes hash-family joins more attractive than the current memo suggests

The JOIN best-practice guidance adds a few concrete implications:

- If a join does not need all right-hand matches, `ANY` joins are preferable to
  a full Cartesian expansion.
- Filters should be pushed before the join, and if the planner does not do that
  well enough, rewriting one side as a subquery is a valid forcing function.
- Direct joins should be reserved for true key-value right-hand sides, which is
  why `gl_user` is a better fit than most namespaced node tables.
- Sort-merge joins are interesting only when both sides are already sorted on
  the actual join key. That is not the dominant GKG shape today.
- ClickHouse itself recommends keeping joins to a minimum and benchmarking real
  data instead of assuming one algorithm will dominate.

For `gl_user`, the dictionary/direct-join path still looks good because:

- `gl_user` is not gated by traversal-path security in the same way as other tables
- Its key shape matches direct key-value lookup more naturally than most node tables

## How Official ClickHouse Guidance Maps to GKG

Across the official ClickHouse docs, the priority order is:

1. Physical layout and denormalization first
2. Alternate layouts second
3. Join-algorithm tuning last

That maps cleanly onto GKG:

- `minimize-optimize-joins` supports denormalizing stable, latency-sensitive
  relationship lookups. For GKG that means FK-like traversals such as
  `IN_PROJECT`, `AUTHORED`, `MERGED_BY`, `TRIGGERED`, `IN_MILESTONE`, and
  `IN_GROUP` should usually move toward node-column lookup paths, not more
  elaborate generic JOIN plans.
- `query-optimization` supports an optimization loop built around
  `system.query_log`, `EXPLAIN`, and schema validation on real query shapes. It
  does not support treating join-algorithm changes as the default first move.
- `sparse-primary-indexes` supports multiple physical layouts when one ordering
  cannot serve all hot queries. For GKG, that means projections or a very small
  number of narrow intermediary tables, not a broad edge-table fan-out.

Decision rules:

- Scalar FK already on source row: use a denormalized column or direct lookup.
- Same rows but different hot filter/sort path: use a projection.
- Repeated hot many-to-many expansion: use a narrow bridge table.
- Derived or pre-aggregated semantics: use a materialized view.

Not first-priority per the official docs:

- Generic merge-join tuning
- Assuming one primary key can simultaneously optimize auth pruning and every
  `state/status/severity + time` top-N read
- Broad physical splitting of `gl_edge` by relationship kind

## ClickHouse Doc and Source Corrections

The current optimization memo should be read with these corrections in mind:

1. Projections are not supported under `FINAL` in current ClickHouse documentation and planner code.
2. `use_query_condition_cache` is default-on now, not opt-in.
3. Diagnostics should disable query condition cache when comparing index usage, otherwise `EXPLAIN indexes = 1` can be misleading.
4. MergeTree `direct` is much narrower than the settings text suggests and is not a generic replacement for the `gl_user` dictionary plan.
5. Projection indexes are a newer, lighter-weight option worth testing for incoming-edge access, but only after validating version support across environments.

Relevant sources:

- https://clickhouse.com/docs/best-practices/minimize-optimize-joins
- https://clickhouse.com/docs/optimize/query-optimization
- https://clickhouse.com/docs/guides/best-practices/sparse-primary-indexes

Local ClickHouse references used for this pass:

- `/Users/angelo.rivera/developer/ClickHouse/docs/en/sql-reference/statements/select/join.md:23`
- `/Users/angelo.rivera/developer/ClickHouse/docs/en/sql-reference/statements/select/join.md:471-479`
- `/Users/angelo.rivera/developer/ClickHouse/docs/en/sql-reference/statements/explain.md:187-188`
- `/Users/angelo.rivera/developer/ClickHouse/docs/en/operations/system-tables/query_log.md:16-23`
- `/Users/angelo.rivera/developer/ClickHouse/docs/en/operations/system-tables/query_log.md:54-72`
- `/Users/angelo.rivera/developer/ClickHouse/docs/en/engines/table-engines/mergetree-family/mergetree.md:615-637`
- `/Users/angelo.rivera/developer/ClickHouse/docs/en/engines/table-engines/special/join.md:50-61`
- `/Users/angelo.rivera/developer/ClickHouse/docs/en/sql-reference/statements/create/dictionary/layouts/direct.md:15-19`

## Optimization Workflow to Add

The findings doc should carry a stricter optimization loop than the current
memo:

1. Identify slow query shapes in `system.query_log`.
2. Group them by `normalized_query_hash` when the same query is executed with
   different literals.
3. Flush logs when needed with `SYSTEM FLUSH LOGS` before inspecting a test run.
4. Disable filesystem cache during troubleshooting runs to expose real I/O and
   schema costs.
5. Use `EXPLAIN indexes = 1, projections = 1` and `EXPLAIN PIPELINE` before
   changing schema.
6. Verify projection usage through `system.query_log.projections`.
7. Change one thing at a time and compare cold and warm behavior separately.
8. Use `clickhouse-benchmark` or an equivalent repeatable harness for A/B runs.

For GKG, this pushes benchmark discipline ahead of any broad `ORDER BY`
migration or large intermediary-table rollout.

## Intermediary Tables

Intermediary tables are worth considering, but only in narrow cases.

The sparse-primary-index guidance makes the options clearer:

### Option A: projection on the existing table

Best when:

- Read routing should stay transparent to the compiler
- The access path is "same rows, better order"
- Write-path simplicity is more important than explicit table control

Tradeoffs:

- Lowest application complexity
- Hidden physical table with its own primary index
- Not available under `FINAL`
- Does not make `ORDER BY` queries faster just because the projection has a
  matching order

### Option B: materialized-view-backed intermediary table

Best when:

- The alternate layout should be explicit and directly queryable
- Another primary ordering is needed for a stable hot query family
- The operational cost is acceptable

Tradeoffs:

- More explicit than projections
- More deletion and lifecycle complexity than projections
- Still adds write amplification

### Option C: explicit specialized table

Best when:

- The alternate layout is semantically different, not just another ordering
- The compiler can deliberately route exact relationship families to it
- The workload is hot enough to justify storage and code-path complexity

Tradeoffs:

- Highest control
- Highest compiler/indexer/deletion complexity
- Easiest place to overfit the schema

### What to build first

The schema shift that matters is not "more edge tables everywhere." It is:
stop paying generic graph-traversal cost for relationships that are really
scalar foreign keys or repeated bridge expansions.

### Decision rule

- Yes, GKG should create a small number of intermediary tables.
- No, GKG should not turn every hot edge into its own table.
- If the source node row already carries the relationship key, prefer a
  denormalized node column over a new table.
- If the relationship is many-to-many and repeatedly queried by the bridge
  key, build a narrow bridge table.
- If a whole domain has a stronger natural scope than `gl_edge` exposes, build
  a domain-specific edge table.

### Build first

- `User bridge family`: `gl_merge_request_user_bridge` for `ASSIGNED` and
  `REVIEWER`, plus `gl_work_item_user_bridge` for `ASSIGNED`. These target user
  work queues and reviewer load. One row per resolved pair, not arrays. Carry
  `traversal_path`, owner scope, `relationship_kind`, `user_id`, entity id, the
  smallest set of duplicated hot filter columns, `updated_at`, `_version`, and
  `_deleted`. Order by the lookup key first.
- `Label bridge family`: `gl_merge_request_label_bridge` and
  `gl_work_item_label_bridge` for `HAS_LABEL`. These target board-style and
  label-filtered workloads. One row per `(label_id, entity_id)` pair with
  owner scope and duplicated hot entity filters.
- `Code-edge family`: a dedicated `gl_code_edge` keyed by
  `(traversal_path, project_id, branch, source_id, source_kind,
  relationship_kind, target_id, target_kind)` plus a reverse projection. This
  is the best case for a specialized edge table because the code node tables
  are already project-and-branch scoped while shared `gl_edge` is not.

These candidates are supported by the current ontology and workload shape:

- Merge requests already expose `target_project_id`, `author_id`,
  `merge_user_id`, `milestone_id`, assignees, labels, and reviewers in
  [merge_request.yaml](../../config/ontology/nodes/code_review/merge_request.yaml#L121).
- Work items already expose `author_id`, `milestone_id`, `namespace_id`,
  assignee ids, and label ids in
  [work_item.yaml](../../config/ontology/nodes/plan/work_item.yaml#L119).
- Pipelines already expose `project_id`, `user_id`, and `merge_request_id` in
  [pipeline.yaml](../../config/ontology/nodes/ci/pipeline.yaml#L146).
- The code tables already use project-and-branch-aware physical ordering in
  [graph.sql](../../config/graph.sql#L414), which is exactly the physical scope
  the shared edge table cannot currently express.

### Physical rules

- Keep `traversal_path` first or near-first on secured intermediary tables.
  Rails-owned auth still injects `startsWith(traversal_path, ...)`, so demoting
  it too far throws away the one predicate every secured query shares.
- Duplicate only the columns needed to answer the hot workload without another
  large join. If the new table still needs the base node table for most filters
  and sort columns, it is not shifting enough work.
- Use `ReplacingMergeTree(_version, _deleted)` and one row per resolved bridge
  pair. Do not use array-valued adjacency rows.
- Prefer explicit ETL-built intermediary tables over incremental materialized
  views for exact edge semantics. Refreshable materialized views are acceptable
  for periodic summaries, not for exact mutable edge mirrors.

### Better as denormalized columns than new tables

- `IN_PROJECT`
- `AUTHORED`
- `MERGED_BY`
- `TRIGGERED`
- `IN_MILESTONE`
- `IN_GROUP`

These are already foreign-key-like relationships in the ETL definitions above. They are usually better handled as additive node columns, selective projections, or late dimension lookup than as standalone bridge tables.

### Bad candidates

- A full per-relationship table split for all edges
- Adjacency-list materializations with array-valued neighbors
- Physical tables for scalar relationships whose source row already contains the foreign key
- A general intermediary table for polymorphic `HAS_NOTE`
- Raw reverse-edge intermediary tables for generic `gl_edge`
- Any design that removes `traversal_path` from the leading access path for secured edge scans

### Why

- The ontology, compiler, indexer, and namespace-deletion flow currently assume one shared edge table
- Projections and additive columns fit the current model much better than duplicated physical write paths
- Full table-family splits would increase write amplification, cleanup complexity, and correctness risk
- ClickHouse treats alternate primary access paths as real storage structures,
  so intermediary tables should be justified by a hot query family, not by a
  planner hunch

### Current write-path and deletion constraints

These constraints are what make intermediary tables expensive:

- The ontology has one shared edge table and one shared edge sort key in [schema.yaml](../../config/ontology/schema.yaml#L12).
- SDLC ETL lowers all foreign-key edge extraction into the configured shared edge destination in [sdlc/plan/lower.rs](../../crates/indexer/src/modules/sdlc/plan/lower.rs#L49).
- Query lowering scans one logical `gl_edge` table through `edge_scan()` in [lower.rs](../../crates/query-engine/compiler/src/lower.rs#L723).
- Namespace deletion generates one delete statement per namespaced node table plus the shared edge table in [namespace_deletion/lower.rs](../../crates/indexer/src/modules/namespace_deletion/lower.rs#L22).
- Code stale-data cleanup also assumes one edge table in [stale_data_cleaner.rs](../../crates/indexer/src/modules/code/stale_data_cleaner.rs#L46).

That makes the tradeoff clear:

- Projections are low-friction because they preserve the current logical table model.
- Specialized intermediary tables are viable only for a small number of hot cases.
- A broad physical table fan-out would require compiler routing, indexer dual-write changes, namespace-deletion coverage, and stale-data-cleanup changes.

### Feasibility tiers

Safest path:

- Use projections on existing base tables.
- Best examples: `gl_edge.reverse_lookup` and hot filter-plus-sort projections
  on the most queried node tables.
- Main cost: merge and rebuild work, not application complexity.

Medium-risk path:

- Add one explicit ETL-managed intermediary base table for one proven hot query
  family.
- Best examples: one owner-aware `MergeRequest` or `Pipeline` fact table, or
  one narrow bridge family such as assignees or labels.
- Main cost: explicit query routing, namespace-deletion coverage, and any
  family-specific cleanup behavior.

High-risk path:

- Avoid synchronous MV-backed mutable graph layouts and broad duplicate forward
  and reverse edge tables as the main strategy.
- Main cost: correctness drift, extra write amplification, and changes across
  ontology assumptions, ETL lowering, compiler routing, and deletion coverage.

Complexity boundary:

- Stop when a proposed intermediary table duplicates a scalar relationship
  already present on the base node row.
- Stop when wildcard traversals would need unions across many intermediary
  tables instead of one generic fallback path.
- Stop when the table count grows beyond a very small hot subset. A practical
  first boundary is three intermediary families total: user bridges, label
  bridges, and code edges.

### Choosing the physical mechanism

If GKG needs an extra access path, the ClickHouse guidance suggests choosing the
physical mechanism deliberately:

- Use a projection when the alternate layout should stay mostly invisible to the
  compiler and query text, and when the source table should remain the main
  logical entry point.
- Use a materialized view when the derived layout has different semantics,
  especially pre-aggregated or intentionally narrower data.
- Use a second explicit table when primary-key control and explicit routing are
  more important than write-path simplicity.

For GKG, that maps to:

- Projection first for `gl_edge.reverse_lookup` and hot filter-plus-sort paths on
  existing node tables
- Materialized views only for clearly derived owner/project summaries
- Explicit intermediary tables only for a very small number of hot bridge or
  fact layouts where the query path must be deterministic

This also means that "intermediary table" should not be treated as one thing.
In practice there are three different classes:

- A transparent alternate layout on the same logical table
- A derived semantic table
- A separately routed hot-path table

## Ranked Recommendations

### P0

1. Add a formal benchmark workflow based on `system.query_log`, filesystem-cache
   control, `EXPLAIN`, and repeatable comparison runs.
2. Replace `gl_edge.by_target` with a full-row `reverse_lookup` projection that starts with `traversal_path`.
3. Broaden denormalization from `project_id` to a hot-FK layer:
   `project_id`, `author_id`, `merge_user_id`, `milestone_id`, `namespace_id`, `merge_request_id`, and similar scalar keys where ETL already has the data.
4. Add workload-shaped projections for the hottest filter-plus-sort combinations instead of relying only on skip indexes.
5. Add at most one specialized intermediary bridge or fact table first:
   `ASSIGNED`, `HAS_LABEL`, `REVIEWER`, or a single owner-aware SDLC fact table.
6. Keep `gl_user` on a separate optimization track using dictionary/direct-join ideas.

### P1

1. Finish slim base queries and post-redaction hydration so the base query reads ids, auth columns, filters, and sort keys first.
2. Rewrite `Direction::Both` paths to avoid `OR` join conditions where possible.
3. Test projection indexes as a lower-storage alternative for reverse lookup once all target environments support the relevant features.

### P2

1. Benchmark selected node tables with base `ORDER BY id` plus traversal-path projection, but do not generalize until secured JOIN plans are validated.
2. If generic `gl_edge` remains hot after the changes above, add a small number of specialized intermediary tables for the hottest edge families only.
3. Consider dual forward and reverse physical edge tables only if projections prove operationally insufficient.

## Risks and Caveats

- The current optimization memo overstates the benefit of changing join algorithms relative to changing physical access paths.
- Some denormalization targets are easy, but not all. `WorkItem` and polymorphic `Note` are immediate examples where the source shape is less friendly.
- The code-indexing tables already use a different pattern in [graph.sql](../../config/graph.sql#L414), which suggests the SDLC side should be challenged more aggressively.
- Any larger table-layout change must be evaluated together with namespace deletion, cleanup, and dedup semantics.

## Recommended Next Benchmark Matrix

Benchmark these variants on representative secured queries:

1. Current schema
2. Current schema plus `reverse_lookup`
3. Current schema plus hot-FK denormalization
4. Current schema plus hot filter-and-sort projections
5. Current schema plus one specialized intermediary bridge table for a hot many-to-many relation
6. Current schema plus one owner-aware intermediary fact table for a hot SDLC family
7. Selected node tables with base `ORDER BY id` plus traversal-path projection

For validation runs:

- Use `EXPLAIN indexes = 1`
- Also use `EXPLAIN projections = 1` and `EXPLAIN PIPELINE`
- Capture `system.query_log` for each run
- Disable query condition cache when comparing index behavior
- Control filesystem cache effects during benchmarking
- Verify projection use through `system.query_log.projections`
- Measure read rows, read bytes, memory, and elapsed time
- Measure storage cost and write amplification for every alternate layout
- Compare both cold and warm cache behavior
- Change one variable at a time

## Local Source Notes

The most relevant local ClickHouse references used in this update were:

- `/Users/angelo.rivera/developer/ClickHouse/docs/en/sql-reference/statements/explain.md:184-188`
  for the current `EXPLAIN indexes` and `EXPLAIN projections` guidance
- `/Users/angelo.rivera/developer/ClickHouse/docs/en/operations/system-tables/query_log.md:16-22,54-71`
  for the query-log fields that matter to optimization
- `/Users/angelo.rivera/developer/ClickHouse/docs/en/sql-reference/statements/system.md:186-199`
  for `SYSTEM FLUSH LOGS`
- `/Users/angelo.rivera/developer/ClickHouse/docs/en/operations/utilities/clickhouse-benchmark.md:12-18,47-76`
  for a repeatable benchmark harness
- `/Users/angelo.rivera/developer/ClickHouse/docs/en/engines/table-engines/mergetree-family/mergetree.md:162-174`
  for part, mark, and granule behavior
- `/Users/angelo.rivera/developer/ClickHouse/docs/en/engines/table-engines/mergetree-family/mergetree.md:197-199`
  for sparse-index overread behavior
- `/Users/angelo.rivera/developer/ClickHouse/docs/en/engines/table-engines/mergetree-family/mergetree.md:205-239`
  for primary-key and sort-key tradeoffs
- `/Users/angelo.rivera/developer/ClickHouse/docs/en/engines/table-engines/mergetree-family/mergetree.md:622,636-673`
  for projection constraints, storage, and selection
- `/Users/angelo.rivera/developer/ClickHouse/docs/en/sql-reference/statements/alter/projection.md:63-75`
  for verifying projection usage in `system.query_log`
- `/Users/angelo.rivera/developer/ClickHouse/docs/en/sql-reference/statements/select/join.md:23,45-47,74-82`
  for JOIN product and `OR`-in-`ON` behavior
- `/Users/angelo.rivera/developer/ClickHouse/docs/en/operations/analyzer.md:209-218`
  for using `normalized_query_hash` to isolate query shapes

## Bottom Line

The current workload looks more like secured fact-table analytics with occasional graph edges than like a graph-native multi-hop traversal engine.

That means the next round of optimization should focus on:

- Better physical access paths for secured one-hop reads
- Fewer unnecessary edge traversals
- Narrower base scans
- Selective denormalization and selective intermediary tables

It should not focus first on:

- General merge-join tuning
- Deep traversal optimizations
- Large-scale physical table fan-out
