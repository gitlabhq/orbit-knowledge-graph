# Query Optimization Log — Phase 2

## Session Start

- Branch: `michaelangeloio/query-optimizer-phase2`
- Based on: `01f0602` (cascading SIP, UNION ALL neighbors, hop frontiers)

## Baseline (from Phase 1)

| Query | rows_read | Status |
|-------|-----------|--------|
| neighbors_mr (both) | 8,613 | 99.97% reduced |
| path_depth_3 | 3,653,580 | 88% reduced |
| pipeline_jobs | 28,501 | 99.7% reduced |
| stress_multi_node_ids | 277,443 | 87.2% reduced |
| agg_mrs_per_project | 3,361,260 | 0% (broad root) |
| stress_wide_limit | 3,361,265 | 0% (broad root) |

## Optimizations Implemented

### 1. SETTINGS `query_plan_convert_join_to_in = 1` (commit a09841b)

- Appends ClickHouse SETTINGS clause to queries with relationships
- Tells ClickHouse optimizer to auto-convert JOINs to IN subqueries when the right side is small
- Added `settings` field to AST `Query` struct, codegen SETTINGS emission, and optimizer pass

### 2. Filtered-node SIP for non-root nodes (commit 5350cb6)

- Generalized the aggregation-only target SIP to work for ALL query types
- Any non-root `to` node with WHERE filters gets materialized in a CTE and pushed into the adjacent edge scan
- Triggers ClickHouse `by_target` projection for dramatic granule reduction
- Measured impact on staging:
  - q3 (Draft MRs traversal): edge scan 13,357 -> 218 granules, elapsed 248ms -> 80ms
  - 267 draft opened MRs correctly filtered into 267-element set
- Subsumes the old `apply_target_sip_prefilter` (was aggregation-only)
- 201 compiler unit tests pass

### 3. Entity kind filters on edge JOINs (commit 9db92d6)

- **Correctness fix**: Edge JOINs previously matched only on source_id/target_id without
  filtering source_kind/target_kind. Different entity types can share numeric IDs (e.g.,
  Pipeline id=1234 and Job id=1234), causing cross-entity inflation in aggregation counts.
- q7 was reporting 78,338 pipelines for top project; correct value is 60,239 (30% inflation!)
- Applied kind filters in JOIN ON (lowering) via `source_join_cond_with_kind` and
  `target_join_cond_with_kind` functions
- Removed dead `source_join_cond` and `target_join_cond` functions

### 4. Skip cascade CTE for broad-filter roots (commit 824d1a9)

- Cascade CTEs were scanning the edge table a second time just to find reachable target IDs
- For broad filters (e.g., 87% of pipelines match), this doubled the edge scan cost
- Now only cascade when root has provably narrow selectivity: node_ids, id_range, or cursor
- Impact: q7 17.3M -> 8.75M rows (49.5%), q9 12.3K -> 6.4K rows (48%)

### 5. LIMIT pushdown into SIP CTE for traversal queries (pending commit)

- For traversal queries with ORDER BY on root node + LIMIT, push ORDER BY and a padded
  LIMIT (3x multiplier) into the _root_ids CTE
- Narrows the SIP set from all matching root IDs to top-N candidates
- Impact: q6 (failed pipelines) 7,925,387 -> **196,237 rows (97.5% reduction)**,
  elapsed 293ms -> 89.6ms (69.5% faster)
- Only applies to Traversal (not Aggregation — needs all rows for correct counts)
- Safety: 3x multiplier accounts for JOINs that may filter out some root IDs

### 6. Remove duplicate kind predicates from optimizer (pending commit)

- `apply_edge_kind_predicates` was adding source_kind/target_kind filters to WHERE,
  but these were already in JOIN ON (from lower.rs opt 3) and ClickHouse auto-promotes
  them to PREWHERE
- EXPLAIN showed duplicate predicates evaluated twice per row in edge PREWHERE
- Removed the redundant optimizer pass; kind filters now only live in JOIN ON
- Also removed dead `Direction::kind_columns()` helper

## Staging Profiling Results (all optimizations applied)

| Query | Description | rows_read | elapsed | Change from Phase 1 |
|-------|-------------|-----------|---------|---------------------|
| q2 | Open MRs in project | 39,544 | 57ms | baseline |
| q3 | Draft MRs traversal | 2,012,168 | 80ms | -10.6% |
| q4 | MRs merged by user | 68,216 | 43ms | baseline |
| q5 | Count MRs per project | 3,361,536 | 136ms | unchanged (broad) |
| q6 | Failed pipelines | **196,237** | **89ms** | **-97.5%** |
| q7 | Pipeline success rate | 8,750,731 | 252ms | -49.5% + correct |
| q9 | Critical vulns | 6,380 | 48ms | -48.2% |
| q11 | Vulns fixed by MRs | 62,174 | 27ms | baseline |
| q14 | User MR history | 4,096 | 11ms | baseline |
| q16 | Comment activity | 418,887 | 116ms | baseline |
| q17 | Comment activity/user | 2,801,085 | 503ms | unchanged (broad) |
| q20 | User contributions | 2,265,277 | 94ms | unchanged (broad) |
| q21 | MR reviewers/project | 4,947,097 | 160ms | unchanged (broad) |
| q24 | Branch by project | 0 | 65ms | baseline |
| q28 | Bot accounts | 385 | 42ms | baseline |

## Key Findings

- gl_edge has 30.3M rows, projections `by_source` and `by_target`
- Edge PK: `(traversal_path, relationship_kind, source_id, ...)`
- Main edge PK only has `(traversal_path, relationship_kind)` as primary key columns
- `startsWith('1/')` matches ALL granules for org-wide queries (no granule skipping)
- SIP with by_source projection reads 1,611/13,357 granules (12%) for broad root
- Without SIP, main table with relationship_kind reads 4,460/14,808 granules (30%)
- SIP IS net positive even for broad root queries
- Edge data distribution: IN_PROJECT=9M, TRIGGERED=8.2M, HAS_JOB=8.1M, HAS_FILE=3M
- AUTHORED edges: 598K, REVIEWER edges: 6,260
- Failed pipelines: 101,365 (LIMIT pushdown reduces SIP set from 101K to 90 IDs)
- gl_user: 1.5M rows, 747 granules — read fully for intermediate JOINs
- **Cross-entity ID collision**: Pipeline/Job IDs overlap numerically (fixed in opt 3)
- **Cascade CTE cost**: For broad roots, cascade CTE reads ~50% of total rows redundantly
- **LIMIT pushdown**: For traversal + ORDER BY on root, narrows SIP CTE to top-N*3 IDs
- **Duplicate PREWHERE**: JOIN ON conditions already promote to PREWHERE; no need for
  redundant WHERE predicates (removed in opt 6)

## Reminders

- ALWAYS run integration tests after changes (worktree needs CONFIG_DIR, FIXTURES_DIR)
- When using worktree, start server with explicit binary path, not `cargo run`
- Worktree integration tests: `CONFIG_DIR=$(pwd)/config FIXTURES_DIR=$(pwd)/fixtures cargo test -p integration-tests`

## Remaining Broad Queries (fundamentally limited by data volume)

- q5: 3.36M — all MRs x IN_PROJECT edges x projects (no filter)
- q7: 8.75M — pipelines with status filter x IN_PROJECT edges x projects
- q17: 2.8M — all notes x AUTHORED edges x users
- q20: 2.27M — all MRs x AUTHORED edges x users
- q21: 4.95M — all MRs x REVIEWER edges x users x IN_PROJECT edges x projects

See `improvements_large.md` for architectural changes that could address these.
