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

### 1. SETTINGS `query_plan_convert_join_to_in = 1`

- Appends ClickHouse SETTINGS clause to queries with relationships
- Tells ClickHouse optimizer to auto-convert JOINs to IN subqueries when the right side is small
- Added `settings` field to AST `Query` struct, codegen SETTINGS emission, and optimizer pass
