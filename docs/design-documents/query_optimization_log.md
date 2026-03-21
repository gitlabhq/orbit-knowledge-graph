# Query Optimization Log

## Session 3 (2026-03-21)

### Fix: Filtered-node SIP regression (commit 1fa07a8)

**Problem**: `apply_filtered_node_sip` was creating unnecessary `_target_*_ids` CTEs for
nodes that only had security-injected conditions (`startsWith` on traversal_path), not
user-specified filters. This caused 3x regressions in q6/q7/q9.

**Fix**: Skip filtered-node SIP when the node has no user filters AND a cascade CTE already
covers it. When no cascade exists, security conditions are still worth materializing since
they narrow edge scans that would otherwise be full table scans.

Also gated `query_plan_convert_join_to_in=1` on CTE presence — the setting hurts queries
without SIP CTEs (measured 10.7M → 3.5M on q21 by removing the setting).

| Query | Before | After | Change |
|-------|--------|-------|--------|
| q6 | 535K | 371K | -31% |
| q7 | 25.9M | 17.3M | -33% |
| q9 | 18.8K | 12.4K | -34% |
| q21 | 10.7M | 3.5M | -67% |

### Optimization: Edge-only aggregation (commit e154417)

**Insight**: For COUNT aggregations where the root node is only used as a count target
(not in GROUP BY, no selected columns), the root table JOIN exists solely to count rows.
The edge table already has the matching IDs via SIP, so we can count edge column values
instead and skip the root table entirely.

**Implementation**:
1. Detect: single-hop relationship, all aggregations are COUNT on root, root not in GROUP BY
2. Rewrite `COUNT(root.id)` → `COUNT(edge.source_id)`
3. Remove root table from JOIN tree
4. Remove now-redundant cascade CTE for the GROUP BY node
5. Clean up root-only WHERE conditions

| Query | Before | After | Change |
|-------|--------|-------|--------|
| q7 | 17.3M | 8.6M | **-50%** |
| q5 | 3.36M | 3.33M | -1% (root has no explicit filters) |

## Session 4 (2026-03-21)

### Fix: ORDER BY rewrite in edge-only aggregation (commit d61f223)

**Problem**: Edge-only aggregation rewrote `COUNT(root.id)` to `COUNT(edge.source_id)` in
SELECT but left ORDER BY unchanged, causing ClickHouse error when the root table was eliminated.

**Fix**: Also rewrite COUNT targets in ORDER BY expressions.

### Optimization: Multi-relationship root narrowing (commit 1fd2eb6)

**Insight**: For multi-relationship aggregation queries where all relationships share the
same root node (e.g., q21: MR → REVIEWER → User, MR → IN_PROJECT → Project), the root
SIP CTE contains all matching root IDs (31K MRs). But most MRs have no REVIEWER edges
(only 6.2K do). By narrowing the root set to only IDs that participate in the first
relationship, subsequent edge scans (IN_PROJECT) read far fewer rows.

**Implementation**:
1. Detect: aggregation query, 2+ relationships, all from same root, root not in GROUP BY
2. Create `_root_narrowed` CTE: `SELECT source_id FROM gl_edge WHERE source_id IN (_root_ids) AND relationship_kind = 'REVIEWER'`
3. Use `_root_narrowed` instead of `_root_ids` for edge SIP injection

| Query | Before | After | Change |
|-------|--------|-------|--------|
| q21 | 3.66M | 1.98M | **-46%** |

### Optimization: Target-only COUNT elimination (commit e3368dc)

**Insight**: When the COUNT target node has no user filters and isn't in GROUP BY,
the target table JOIN exists only to validate existence. Replace `COUNT(target.id)`
with `COUNT(edge.end_col)` and eliminate the target table entirely. This is the
counterpart to edge-only aggregation (which eliminates the root).

**Implementation**:
1. Find the relationship whose `to` node is the COUNT target
2. Rewrite `COUNT(target.id)` → `COUNT(edge.end_col)` in SELECT and ORDER BY
3. Remove target table from JOIN tree (recursive to handle any position)
4. Preserve edge-side conditions (e.g., `target_kind = 'Note'`) in WHERE
5. Remove cascade CTE for the eliminated target node

| Query | Before | After | Change |
|-------|--------|-------|--------|
| q17 | 2.78M | 2.21M | **-20%** |
| q20 | 1.71M | 1.68M | -2% |
| q21 | 1.98M | 1.68M | **-15%** |

## Cumulative Results (all optimizations this branch)

| Query | Description | Baseline | Current | Total Change |
|-------|-------------|----------|---------|-------------|
| q2 | Open MRs | 238K | 238K | Same |
| q3 | Draft MRs | 1.18M | 1.18M | Same |
| q5 | Count MRs/project | 3.36M | 3.33M | -1% |
| q6 | Failed pipelines | 371K | 371K | Same |
| q7 | Pipeline success rate | 17.3M | 8.6M | **-50%** |
| q9 | Critical vulns | 12.4K | 12.4K | Same |
| q10 | Vuln aggregation | 51 | 51 | Same |
| q17 | Comment activity | 2.8M | 2.21M | **-20%** |
| q20 | User contributions | 2.27M | 1.68M | **-26%** |
| q21 | MR reviewers/project | 3.5M | 1.68M | **-52%** |

## Remaining Bottlenecks

The remaining high-row queries (q7: 8.6M, q5: 3.3M, q17: 2.2M) are dominated by edge
table scans where the `by_source` projection sort order `(source_id, relationship_kind)`
doesn't include `source_kind`. This means `source_kind = 'Pipeline'` or `'MergeRequest'`
filtering happens post-read, scanning all IN_PROJECT edges regardless of entity type.

Further improvement requires schema changes:
- Add `source_kind` to `by_source` projection: `ORDER BY (source_kind, relationship_kind, source_id)`
- Add node table projections for filtered queries (e.g., `(state, created_at)` on gl_merge_request)
- See `clickhouse_schema_analysis_2026_03_20.md` for details
