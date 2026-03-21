# Large Architectural Improvements

Query optimizations that require significant changes or schema modifications.

## 1. Edge-Only Aggregation for COUNT queries

**Impact**: Could eliminate reading entire node tables (e.g., gl_user 1.5M rows, gl_note)

For aggregation queries where an intermediate node is only used as a COUNT target with no
filters, the node JOIN exists solely to validate existence. Replace `COUNT(node.id)` with
`COUNT(edge.target_id)` and skip the node table entirely.

Example (q21 - MR reviewers per project):

```sql
-- Current: reads 1.5M users into hash table for COUNT(reviewer.id)
FROM gl_merge_request mr
JOIN gl_edge e0 (REVIEWER) ON mr.id = e0.source_id
JOIN gl_user reviewer ON e0.target_id = reviewer.id  -- 747 granules
JOIN gl_edge e1 (IN_PROJECT) ON mr.id = e1.source_id
JOIN gl_project p ON e1.target_id = p.id

-- Proposed: skip gl_user entirely
FROM gl_merge_request mr
JOIN gl_edge e0 (REVIEWER) ON mr.id = e0.source_id
JOIN gl_edge e1 (IN_PROJECT) ON mr.id = e1.source_id
JOIN gl_project p ON e1.target_id = p.id
-- COUNT(e0.target_id) instead of COUNT(reviewer.id)
```

**Risks**:

- Orphaned edges (edge target_id references deleted user) would inflate counts
- Security: gl_user is in SKIP_SECURITY_FILTER_TABLES anyway, so no auth concern
- Semantics: changes from "count existing users" to "count edges"

**Changes needed**:

- Optimizer pass to detect eliminable JOINs (COUNT-only target, no filters, no selected cols)
- Rewrite aggregate target from node column to edge column
- Remove the node table from the JOIN tree and edge-to-node JOIN

**Estimated impact**: q21 ~4.9M -> ~3.4M rows (remove 1.5M user read)

## 2. Edge-Only Aggregation for Broad-Root Queries

Same pattern but more aggressive: for queries like q5 (count MRs per project) and q17
(count notes per user), the root node table has no filters. The entire aggregation could
be computed from the edge table alone:

```sql
-- Current q5: reads gl_merge_request + gl_edge + gl_project
-- Proposed: edge-only with project join
SELECT p.id, COUNT(e0.source_id)
FROM gl_edge e0
JOIN gl_project p ON e0.target_id = p.id
WHERE e0.relationship_kind = 'IN_PROJECT'
  AND e0.source_kind = 'MergeRequest'
  AND startsWith(e0.traversal_path, '1/')
GROUP BY p.id
```

**Risks**:

- Security filter bypass: root node's traversal_path filter is lost
- Edge traversal_path may not match node traversal_path (currently they do)
- Orphaned edges inflate counts

**Estimated impact**: q5 3.36M -> ~1.7M rows (skip root table)

## 3. by_source Projection with source_kind in Sort Order

**Impact**: Enable granule skipping for relationship_kind + source_kind queries

Current by_source projection: `ORDER BY (source_id, relationship_kind, ...)`
Proposed: `ORDER BY (source_kind, relationship_kind, source_id, ...)`

This would let ClickHouse skip granules based on source_kind, dramatically reducing
reads for queries that filter by both kind and relationship. Currently, kind filtering
happens after reading the granule.

**Risks**: Schema migration required, projection rebuild, storage impact

**Estimated impact**: 30-50% reduction for edge scans with kind filters

## 4. Data-Skipping Indexes on Edge Table

Add bloom filter or minmax indexes on frequently filtered columns:

- `relationship_kind` (LowCardinality) - bloom filter
- `source_kind` / `target_kind` (LowCardinality) - bloom filter
- `source_id` / `target_id` (Int64) - minmax

These would enable granule skipping on the main table PK (which only has
traversal_path, relationship_kind as primary key columns).

**Risks**: Storage overhead, insertion performance impact

**Estimated impact**: Variable, depends on data distribution within granules

## 5. Reverse SIP for Intermediate Nodes

For intermediate nodes connected via small edge sets, push edge-derived IDs into the
node table scan. Example: REVIEWER edges (6,260) connect to ~2K users, but we read
all 1.5M users.

```sql
-- Push reviewer user IDs from edge into user scan
WHERE reviewer.id IN (
  SELECT target_id FROM gl_edge
  WHERE relationship_kind = 'REVIEWER' AND startsWith(traversal_path, '1/')
)
```

**Risks**: Doubles edge reads (once for subquery, once for JOIN), but edge set is small

**Changes needed**: Optimizer pass to detect intermediate nodes with small connected edge sets

**Estimated impact**: q21 gl_user 747 -> ~3 granules (if 2K unique users)
