# Layered Traversal Research

Research into replacing edge-centric JOIN-based traversals with a layered
pre-fetch pattern using `groupArray` + `arrayJoin`, and `arrayFold` for
frontier propagation.

## Problem

The current edge-centric traversal compiles multi-relationship queries into
edge-edge JOINs with cascade CTE filters:

```sql
WITH
  _nf_u AS (SELECT id FROM gl_user WHERE id IN [1,3,5]),
  _cascade_mr AS (SELECT target_id AS id FROM gl_edge WHERE source_id IN (SELECT id FROM _nf_u) AND ...),
  _nf_mr AS (SELECT id FROM gl_merge_request WHERE state='merged' AND id IN (SELECT id FROM _cascade_mr)),
  ...
SELECT e0.*, e1.*
FROM gl_edge e0
INNER JOIN gl_edge e1 ON e0.target_id = e1.source_id
WHERE ... AND e0.source_id IN (SELECT id FROM _nf_u)
      AND e0.target_id IN (SELECT id FROM _nf_mr)
LIMIT 50
```

Issues:

- The hash join materializes the full right side before LIMIT kicks in.
- CTEs are inlined in ClickHouse (no materialization), so referenced CTEs
  re-evaluate at each use.
- Q11 (Project -> Pipeline -> Job) reads 8.5M rows and returns 0 results.
- Q09 (aggregation, 3 pinned projects) reads 4.4M rows to return 3 results.

## Layered pre-fetch pattern

Pre-fetch each hop's matching edges into an array via `groupArray`, then narrow
subsequent hops using the previous hop's frontier via `IN (SELECT arrayJoin(...))`.
The final output joins arrays in memory via `arrayJoin`, eliminating the edge-edge
table JOIN.

```sql
WITH
layer0 AS (
  SELECT groupArray((source_id, target_id, traversal_path, source_kind, target_kind, relationship_kind)) AS edges
  FROM gl_edge
  WHERE source_id IN (1, 3, 5)
    AND relationship_kind = 'AUTHORED'
    AND target_kind = 'MergeRequest'
    AND startsWith(traversal_path, '1/')
    AND target_id IN (SELECT id FROM gl_merge_request WHERE state = 'merged')
),
hop0_frontier AS (
  SELECT arrayDistinct(arrayMap(e -> e.2, (SELECT edges FROM layer0))) AS ids
),
layer1 AS (
  SELECT groupArray((source_id, target_id, traversal_path, source_kind, target_kind, relationship_kind)) AS edges
  FROM gl_edge
  WHERE relationship_kind = 'IN_PROJECT'
    AND source_kind = 'MergeRequest'
    AND startsWith(traversal_path, '1/')
    AND source_id IN (SELECT arrayJoin((SELECT ids FROM hop0_frontier)))
)
SELECT
  l0.1 AS e0_src, l0.2 AS e0_dst, l0.6 AS e0_type, ...
  l1.1 AS e1_src, l1.2 AS e1_dst, l1.6 AS e1_type, ...
FROM (SELECT arrayJoin((SELECT edges FROM layer0)) AS l0) t0
JOIN (SELECT arrayJoin((SELECT edges FROM layer1)) AS l1) t1
  ON t0.l0.2 = t1.l1.1
LIMIT 50
```

Each layer scans the edge table once with a narrow IN filter from the previous
frontier. The final JOIN operates on in-memory arrays, not table scans.

## Benchmark results (27M-edge staging dataset)

Base query only (excludes hydration round-trips).

### Traversals

| Query | Description | Current rows | Layered rows | Current ms | Layered ms |
|-------|-------------|-------------:|-------------:|-----------:|-----------:|
| Q01 | User(1,3,5)->MR(merged)->Project | 94,200 | 97,593 | 393 | 82 |
| Q02 | Project(20699)->MR(merged)->Author+Notes | 2,250,450 | 1,982,696 | 605 | 260 |
| Q10 | User(1)->Group | 5,119 | 1,024 | 263 | 25 |
| Q11 | Project(13083)->Pipeline(failed)->Job(failed) | 8,523,330 | 10,305,575 | 614 | 479 |

### Aggregations

| Query | Description | Current rows | Layered rows | Current ms | Layered ms |
|-------|-------------|-------------:|-------------:|-----------:|-----------:|
| Q07 | Count merged MRs per project (broad) | 4,920,532 | 3,518,362 | 527 | 175 |
| Q08 | Sum pipeline duration per project (broad) | 10,234,376 | 7,276,712 | 688 | 248 |
| Q09 | Count merged MRs (3 pinned projects) | 4,407,508 | 857,405 | 495 | 69 |

### Path-finding

| Query | Description | Current rows | Layered rows | Current ms | Layered ms |
|-------|-------------|-------------:|-------------:|-----------:|-----------:|
| Q06 | User(1)->Project(13083) depth 2 | 3,157,248 | 991,162 | 618 | 121 |

### Summary

- 7 of 8 queries improved. Average reduction: 53% fewer rows, 66% faster.
- Q11 is the only regression (10.3M vs 8.5M, +21%). See analysis below.

## arrayFold for frontier propagation

ClickHouse's `arrayFold` can propagate a frontier through pre-fetched edge layers
in a single expression with no inter-iteration CTE overhead:

```sql
SELECT arrayFold(
  (frontier, layer) -> arraySlice(
    arrayDistinct(
      arrayMap(e -> e.1, arrayFilter(e -> has(frontier, e.2), layer))
    ),
    1, 50  -- cap at LIMIT
  ),
  [(SELECT edges FROM l0), (SELECT edges FROM l1)],
  [toInt64(1), toInt64(3), toInt64(5)]  -- starting frontier
) AS final_frontier
```

Tested on Q01: 72ms, 97K rows -- comparable to CTE-chain (82ms). The fold
itself adds no overhead; it computes which nodes are reachable without
materializing intermediate CTEs.

`arraySlice(frontier, 1, LIMIT)` at each step caps the frontier size, providing
natural LIMIT propagation through the traversal.

### Limitation: has() is O(n)

`arrayFilter(e -> has(frontier, e.2), layer)` calls `has()` for each edge in the
layer. `has()` on an `Array(Int64)` is a linear scan -- O(frontier_size) per call.
For a layer with L edges and frontier of size F, the cost is O(L * F).

Q01: F=3 (users), L=104 (edges) -- 312 comparisons, instant.
Q11: F=34,183 (pipelines), L=34,183 (edges) -- 1.2B comparisons, OOM.

This makes arrayFold suitable only when the frontier stays small (<1000 IDs).
For queries where hop N expands the frontier (1 project -> 34K pipelines), the
quadratic cost of `has()` makes arrayFold impractical.

There is no `hasAll` or hash-set variant for use inside array lambdas in
ClickHouse 25.12.

## Why Q11 regresses

Q11: Project(13083) -> Pipeline(status='failed') -> Job(status='failed')

Hop 0 fans out from 1 project to 34,183 failed pipelines. This creates a large
intermediate frontier. Both approaches suffer:

- **Current**: Cascade CTEs narrow the IN sets, but the edge-edge JOIN still
  materializes. Reads 8.5M rows.
- **Layered CTE-chain**: `layer0` (34K edges) gets referenced by both
  `hop0_frontier` and the final `arrayJoin`. CTE inlining re-evaluates `layer0`
  at each reference. Reads 10.3M rows.
- **arrayFold**: `has()` on a 34K frontier is O(34K) per edge, making the fold
  quadratic. OOM at 50GB attempted allocation when node filters were applied
  inside the lambda.

The fundamental issue: this query shape expands then contracts (1 -> 34K -> 0).
Neither approach can avoid scanning the 34K pipeline edges. The layered pattern
helps when traversals narrow at each step (3 users -> 94 MRs -> 12 projects).

Possible fix: two-phase execution. Run hop 0 as a separate query, check if the
frontier is empty or too large, then decide whether to proceed with hop 1. This
requires application-level control flow, not pure SQL.

## When to use which approach

| Pattern | Best for | Avoid when |
|---------|----------|------------|
| Edge-centric JOIN (current) | Single-rel traversals | Multi-rel with large intermediate sets |
| CTE-chain layered | Multi-rel traversals, aggregations, path-finding | Frontier expands >10K at any hop |
| arrayFold | Variable-depth hops from small starting sets | Large frontiers (has() is O(n)) |

## Implementation path

The CTE-chain layered pattern can be implemented as a new lowering function
(`lower_traversal_layered`) that generates `groupArray` CTEs per relationship,
frontier CTEs via `arrayDistinct(arrayMap(...))`, and a final `arrayJoin`-based
SELECT. This requires AST extensions for:

- Tuple expressions: `(source_id, target_id, ...)`
- Scalar subqueries: `(SELECT edges FROM layer_cte)`
- Tuple field access: `l0.1`, `l0.2`
- Lambda expressions: `e -> e.2`
- `arrayJoin`, `arrayMap`, `arrayFilter`, `arrayDistinct`, `groupArray`

These can be added as new `Expr` variants or via a raw SQL escape hatch for
prototyping.
