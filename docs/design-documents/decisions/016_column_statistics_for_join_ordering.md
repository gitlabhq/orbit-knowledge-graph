---
title: "GKG ADR 016: Column statistics for cost-based join ordering"
creation-date: "2026-06-05"
authors: [ "@michaelusa" ]
toc_hide: true
---

## Status

Proposed

## Date

2026-06-05

## Context

The query compiler chooses join order using heuristic selectivity labels (`High`/`Low`)
from the ontology and a fixed strategy per query type: edge-first for flat chains,
node-first for FK-star. The heuristics are coarse. A `High` selectivity string filter
on a column with 50K distinct values and a `High` selectivity string filter on a column
with 3 distinct values receive identical treatment.

Profiling showed this matters. Replacing FilterOnly CTEs with JOINs (!1533) cut
read_rows by 70% on code-graph queries. But the improvement was accidental -- the
compiler happened to emit a better shape for that query. For a different filter
combination the old shape might have been better. Without cardinality data, the
compiler cannot make this choice systematically.

The compiler already supports multiple join strategies:

| Strategy | Entry point | When used |
|---|---|---|
| FK-star | Center node | All hops FK to same center |
| Flat chain | Edge table | Default for edge-based queries |
| Single node | Node table | No edges |

The gap is not strategy support but strategy selection. The compiler needs per-column,
per-namespace row counts and value distributions to estimate which entry point and
join order minimizes intermediate result sizes.

## Decision

### Query types affected

Join ordering applies to any query that joins two or more tables. In the current
compiler, that is every query type except bare single-node searches:

| Query type | Tables joined | Current join strategy |
|---|---|---|
| Traversal (single-hop) | 1 edge + 1-2 nodes | Edge-first, nodes JOINed |
| Traversal (multi-hop) | N edges + N+1 nodes | Edge-first, each hop JOINed sequentially |
| Aggregation | Same as traversal + GROUP BY | Same as traversal |
| FK-star | Center node + N target nodes (no edges) | Center-first via FK columns |
| Neighbors | 1 center node + 1 edge scan | Center-first |
| PathFinding | 2 anchor nodes + recursive edge CTEs | Anchor CTEs, then edge expansion |

For single-hop traversals, the choice is: start from the edge table or start from
the more selective node and join to the edge. For multi-hop, the direction of each
hop matters. For FK-star, the order of target JOINs matters (join the smallest
target first to narrow the pipeline early). For neighbors, the choice between center
scan vs edge scan depends on whether the center has selective filters beyond the
pinned IDs.

PathFinding is partially constrained by its recursive CTE structure, but the anchor
selection (which endpoint to expand from first, forward vs backward depth split)
is a join ordering decision.

### Stat types

Three stat types, each in its own AggregatingMergeTree table:

| Table | Stat type | Covers | Key columns |
|---|---|---|---|
| `gkg_column_stats` | Value frequency | Categorical (boolean, enum, low-NDV string) | (table, column, tp, value) |
| `gkg_token_stats` | Token frequency | Text (medium/high-NDV string) | (table, column, tp, token) |
| `gkg_histogram_stats` | Equi-depth histogram | Continuous (int64, timestamp, date, float) | (table, column, tp, bucket) |

UUIDs are skipped (selectivity is always 1/row_count). Virtual and non-filterable
columns are skipped. The categorization is derived from ontology property types at
load time, not manually listed.

### Collection via materialized views

One MV per node table fires on insert and writes aggregate states to the
corresponding stats table. The MV uses `uniqState(id)` to count distinct entities per
value/bucket. `uniqState` is based on HyperLogLog, which is idempotent for duplicate
values -- ReplacingMergeTree row versions do not inflate counts.

Deleted rows (`_deleted = true`) contribute to stats. This is a known inaccuracy
accepted because the alternative (querying with FINAL) requires a periodic batch job,
which we want to avoid. For join ordering the error is negligible.

### Namespace scoping

Stats are partitioned by `traversal_path`. Each namespace gets its own stat rows.
Entities without `traversal_path` (User, Runner) use an empty partition key for
global stats. Total row count per entity comes from the histogram table's `id` column.

### Dictionary for query-time lookups

A ClickHouse dictionary (`gkg_column_stats_dict`) over the stats tables provides
sub-millisecond lookups. It auto-refreshes every 2-5 minutes via `LIFETIME(MIN 120
MAX 300)`. The server fetches stats for the user's traversal paths before compilation
and passes them into `CompilerMetadata`.

### Cardinality estimation

The planner estimates rows after filtering:

```
estimated_rows = total_row_count * product(selectivity_per_filter)
```

Where `total_row_count` comes from the histogram table's `id` column
(`uniqMerge(row_count)` across all buckets for the entity's `id` field).

**Selectivity per filter type:**

| DSL filter op | Stat source | Selectivity formula |
|---|---|---|
| `eq` | Value frequency | `value_count / total_row_count` |
| `in` | Value frequency | `sum(matching value_counts) / total_row_count` |
| `gt`, `lt`, `gte`, `lte` | Histogram | `sum(buckets in range) / total_row_count` |
| `contains` | Token frequency | `token_count / total_row_count`, fallback 0.05 |
| `starts_with` | Token frequency | same as `contains` |
| `ends_with` | Token frequency | same as `contains` |
| `is_null` | Value frequency | `null_value_count / total_row_count`, fallback 0.01 |
| (no filter) | -- | 1.0 |
| (pinned IDs) | -- | `min(len(node_ids), total_row_count) / total_row_count` |
| (id_range) | Histogram | `sum(buckets in range) / total_row_count` |

When multiple filters apply to the same node, their selectivities are multiplied
(independence assumption). This is the standard approach in PostgreSQL, MySQL, and
every other query planner. Correlated filters (e.g., `city = 'Paris' AND country =
'France'`) produce overestimates, but in the GKG ontology most filter columns are
genuinely independent.

**Join output estimation** uses the FK-join formula:

```
join_output = left_rows * right_rows / max(ndv_left_key, ndv_right_key)
```

For edge-to-node joins, the join key is `id` on the node side and `source_id` or
`target_id` on the edge side. The NDV of the join key comes from the histogram table.

**Example:**

```
Query: User(node_ids=[1]) --AUTHORED--> MR(source_branch="main") --IN_PROJECT--> Project

Stats for namespace 1/9970/:
  gl_merge_request: total=82K, source_branch NDV=4200
  gl_edge (AUTHORED, source_kind=User): ~500K rows
  gl_project: total=200

Entry point estimates:
  User:    1 row (pinned)
  MR:      82K * (1/4200) = ~20 rows
  Edge:    500K * (1/82K) = ~6 rows (narrowed by source_id=1)

Ordering 1: User → edge → MR → edge → Project
  Step 1: 1 user
  Step 2: ~6 edges (source_id=1, rel=AUTHORED)
  Step 3: ~6 MRs joined, ~1 passes source_branch filter
  Step 4: 1 FK lookup to Project
  Total work: ~13 rows processed

Ordering 2: MR(source_branch=main) → edge → User → edge → Project
  Step 1: ~20 MRs
  Step 2: ~20 edges joined
  Step 3: 1 user matches (pinned)
  Step 4: ~1 FK lookup
  Total work: ~42 rows processed

Planner picks ordering 1.
```

### Join order selection

The planner enumerates valid join orderings constrained by the ontology's declared
relationships. For each ordering it estimates the intermediate row count at each step
using the cardinality model. It picks the ordering with the lowest estimated total.

The search space is small: the DSL caps at 5 nodes and 4 relationships, producing
10-20 valid orderings. Brute-force evaluation is sub-microsecond. No dynamic
programming is needed.

### Pipeline placement

The server query pipeline currently runs these stages in order:

```
Security → PathResolution → Compilation → ClickHouse → Extraction →
Authorization → Redaction → Hydration → Output
```

Stats fetching runs **after PathResolution and before Compilation**. At that point
the server has:

- The user's resolved traversal paths (from PathResolution)
- The parsed query input with node entities and filters (from Security/validation)
- The ontology (loaded at startup)

It does not yet have compiled SQL. This is the right moment because the compiler
needs the stats to make join ordering decisions during the plan pass.

The stats fetch is a single `dictGet` call (or a small batch) to the ClickHouse
dictionary, returning stats for the query's node tables scoped to the user's
traversal paths. Results are cached in-process with a 2-5 minute TTL keyed by
`(table, column, traversal_path)`. For most requests the stats come from the cache
with zero ClickHouse round-trips.

The stats are passed into `CompilerMetadata` as a `HashMap<(table, column), ColumnStats>`
where `ColumnStats` holds value frequencies, token frequencies, or histogram buckets
depending on column type. The compiler's plan pass reads this map during
`optimize_join_order`.

### Schema configuration

The `statistics` section in `schema.yaml` declares table names, dictionary config,
histogram bucket count, top-K token count, partition key, and exclusions. Column
categorization is auto-derived from ontology property types. The config is 15 lines.

```yaml
statistics:
  stats_table: gkg_column_stats
  histogram_table: gkg_histogram_stats
  token_table: gkg_token_stats
  dictionary: gkg_column_stats_dict
  lifetime: {min: 120, max: 300}
  histogram_buckets: 16
  top_k_tokens: 100
  partition_key: traversal_path
  exclude:
    - {node: Note, columns: [st_diff]}
```

## Implementation plan

1. Schema config and ontology types -- !1555 (done)
2. DDL generation: stats tables, MVs, dictionary
3. Server pipeline stage: fetch stats, cache with TTL, pass to compiler
4. Compiler plan pass: `optimize_join_order` after `build_hops`
5. Compiler lowering: generalize `emit_flat_chain` to start from node or edge

## Why not the alternatives

**system.parts metadata only.** Gives approximate global row counts with zero
infrastructure. But no per-namespace scoping, no per-column NDV, no value
distributions. Too coarse for join ordering where the decision depends on filter
selectivity, not just table size.

**Periodic batch job.** Would allow FINAL-based scans for exact stats. But adds
operational complexity (scheduler, failure handling, staleness windows). The MV
approach is simpler and self-maintaining.

**Inline subquery at query time.** Compute stats on the fly before each query. Too
slow -- a `SELECT uniqExact(state) FROM gl_merge_request FINAL` is itself a full
table scan, which is what we're trying to avoid.

**Single stat type (NDV + row_count only).** Covers equality and IN filters but
cannot estimate range selectivity or text filter selectivity. Three types cover all
DSL filter patterns with minimal additional complexity.

## Consequences

**What improves:**

- Queries with multiple filtered nodes get the optimal join order instead of the
  fixed edge-first or node-first strategy.
- The planner can skip narrowing CTEs when stats show the filter is non-selective,
  replacing the current binary `High`/`Low` heuristic with actual numbers.
- Code-graph queries (File, Definition, ImportedSymbol) benefit most due to large
  table sizes and high variance in filter selectivity.

**What gets harder:**

- Schema migrations must create/drop the stats tables, MVs, and dictionary alongside
  graph tables.
- The MV-per-table pattern generates ~25 MVs. Each fires on every insert to its
  source table, adding write amplification.
- Stats for deleted entities are slightly inflated until the HyperLogLog sketches
  are rebuilt.

## References

- Issue: #826
- Schema config MR: !1555
- FilterOnly-to-JOIN profiling (!1533): 70% read reduction on code-graph queries
- ClickHouse AggregatingMergeTree docs: https://clickhouse.com/docs/engines/table-engines/mergetree-family/aggregatingmergetree
- Prior art: `config/ontology/schema.yaml` `auxiliary_dictionaries` section
- Prior art: `crates/ontology/src/loading/mod.rs` materialized view loading
