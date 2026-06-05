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
estimated_rows = total_row_count * product(filter_selectivity)
```

Filter selectivity per type:

- Equality: `value_count / total_row_count` (from value frequency table)
- IN list: sum of matching value counts / total
- Range: sum of histogram buckets in range / total
- Contains/starts_with/ends_with: token frequency lookup, fallback 0.05
- No filter: 1.0

Independence between filters is assumed (standard in all query planners).

### Join order selection

The planner enumerates valid join orderings constrained by the ontology's declared
relationships. For each ordering it estimates the intermediate row count at each step
using the cardinality model. It picks the ordering with the lowest estimated total.

The search space is small: the DSL caps at 5 nodes and 4 relationships, producing
10-20 valid orderings. Brute-force evaluation is sub-microsecond. No dynamic
programming is needed.

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
