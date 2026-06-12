# Aggregation lowering: why FINAL stays the default

Status: decided (FINAL retained). Records an investigation into replacing the
`FINAL` aggregation form with `argMax`-based alternatives, and why the
measurements rejected them on a healthy (well-merged) cluster.

## Current form

Count-by-state aggregations ("merged MRs per author", "failed jobs by name")
lower to one form in `crates/query-engine/compiler/src/passes/lower/`
(`single_node.rs` emits `TableRef::scan_final`): scan with `FINAL`, then
`GROUP BY` and `COUNT()`.

```sql
SELECT j.name, COUNT() FROM gkg.v58_gl_job AS j FINAL
WHERE startsWith(j.traversal_path,'1/9970/')
  AND j.status='failed' AND j.finished_at>='2026-04-10' AND j._deleted=false
GROUP BY j.name
```

`FINAL` deduplicates a `ReplacingMergeTree` to the latest version per
`(traversal_path, id)` by a streaming merge of pre-sorted parts. Key property:
**memory is bounded regardless of result cardinality** (no hash table). Cost
scales with the number of version-rows merged, which is read-amplified when the
table is fragmented into many small parts.

## Two semantics (the part that stays true)

Dropping `FINAL` and filtering raw version-rows is a different question, not a
cheaper approximation:

- **latest-state** (`FINAL`): does the entity's *current* version match?
- **ever-matched** (filter-first): did *any* version match?

These diverge for any **reversible** column. Proof on prod (`gl_job`
`id=14751794103`): `failed` at 19:36, `success` at 03:20 (latest).
`WHERE status='failed'` (no FINAL) counts it; `FINAL ... WHERE status='failed'`
does not. ClickHouse refuses to auto-push a `WHERE` below `FINAL` for exactly
this reason. A column is **version-stable** when its value is identical across
every version-row, so filter-first equals latest-state: sort-key columns
(`traversal_path`, `id`), immutable attributes (`created_at`, `*_id`), and
absorbing enum values (verified: `MergeRequest.state=merged`, 0/652127
un-merges; `Job.status` reverses on retry so is never stable).

## The alternatives considered

| Form | Idea | Latest-correct? |
|---|---|---|
| **K1** | filter in `WHERE`, `argMaxIfOrNull(col,_version,_deleted=false)` in `HAVING` | only for version-stable filter columns |
| **M** | candidate-prune `id IN (selective-set)` + `argMax(col,_version)` in `HAVING` | yes, any column |

Both resolve latest-version-per-id with `GROUP BY id`, so the hash table is
sized to the **result cardinality**.

## Why they were rejected (settled-parts measurements)

Authoritative, `log_comment`-tagged, prod v58, scope `1/9970/`, parts merged
(gl_job 168 parts / 4.4B rows; gl_merge_request 63 parts / 29M rows):

| case (result rows) | K1 | FINAL | M |
|---|---|---|---|
| merged MRs (524K) | 169ms / 279 MiB | **51ms / 91 MiB** | n/a |
| failed jobs (619K) | 1,331ms / 342 MiB (+16, ever-matched) | 3,052ms / 1.0 GiB | 5,590ms / 433 MiB |
| busiest project's jobs by status | **49,154ms / 20.45 GiB** | **1,492ms / 838 MiB** | n/a |

1. **K1 is an unbounded-memory footgun.** Its `GROUP BY id` hash table scales
   with result cardinality. On the busiest project (millions of jobs) that is
   20.45 GiB / 49s versus FINAL's 838 MiB / 1.5s. FINAL's streaming merge stays
   bounded; K1 does not. The deciding cardinality is unknowable at compile time,
   so K1 cannot be applied automatically without OOM risk on a shared cluster.
2. **K1 loses on small tables.** Merged MRs: FINAL 51ms vs K1 169ms. The merge
   is trivial when the scan is small; the grouping is pure overhead.
3. **M never wins.** 5,590ms > FINAL's 3,052ms: the candidate `id IN (...)` does
   not prune granules when ids are spread (read 283M = full table). Its only
   edge is memory, which FINAL already has.

## Decision

Keep `FINAL` as the single aggregation-lowering form. Do not build the K1/M
multi-strategy selector.

The earlier benchmark that motivated this work (FINAL at 16.5s, alternatives
4-16x faster) was measured while the v58 backfill had gl_job fragmented into
many small parts, read-amplifying the merge. On settled parts FINAL is
Pareto-optimal: fast (51ms-3s), bounded-memory, and exact in every case.

**If FINAL aggregations are slow, treat it as a part-fragmentation signal** and
fix it at the storage layer (merge/compaction settings, backfill pacing,
projections), not by swapping query forms. Benchmark aggregation forms only on a
well-merged cluster; FINAL's cost is part-state-dependent.

## If a guarded variant is ever revisited

K1 would need a reliable compile-time cardinality bound to be safe (it is only a
win for large, merge-dominated scans with a small, version-stable result set).
The `tp_count` aggregate projection (`uniq(id)` per `traversal_path`) is the
only cardinality estimate available, and it is per-namespace, not per-filter, so
it cannot bound K1's hash table for a filtered aggregation. Absent such a bound,
K1 stays out.
