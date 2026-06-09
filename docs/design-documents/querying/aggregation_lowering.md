# Aggregation lowering: latest-state counts without FINAL

Status: proposed. Targets `crates/query-engine/compiler/src/passes/lower/`
(`single_node.rs`, `aggregation.rs`) and the ontology schema/loader.

## Problem

Count-by-state aggregations ("merged MRs per author", "failed jobs by name")
lower today to a single form: scan the table with `FINAL`, then `GROUP BY` and
`COUNT()`.

```sql
SELECT j.name AS job_name, COUNT() AS failure_count
FROM gkg.v58_gl_job AS j FINAL
WHERE startsWith(j.traversal_path, '1/9970/')
  AND j.status = 'failed' AND j.finished_at >= '2026-04-10' AND j._deleted = false
GROUP BY j.name ORDER BY failure_count DESC LIMIT 60
```

`FINAL` deduplicates a `ReplacingMergeTree` to the latest version per
`(traversal_path, id)` by a streaming merge of pre-sorted parts. It is
memory-light (no hash table) but slow on wide tables, because the merge reads
the fat `traversal_path` string for every version-row in scope. On
`gl_job` (269M rows, ~614K matches) this is 16s / 562 MiB on prod v58.

The merge is the cost. The goal is to keep `FINAL`'s **latest-state**
correctness while skipping the merge.

## Two semantics, and why they differ

Dropping `FINAL` and filtering raw version-rows is not "latest with a small
error". It is a different question:

- **latest-state** (`FINAL`): does the entity's *current* version match?
- **ever-matched** (filter-first, no `FINAL`): did *any* version match?

These diverge for any **reversible** column. Proof on prod (`gl_job`
`id=14751794103`):

| `_version` | status |
|---|---|
| 2026-06-08 19:36 | `failed` |
| 2026-06-09 03:20 | `success` (latest) |

`WHERE status='failed'` (no FINAL) counts it (1). `FINAL ... WHERE
status='failed'` does not (0). ClickHouse will not auto-push a `WHERE` below
`FINAL` for exactly this reason: a predicate on a non-sort-key column applied
before the merge can change which version wins. So "drop FINAL" is only correct
when the predicate cannot change across an entity's versions.

## The decision table

Three forms. The compiler picks per single-table node aggregation. All three are
latest-state-correct on the columns that matter; they differ in how the
`GROUP BY id` cardinality (the only memory cost) is bounded.

| Form | When | Shape | Cost (failed-jobs, prod v58) | Correctness |
|---|---|---|---|---|
| **K1** | every WHERE filter column is **version-stable** | filter in `WHERE`, `argMaxIfOrNull(col, _version, _deleted=false)` in `HAVING` | 1.0s / 568 MiB | exact |
| **M** | a filter column is **reversible** but selective | candidate-prune `id IN (selective-set)` + `argMax(col, _version)` in `HAVING` | 4.0s / 561 MiB | strict-latest, all columns |
| **FINAL** | nothing selective prunes the cardinality | today's form | 16.5s / 562 MiB | strict-latest |

Anti-pattern (never emit): `argMax` in `HAVING` with no prune groups every id in
scope and builds a 60 GiB hash table (10.4s / 60.5 GiB). The prune is mandatory.

### Why the prune is the whole trick

`argMax`/`argMaxIfOrNull` resolve latest-version-per-id via `GROUP BY id`. The
hash table holds one entry per grouped id. Earlier work concluded "argMax always
costs 25-70 GiB and loses to FINAL", but that measured `GROUP BY id` over the
*whole table*. The fix is to bound the **group cardinality**, not just the scan:

- **K1** filters in the `WHERE`, so only the ~614K matching ids are grouped.
  568 MiB, not 60 GiB.
- **M** filters via a candidate `id IN (SELECT id ... <selective filter>)`,
  which bounds the grouped ids to the candidate set while the `HAVING` still
  reads all versions of those ids (so `argMax` sees the true latest).

### K1 (version-stable filter, exact)

```sql
SELECT grp, count() FROM (
  SELECT argMaxIfOrNull(grp_col, _version, _deleted = false) AS grp
  FROM gkg.v58_gl_merge_request
  WHERE startsWith(traversal_path, '1/9970/') AND state = 'merged'   -- prune
  GROUP BY id
  HAVING isNotNull(argMaxIfOrNull(id, _version, _deleted = false))   -- drop all-deleted
     AND argMaxIfOrNull(state, _version, _deleted = false) = 'merged'
) GROUP BY grp
```

The `WHERE` filter prunes scan and group cardinality. It is exact **iff** the
filtered column is version-stable: filter-first then sees the same rows the
latest-state query would. The `argMaxIfOrNull(..., _deleted=false)` in the
`HAVING` is the correct deletion guard: it resolves the latest non-deleted
version, so all-deleted entities drop out. Measured: merged MRs = 523,881,
byte-identical to FINAL, 54ms.

### M (reversible filter, strict-latest, universal)

```sql
SELECT grp, count() FROM (
  SELECT id FROM gkg.v58_gl_job
  WHERE startsWith(traversal_path, '1/9970/')
    AND id IN (SELECT id FROM gkg.v58_gl_job
               WHERE startsWith(traversal_path, '1/9970/')
                 AND status = 'failed' AND finished_at >= '2026-04-10')   -- candidate prune
  GROUP BY id
  HAVING argMax(status,      _version) = 'failed'      -- TRUE latest version
     AND argMax(_deleted,    _version) = false
     AND argMax(finished_at, _version) >= '2026-04-10'
) GROUP BY grp
```

Nothing is filtered pre-group, so the `HAVING` sees every version of each
candidate and `argMax(status, _version)` is the genuine latest. A
`failed -> success` job is in the candidate set but excluded by the `HAVING`
(exactly what FINAL does). `argMax` (not `argMaxIf`) on each dedup column gives
FINAL-exact semantics. Measured: 614,461, matches FINAL within snapshot churn,
4.0s / 561 MiB.

## What decides K1 vs M: version-stability, declared in the ontology

A column is **version-stable** when its value is identical across every
version-row of an entity, so dedup-then-filter equals filter-then-dedup. Two
sources, both new ontology declarations validated against prod:

1. **`terminal_values`** on an enum field: values that, once reached, never
   change. Verified: `MergeRequest.state = merged` is absorbing (0 of 652,127
   MRs ever un-merged). `closed` is reopenable, `Job.status = failed` reverses
   on retry, so neither is terminal.

   ```yaml
   state:
     type: enum
     values: {1: opened, 2: closed, 3: merged, 4: locked}
     terminal_values: [merged]
   ```

2. **`immutable: true`** on a field: never changes after creation (`id`, `iid`,
   `created_at`, and the foreign-key `*_id` columns). This is the broader win:
   it unlocks K1 for "by author", "by project", "created per month" with no
   state filter at all.

   ```yaml
   author_id: {immutable: true}
   ```

The compiler rule: if every WHERE filter column is version-stable (terminal
value or immutable) then K1 (exact); else if a filter is selective then M
(strict); else FINAL.

`terminal_values` and `immutable` go in `config/schemas/ontology.schema.json`,
are validated there, and are carried onto the field specs by the ontology
loader. Sort-key columns (`traversal_path`, `id`) are implicitly version-stable.

## Deletion is handled, not assumed away

`_deleted` is never version-stable: a tombstone is a higher-`_version` row with
`_deleted = true`. It is, however, **monotonic** (verified: 0 resurrections
across MR / pipeline / work_item in scope). Both K1 and M honor it through the
`HAVING`, not the `WHERE`:

- K1: `argMaxIfOrNull(col, _version, _deleted = false)` returns the latest
  non-deleted version, NULL if all-deleted; the `isNotNull(...)` guard drops
  all-deleted entities.
- M: `argMax(_deleted, _version) = false` keeps only entities whose latest
  version is live.

This is strictly better than a `WHERE _deleted = false` (which would resurrect
a now-deleted entity via its old live rows) or a `NOT IN (deleted ids)`
anti-set.

## Where it lands in code

- `emit_single_node` (`single_node.rs`) currently emits
  `TableRef::scan_final(table, alias)` unconditionally. It gains a strategy
  selector: inspect the plan's node predicates, classify filter columns against
  the ontology version-stability flags, and emit the K1 / M / FINAL subquery
  shape accordingly.
- `build_aggregation` / `build_agg_expr` (`aggregation.rs`) already emit `-If`
  combinators when `if_cond` is set (the edge LIMIT BY path). The node-only path
  needs the nested `SELECT ... GROUP BY id HAVING ...` inner query that K1/M
  introduce; the outer `GROUP BY grp` aggregation is unchanged.
- Ontology: `terminal_values` (array, per enum field) and `immutable` (bool, per
  field) added to the schema, validated, and threaded through the loader to the
  field metadata the compiler reads.

The selector defaults to FINAL whenever classification is uncertain, so the
change is correctness-safe by construction: a missing or wrong ontology flag
costs speed, never correctness.

## Acceptance tests

| Case | Form | Expected | Source |
|---|---|---|---|
| merged MRs in `1/9970/` | K1 | 523,881, byte-identical to FINAL | prod v58 |
| failed jobs in `1/9970/` since 2026-04-10 | M | 614,461, matches FINAL within churn | prod v58 |
| failed jobs, K1 (wrong: status is reversible) | K1 | over-counts vs FINAL (+182) | regression guard: must NOT pick K1 here |

The third row is the guard: a reversible column in the `WHERE` must force M, not
K1. The corpus validation (`scripts/devtools/agg_lowering_coverage.py`) flags
these.

## Coverage

Populated by `scripts/devtools/agg_lowering_coverage.py` over the 216 corpus
aggregations: how many land in K1 / M / FINAL under a conservative
(sort-key + creation fields + MR merged) versus FK-immutable map, and the list
of reversible-WHERE traps. The gap between the two maps is the value of marking
FK columns immutable.

<!-- coverage numbers inserted after the validation run completes -->

## Limits

- Applies to single-table **node** aggregations. Edge/multi-hop aggregations
  already use `-If` combinators on a bounded chain and are out of scope.
- K1's exactness depends on the ontology flag being correct. The flags are
  verified against prod data before they are trusted (see the invariant checks
  above); a `build.rs`-style or CI assertion that re-verifies absorbing/immutable
  claims against a sample is a candidate follow-up.
- M reads more than K1 (it scans all versions of the candidates) so it is the
  4s tier, not the 1s tier. It is still 4x faster than FINAL and equally light.
