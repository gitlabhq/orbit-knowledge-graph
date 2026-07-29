# Tombstone sweep validation harness

Validates the `maintenance.table_cleanup` sweep against production data by
cloning a graph table (zero-copy `CLONE AS`), running the real statements on the
clone, then dropping it. Production tables are never written to.

```bash
RESULTS=/tmp/sweep.txt scripts/tombstone-sweep/sweep_one_table.sh v85_gl_edge
```

`chw` is the execution channel. It runs SQL as `gkg_writer` from inside a
dispatcher pod, reading the endpoint from the pod's mounted config and the
password from `/etc/secrets/graph/password`, so no credential reaches the
developer machine. The ClickHouse Cloud console gateway caps statements at ~30
seconds and therefore cannot run these statements at all.

## Why the sweep needs a helper table

Each alternative was measured against production and rejected:

| approach | outcome |
|---|---|
| `OPTIMIZE TABLE … FINAL CLEANUP` | no-op on an unpartitioned table under continuous insert; 10 consecutive daily runs reported success and left 87% of `v85_gl_edge` untouched |
| unscoped subquery inside the `DELETE` | 13 parts at zero progress after 20 min — a mutation re-evaluates its subquery once per part |
| per-namespace scoped `DELETE` | progresses, but mutations serialise at ~1 per 2 min, i.e. thousands of statements per night |
| `min_age_to_force_merge_seconds` | collapses tombstones correctly and needs no helper table, but the CLEANUP variant requires whole-partition merges that never fire here, and a 25 GiB force-merge never landed |

## Permission constraints this harness established

- `gkg_writer` has **no** `SELECT` on `system.mutations` (`Code: 497`), so
  completion must be confirmed by counting rows that still match the predicate.
- `CREATE DATABASE` is granted for `gkg` only, so the helper table lives in
  `gkg`. It is named to avoid the `v<version>_*` glob that schema GC uses.
- `lightweight_deletes_sync = 2` waits for every replica and added 30+ minutes
  after the data was already applied; `sync = 1` plus the row-count check is the
  combination that ships.

## Measured results (7-day window, `sync = 1`)

| table | rows | keys | build | delete | removed | still_matching |
|---|---|---|---|---|---|---|
| `v85_gl_ci_edge` | 12.41B | 21,954,193 | 61 s | ~13 min | 32,135,655 | 0 |
| `v85_gl_job` | 1.81B | 1,908,794 | 47 s | 494 s | 3,795,149 | 0 |
| `v85_gl_finding` | 1.57B | 653,791 | 17 s | 406 s | 954,906 | 0 |
| `v85_gl_merge_request_diff_file` | 840M | 6,437,970 | 14 s | 300 s | 9,324,446 | not run |
| `v85_gl_stage` | 717M | 160,092 | 8 s | 210 s | 309,016 | not run |
| `v85_gl_imported_symbol` | 619M | 1,358,057 | 77 s | 206 s | 3,202,931 | not run |
| `v85_gl_note` | 62.7M | 46,119 | 3 s | 63 s | 55,160 | not run |
| `v85_gl_merge_request` | 27.3M | 15,153 | — | 21 s | 21,176 | 0 |
| `v85_gl_commit` | 20.6M | 11,422 | 3 s | 52 s | 14,634 | not run |
| `v85_gl_project` | 763K | 231 | 2 s | 19 s | 413 | 0 |

Peak memory never exceeded 2.53 GiB against an 8 GiB cap.

`v85_gl_merge_request` also has a full row-level accounting: 15,153 keys covered
16,164 tombstone rows plus 5,012 superseded live rows, exactly 21,176 rows were
removed, and all 26,908,910 keys whose newest version is live were untouched.

## Outstanding

`v85_gl_edge` (6.82B) and `v85_gl_sec_edge` (3.47B) were swept and the row
deltas observed (~41,800,000 and 3,001,676), but the harness died before
recording build and delete durations and neither got the `still_matching` check.
Re-run both with `sweep_one_table.sh` to close this.

Earlier runs of an async variant (poll instead of `sync = 1`) covered
`v85_gl_code_edge` (8.87B, 77,974,551 removed), `v85_gl_definition` (2.09B,
13,606,848) and `v85_gl_diff_edge` (940M, 8,908,369); those numbers are not
directly comparable to the table above.
