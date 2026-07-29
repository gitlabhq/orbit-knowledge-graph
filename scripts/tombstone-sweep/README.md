# Tombstone sweep validation harness

Validates the `maintenance.table_cleanup` sweep against production data inside
the `gkg_probe` scratch database. Production tables are read, never written.

```bash
scripts/tombstone-sweep/run_tier.sh fast                    # every swept table, minutes each
scripts/tombstone-sweep/run_tier.sh full v85_gl_edge ...    # zero-copy clones, real volume
scripts/tombstone-sweep/collect_metrics.sh 2                # query_log metrics for the last 2h
```

Two tiers, both running the exact shipped statement shapes from
`table_cleanup.rs`:

- **fast** — a bounded slice of the table (`1/9970/` prefix, capped at 5M rows)
  plus synthetic scenarios injected into every table: 50 keys whose newest
  version becomes a tombstone (every row must go), 50 keys whose tombstone is
  superseded by a newer live row (nothing of theirs may go). Proves statement
  shape and row-exact accounting on every schema in under a minute per table.
- **full** — the whole table via zero-copy clone, shipped 7-day window. Proves
  cost and completion at real volume.

Every statement carries `log_comment='tsp:<tier>:<table>:<phase>'`. Collect
metrics **immediately after a run**: Cloud compute nodes recycle and take their
node-local `system.query_log` with them (a 4-hour-old run was already gone).

`chw` is the execution channel: SQL as `gkg_writer` from inside a dispatcher
pod, endpoint from the pod's mounted config, password never leaves the pod.
The Cloud console gateway caps statements at ~30s and cannot run any of this.

Results append to `$RESULTS` (default `/tmp/tombstone-sweep/results.jsonl`);
`run_tier.sh` skips tables that already have an `ok` for the tier, so a died
run resumes where it left off.

## Validation record (2026-07-29, production clones)

Fast tier: **37/37 tables pass** — every sort-key shape in the ontology, exact
`removed == expected` accounting, `still_matching = 0`, injected tombstoned
keys all swept, injected re-created keys all survive (the `_version DESC`
predicate at work).

Full tier (7-day window; `build`/`delete`/`settle` are wall seconds):

| table | rows | keys | removed | build | delete+settle | still_matching |
|---|---|---|---|---|---|---|
| `v85_gl_diff_edge` | 960M | 9,912,369 | 13,983,096 | 53 | 535 | 0 |
| `v85_gl_definition` ¹ | 2.21B | 13,601,215 | 27,638,284 | 248 | 922 | 0 |
| `v85_gl_sec_edge` ¹ | 3.50B | 2,380,337 | 3,369,854 | 68 | 1,500 | 0 |
| `v85_gl_edge` ² | 6.90B | 21,826,441 | ~73M | ~240 | ≤ 41 min | 0 |
| `v85_gl_code_edge` ¹ ³ | 9.23B | 73,923,278 | 183,543,220 | ~28 min | 1,636 | 0 |

¹ ran **concurrently** with the other two ¹ tables — all passed, and the
makespan of the three was the slowest table, not the sum.
² transport died mid-delete (finding 1); the mutation completed on its own,
verified by polling the remaining-count to zero.
³ build ran concurrently with two other sweeps.

Peak memory across every phase collected before the logs recycled: 3.26 GiB
(a fast-tier key build over a 293-part probe); counts carrying
multi-million-key `IN` sets: 2.78 GiB. All against an 8 GiB cap.

## What production testing established (beyond the earlier campaign)

1. **A silent synchronous statement dies at ~20 minutes.** The Cloud HTTP path
   drops an idle connection (curl exit 52). `lightweight_deletes_sync = 1` on
   a multi-billion-row delete and a long key build both hit it. The server
   keeps going: mutations are unaffected by client disconnect, and a
   `CREATE TABLE AS` writes nothing to the socket until it finishes, so the
   disconnect is never noticed. Consequence for the shipped task: submit the
   delete with `sync = 0` and treat the existing remaining-rows poll as the
   completion signal; keep long builds alive with
   `send_progress_in_http_headers = 1`.
2. **Large `IN`-set counts wedge in index analysis, unkillably.** At ~13M keys
   the verification count hung in primary-index analysis, where
   `max_execution_time` is never checked; `KILL QUERY` and dropping the table
   both failed to stop it. `use_index_for_in_with_subqueries_max_values =
   1000000` makes oversized sets skip index analysis and take a predictable
   filtered scan. The shipped verification query needs this cap.
3. **Cost is read-bound, so concurrency is the wall-clock lever.** Delete time
   tracks table rows scanned, not key count.
   `lightweight_delete_mode = 'lightweight_update'` (patch parts, 25.7+) was
   measured at 601s vs ~494s for the mutation path on the same table — it only
   saves writes, which were never the bottleneck (its one win: visibility is
   immediate, settle 3s vs minutes). Running the big tables concurrently is
   what collapses the nightly wall time from the ~2h sum to the slowest
   table (~25–45 min); the three-way concurrent run above is the evidence.
4. **Cross-database `CLONE AS` is broken for index-bearing tables** ("Tables
   have different secondary indices", 26.4). The documented two-step
   equivalent — `CREATE TABLE AS` + `ALTER TABLE … ATTACH PARTITION ALL FROM` —
   works and is what the harness uses.
5. **Replacing merges corrupt clone accounting.** Probe tables are
   `SharedReplacingMergeTree`; background merges collapse duplicate versions
   mid-run (24.2M rows on one probe), inflating the row delta. The harness
   freezes merge scheduling on probes
   (`max_bytes_to_merge_at_max_space_in_pool = 1`); mutations still run.

## Earlier findings that still hold

- `OPTIMIZE TABLE … FINAL CLEANUP` is a no-op on an unpartitioned table under
  continuous insert — ten consecutive "successful" nightly runs left 87% of
  `v85_gl_edge` untouched.
- An unscoped `IN`-subquery inside the `DELETE` left 13 parts at zero progress
  (a mutation re-evaluates its subquery once per part); materialising the key
  set first is what makes the delete tractable.
- `gkg_writer` has no `SELECT` on `system.mutations` (`Code: 497`), so
  completion must be confirmed against the data itself.
- `lightweight_deletes_sync = 2` waited 30+ minutes after the data was already
  applied.
