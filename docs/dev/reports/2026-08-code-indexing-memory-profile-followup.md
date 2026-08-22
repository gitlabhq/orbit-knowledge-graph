# Code indexing memory: what the improved harness measures

Follow-up to [the first profile](2026-08-code-indexing-memory-profile.md). That
report described the pipeline as it stood before
`perf/reduce-code-indexing-peak-memory`. This one records what changed in the
measurement itself, and the numbers the new signals produced on the same corpora
with those optimizations already in.

All runs: M3 Max, `--profile memprofile`, ClickHouse in Colima, per-file CPU
budgets at production defaults unless stated.

## What the harness could not see before

| Gap | Consequence | Now |
|---|---|---|
| Peaks came only from a sampler | The sampler's real cadence is 30 ms, not the nominal 50 Hz, and it reached 112 ms under load. A spike narrower than the gap was invisible. | The allocator wrapper keeps its own high-water mark, refreshed whenever a thread has moved 64 KiB. `ri_lifetime_max_phys_footprint` and mimalloc's `peak_commit` are read directly. The wrapper's mark is sampling-independent but not exact: it can miss a peak assembled from sub-threshold contributions across threads, bounded by threads times 64 KiB, about 5 MiB at the thread counts these runs reach. |
| Tree-sitter allocates through C `malloc` | Parse trees and their scratch buffers were counted by nothing: not the tracking allocator, not dhat. | The profiler points tree-sitter at the Rust allocator. On Elasticsearch that is 74M allocations, 35% of all allocation traffic, previously absent. |
| No size-class accounting | The gap between requested bytes and footprint was attributed to rounding and purge lag by conjecture. | The `good-size` feature counts `mi_good_size` of every live block. |
| One task per process | Neither the stale sweep, which only runs when a checkpoint exists, nor whether memory comes back between tasks. | `--repeat N` indexes with an increasing task id and records settled memory per round. `--flush-between-rounds` separates pooled rows from retained data. |
| The write buffer | The largest holder after conversion reported nothing. | A probe on pending plus in-flight bytes, deduplicated on the underlying allocation. |
| The tag cache | Alive for the whole edge batch build, which is the peak. | Probed. |
| petgraph capacity | Reported as bytes, so a mis-sized reservation looked like real data. | Node and edge capacity emitted next to the counts. |
| Hashbrown maps | `map_bytes` charged `capacity` buckets; hashbrown allocates `next_pow2(capacity * 8/7)`. | Corrected; maps were understated by 14% to 2x. |
| petgraph slot size | The probe charged weight plus links without padding, so a five-byte edge weight counted 21 bytes against a real 24. | Corrected; edge arrays were understated by 12%. |
| Peak phase label | Every report said the peak happened in `parse`. It happens in Arrow conversion. | Phases derive from the probes. |
| Comparisons | `memprofile-compare.py` failed on row counts and wall clock only, so a candidate that doubled peak memory exited 0. | Fails on peak memory too, takes N runs per side and compares medians, and refuses to compare runs that aborted a different number of files. |

Cost of all of it: no measurable wall-clock change (4,648 ms against 4,651 ms on
Django, three runs). The `good-size` mode adds about 2% and is off by default.

## Where memory goes, with the current optimizations in

| | Elasticsearch | linux | gitlab | VS Code |
|---|---:|---:|---:|---:|
| peak live heap | 1,114 MiB | 1,471 MiB | 868 MiB | 859 MiB |
| peak footprint | 1,349 MiB | 1,858 MiB | 1,163 MiB | 1,194 MiB |
| peak commit (mimalloc) | 2,075 MiB | 2,642 MiB | 1,618 MiB | 1,602 MiB |
| write buffer at its peak | 572 MiB | 643 MiB | 319 MiB | 163 MiB |
| write buffer as a share of live heap then | 89% | 87% | 92% | 91% |
| tag cache | 116 MiB | 237 MiB | 56 MiB | 67 MiB |
| petgraph capacity slop | 124 MiB | 127 MiB | 36 MiB | 10 MiB |
| size-class rounding | 1.0% | 1.2% | | |

Three of these are new.

**The write buffer holds almost the whole live heap once conversion ends.** On
Elasticsearch it peaks at 572 MiB at t=17.2 s, when the process's entire live heap
is 641 MiB, so 89% of what the process is holding is rows waiting on ClickHouse.
The share is 87% to 92% on every corpus.
Rows stay resident from submit until the insert is acknowledged.

Measuring this correctly took three attempts, which is worth recording because the
first two produced numbers that looked plausible. The pipeline submits slices of
one batch, and slicing a list array narrows only the parent while the child arrays
stay whole, so `get_array_memory_size` and `get_slice_memory_size` both charge the
shared children once per slice: 1,787 MiB, or 279% of the live heap. Charging each
slice only its own portion fixes that but then reads low once some slices drain,
because the survivors still pin the whole parent. What the probe does now is
deduplicate on the start of each allocation and charge its capacity, across pending
and in-flight parts together. An independent check agrees: settled live heap before
the flush minus after it is about 600 MiB.

**petgraph is allocated at roughly twice what it uses.** Elasticsearch: 828,736
nodes in 1,640,364 slots, 4,416,851 edges in 8,739,266 slots. `reserve_for` sizes
the node array from files, definitions and imports and omits directories, and
`reserve_exact_edges` sizes the edge array from the phase-2 count before phase 3
adds more. One push past an exact reservation doubles the array.

The reservations were added by the optimization commit, and on this evidence they
cost more than they save. Capacity for the two arrays, in MiB:

| | with the reservations | with no reservation at all | reserving the true total |
|---|---:|---:|---:|
| Elasticsearch nodes | 50.1 | 32.0 | 25.3 |
| Elasticsearch edges | 200.0 | 192.0 | 101.1 |
| linux nodes | 102.2 | 64.0 | 51.2 |
| linux edges | 152.0 | 96.0 | 76.0 |

Growing by doubling from nothing lands on the next power of two, which is closer
to the count than a reservation that gets doubled.

Deleting the reservations was tried first and is the wrong fix: it restores
amortized doubling during graph build, which cost 3 to 6% of indexing time on
three repositories and left gitlab 4% worse on memory. Keeping both reservations
and shrinking the arrays to fit in `release_resolution_state`, which already runs
immediately before conversion, gets exact capacity where it is paid for without
touching the build. Measured against the merge request as it stood, that is
-11.1% peak live heap on Elasticsearch and -8.6% on linux, with wall clock
unchanged or slightly better.

**The tag cache is 116-237 MiB and lives across the peak.** `build_node_tags`
allocates two `String`s per tag per node, for tags whose cardinality the ontology
fixes at a handful of values.

**Size-class rounding is not where the footprint gap is.** 1.0% on Elasticsearch,
1.2% on linux, 5.8% on Django. That accounts for only part of the 1,114 to 1,349
MiB spread; the rest is page retention and purge lag plus two things still
unmeasured, the padding mimalloc adds for 64-byte-aligned Arrow buffers and the
transient where a growing allocation holds its old and new blocks at once. The
`realloc-pessimistic` mode exists to measure the second and has not been run.

## Concurrency composes linearly at the peak

Two lanes indexing the same repository, so their conversions land together:

| | one lane | two aligned lanes |
|---|---:|---:|
| peak live heap | 1,114 MiB | 2,227 MiB (2.00x) |
| peak footprint | 1,459 MiB | 2,674 MiB (1.83x) |
| peak commit | 2,038 MiB | 4,202 MiB (2.06x) |
| write buffer at its peak | 572 MiB | 1,144 MiB (2.00x) |
| wall clock | 25.0 s | 37.5 s |

The first report's two-lane number (2,904 MiB combined against 4,379 sum-of-solos)
came from two repositories whose peaks happened to miss each other. Aligned, they
do not: live heap is 2.00x, and the write buffer 2.00x. Dividing a pod's budget by
the lane count is the right model for live heap. Footprint composes at 1.83x and
commit at 2.06x, both close enough to linear that run-to-run spread (footprint
varies 5% between runs of the same corpus) covers the difference.

A single linux lane peaks at 2,555 to 2,648 MiB of commit across three runs,
against a `WORKER_MEMORY_BUDGET_BYTES` of 2,560 MiB. Two of the three are over it.

## Nothing leaks, and the stale sweep is cheap

Three rounds of Elasticsearch in one process, each with the next task id so
rounds two and three re-index and take the stale-sweep path:

| round | wall clock | settled live | settled footprint | settled RSS | commit |
|---:|---:|---:|---:|---:|---:|
| 1 | 19.9 s | 2.3 MiB | 858 MiB | 1,751 MiB | 2,022 MiB |
| 2 | 20.4 s | 3.5 MiB | 559 MiB | 2,331 MiB | 2,466 MiB |
| 3 | 20.4 s | 3.9 MiB | 277 MiB | 2,370 MiB | 2,466 MiB |

Live heap returns to a few MiB every round, so the pipeline gives everything back.
Footprint falls as purge catches up. RSS and commit plateau near the peak rather
than climbing. Re-indexing costs 0.5 s and does not raise the peak, so the stale
sweep is not the memory risk it looked like from the code.

Without `--flush-between-rounds` the settled live heap climbs by 33 MiB a round on
Django. That is rows pooled in the write buffer, not a leak, and it is what
production does between tasks.

linux behaves the same way in live heap and worse in the numbers a cgroup sees:

| round | wall clock | settled live | settled footprint | settled RSS | commit |
|---:|---:|---:|---:|---:|---:|
| 1 | 48.1 s | 5.6 MiB | 947 MiB | 1,086 MiB | 2,612 MiB |
| 2 | 47.2 s | 7.0 MiB | 519 MiB | 2,561 MiB | 2,803 MiB |
| 3 | 45.7 s | 7.7 MiB | 557 MiB | 2,721 MiB | 2,839 MiB |

Live heap is flat at single-digit MiB, but RSS and commit settle at 2,721 and
2,839 MiB, both above the 2,560 MiB per-lane budget, and they get there by
ratcheting over the first two rounds rather than by any single peak. A pod
indexing linux-sized repositories back to back holds that much whether or not it
is doing anything.

## mimalloc `secure` is not a penalty

`orbit-server` builds mimalloc with `secure`; the profiler did not. Measured on
Elasticsearch: same peak live heap, footprint 1,330 against 1,424 MiB, commit
1,729 against 2,056 MiB, wall clock 2.2% slower. Past numbers were not biased by
the mismatch. `mise memprofile:run:secure` runs under the production
configuration.

## What the allocator itself reports

`mi_stats_get_json` now lands in `summary.json`. At the end of a single-lane run:

| | Elasticsearch | linux |
|---|---:|---:|
| address space reserved | 3,077 MiB | 3,077 MiB |
| committed, peak | 2,048 MiB | 2,648 MiB |
| purged over the run | 2,043 MiB | 3,714 MiB |
| abandoned pages, peak | 6,747 | 8,514 |
| threads, peak | 52 | 61 |

The pipeline builds a fresh rayon pool per language family and drops it, so worker
threads are created and destroyed throughout the run. A thread that dies while
holding partly used pages abandons them, and an abandoned page stays committed
until another thread reclaims it. Thousands of them at peak is a candidate
explanation for the 600 to 1,200 MiB spread between live heap and commit, and it
is testable without touching the pipeline.

## Allocator tuning measured and rejected

Elasticsearch, one lane, against the 24.9 s / 1,349 MiB footprint baseline:

| setting | footprint | commit | wall clock |
|---|---:|---:|---:|
| `MIMALLOC_PURGE_DELAY=0` | 1,268 MiB (-6.0%) | 1,992 MiB (-4.0%) | 27.5 s (+10.6%) |
| `MIMALLOC_ARENA_EAGER_COMMIT=0` | 1,423 MiB (+5.5%) | 2,053 MiB (-1.1%) | 26.6 s (+6.8%) |

Returning pages immediately buys 6% of footprint for 11% of wall clock, which is
the wrong trade at a per-lane budget that peak live heap, not settled footprint,
is the binding constraint on. Eager commit off is worse on both.

## The probes do not change what gets written

Django with the per-file budgets disabled, the same binary with and without the
new probes, into two databases: all 40 tables byte-identical across 254,571 rows,
every column except the two run timestamps. Every probe sits behind
`memprobe::enabled()`, so a production build walks nothing.

## Reproducing

```shell
mise memprofile:ch
mise memprofile:corpus
mise memprofile:run -- --project-id 100001 --label es --settle-secs 0
mise memprofile:report .memprofile/runs/es
mise memprofile:stages .memprofile/runs/es
```

Steady state and re-indexing: `--repeat 3 --settle-secs 3 --flush-between-rounds`.
Aligned lanes: `--project-id 100001 --project-id 100001 --indexing-lanes 2`.
Size-class rounding: `mise memprofile:run:goodsize`. Production allocator:
`mise memprofile:run:secure`.
