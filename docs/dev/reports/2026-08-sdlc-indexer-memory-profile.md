# SDLC indexer backfill peak memory, August 2026

Four of the eight `gkg-indexer-sdlc` pods in `orbit-prd` were OOM-killed (exit 137) between 05:55
and 07:08 UTC on 2026-08-26 while a full SDLC backfill ran. The pool's working set went from a
0.44 GiB steady state to 31.74 GiB against its 32 GiB limit.

A backfill saturates all 20 of the pool's SDLC handler slots. The peak is dominated by the page
budget those slots hold, and most of it turns out to be inherent to the current design rather than
waste: at production shape the peak tracks `slots x page size` and no small code change moves it.
Two places are exceptions, and both are measured below. Every candidate was built on its own and
measured against the same baseline, so each number is attributable to one change.

## What production did

| Fact | Value | Source |
|---|---|---|
| Pods OOM-killed | 4 of 8, exit 137, 05:55 to 07:08 UTC 2026-08-26 | pod `lastState.terminated` |
| Restarts in the window | 5 | `kubernetes.io/container/restart_count` |
| Same-time-of-day spike the previous day | 32 restarts at 06:37 UTC 2026-08-25 | same |
| Peak working set | 31.74 GiB at 06:37 UTC | `memory/used_bytes`, `non-evictable`, per-pod max |
| Steady state outside the window | 0.44 to 0.90 GiB | same |
| Limit / request / replicas | 32 GiB / 8 GiB / 8 | `argocd-apps` values |
| Version | 0.106.2 | deployment image |

The pool has no headroom during a backfill by construction. `datalake_batch_size` is unset in config
and derives from the container memory limit, which at 32 GiB is exactly 500,000 rows per page. The
pool runs 20 concurrent SDLC slots. A backfill has no parent checkpoint, so its watermark window is
`(epoch, target]` and every namespaced pipeline has its whole history in the window at once, which
is the state that fills all 20 slots.

## Reproduction

`crates/sdlc-profiler` seeds a synthetic siphon datalake in ClickHouse (`numbers()`-generated rows,
so seeding never runs through the profiled process), creates the graph schema from the ontology, then
runs the real SDLC handlers under the real `WorkerPool` and writes transformed rows to ClickHouse. It
reproduces `Engine::run_handlers` directly rather than standing up NATS, because that fan-out is what
sets the peak. See [the runbook](../runbooks/sdlc_memory_profiling.md).

The profiler builds with mimalloc `secure`, which is what `orbit-server` ships, so the gap between
requested bytes and footprint is production's allocator overhead and not a cheaper default's.

Measured on a 32 vCPU / 256 GB Linux VM. Linux matters: `RssAnon` drops decommitted pages that macOS
keeps in RSS, so a macOS run is not comparable to a pod's working set.

## Fidelity of the synthetic load

The synthetic datalake reproduces the production envelope rather than production data:

| Property | Production | Harness |
|---|---|---|
| Rows per page | 500,000 | 500,000 |
| Concurrent SDLC slots | 20 | 20 |
| Entity handlers per namespace message | 46 | 46 |
| Namespaces swept concurrently | all enabled | 2 |
| Rows per entity table per namespace | varies, millions on large namespaces | 1,000,000 |
| Note body / description width | real markdown | 600 B / 1200 B mean, incompressible |

Because the peak is set by how much a slot holds, not by how many pages a pipeline eventually walks,
two full pages per pipeline is enough to reach the steady-state peak. It is reached, and it is 21 GiB
against production's 32 GiB limit, on rows narrower than production's.

Four things in the seeded data are shaped deliberately, because the pipelines would otherwise take a
cheap path and the measurement would understate the peak: traversal paths agree with the two
traversal-path tables so the namespaced extracts match rows; system-note bodies carry a GFM reference
so the note parser resolves references instead of finding none; `system_note_metadata.action` is drawn
from the actions the parser dispatches on rather than random strings it logs and drops; and half of
`siphon_notes` is system notes so both the `Note` and `SystemNote` pipelines get a full-width page.

## Measurement hygiene

Three things silently invalidate a comparison here, all found by hitting them.

**A retained graph database poisons the next run.** A production-shape run writes about 198M graph
rows. Left in place, it merges them in the background, and the next arm then fails with ClickHouse
server memory errors rather than indexer ones. The arm does less work, so its peak looks better and
its output legitimately differs. The first comparison run reported an output mismatch for exactly
this reason. Every run now gets an empty graph database and drops it before the next run starts; the
baseline's fingerprints are dumped to a file so its database can be dropped too. Any arm with a
non-empty `failures` list is discarded.

**Pre-merge row counts are not an output check.** Graph tables are `ReplacingMergeTree`, so two runs
that wrote identical rows report different counts depending on merge progress. Output equivalence is
a per-table fingerprint read `FINAL`: row count, additive checksum and XOR checksum of per-row
hashes, over every column except the three that are wall-clock at index time (`_version`, and
`indexed_at` and `watermark` on the checkpoint).

**The peak has a run-to-run spread of about 10% at production shape**, because 46 pipelines race for
20 slots and which of them are simultaneously mid-page when the peak lands is a scheduling accident.
Two repeats there resolve nothing smaller than that, so per-change attribution belongs in
single-pipeline runs, where the spread falls to a few percent and one baseline reproduced its peak to
the second decimal. Arms are also interleaved one round at a time: the first sweep ran each arm's
repeats consecutively and reported wall-clock gains of 13 to 14% for several arms, which turned out
to be an ordering artifact. Re-running the baseline after that sweep put it at 275 s against the
285 s it measured first, accounting for the whole effect.

## Results

Four things came out of this: one config lever, one large code win on a specific path, one small
code win, and a null result for the rest.

**The page size buys the same memory as the concurrency, at under half the throughput cost.** The
peak tracks `slots x page size`, so halving either cuts it by the same amount. What differs is what
each costs:

| Shape | Peak footprint | Namespace phase |
|---|---:|---:|
| 500k pages, 20 slots (production) | 19.65 GiB | 281 s |
| 250k pages, 20 slots | 11.20 GiB (-43%) | 308 s (+9.6%) |
| 500k pages, 10 slots | 12.23 GiB (-41%) | 342 s (+24%) |

The two page sizes were measured paired and alternated, n=4 each, so the comparison is not confounded
by drift. The slot-count row is a separate n=2 set on the same datalake. The first 500k run of the
paired set ran while ClickHouse was still merging a reseeded table and reads high (22.21 GiB, 389 s);
dropping it leaves the peak conclusion unchanged at -43% and moves the throughput cost to +12%.

Output is byte-identical across all three shapes: the per-table fingerprints of the 250k-page and
10-slot runs match the 500k-page run exactly. Page size only changes how the same rows are chunked
into requests.

Halving the page is not free, and an earlier unpaired estimate of its cost (+3.6%) was too
optimistic. At +9.6% it is still the better of the two levers by a factor of about 2.5, and unlike
adding replicas it costs nothing in capacity.

**The page drop works, on one pipeline out of forty-six.** Releasing the extracted page before the
writes has no effect on a plan whose transform is a DataFusion projection, because a projection over
a `MemTable` hands back the same `Arc`-backed column buffers, so the output keeps the input alive.
It has a large effect on the one plan whose transform builds independent arrays:

| Metric | `--only Note` (DataFusion) | `--only SystemNote` (Rust transform) |
|---|---:|---:|
| Peak footprint | +2.1% | **-37.2%** |
| Peak live | -0.6% | **-46.2%** |

Run-to-run spread on those measurements is 1 to 3%, so the second column is real. This is why the
change measures as nothing in aggregate and should still ship: it is four lines, and `SystemNote`
runs for every namespace.

**Borrowing the note text is a throughput change, not a memory change.** On `SystemNote` it removes
half the allocations the pipeline makes and 14% of its wall clock, and leaves the peak alone:

| Metric | Baseline | Borrowed |
|---|---:|---:|
| Allocation count | 20M | 10M (-48.6%) |
| Allocated bytes | 9 GiB | 8 GiB (-12.6%) |
| Namespace phase | 12 s | 10 s (-14.4%) |
| Peak footprint | 1.06 GiB | 1.06 GiB (-0.3%) |

**Splitting the page budget across a partitioned initial load is the one large peak win.** On a
backfill only, a plan that probes above the partition threshold runs one paging loop per key range
concurrently under the single worker slot the handler holds, and each range kept the whole page
budget. Measured on `Job` alone with five partitions over 30M rows:

| Metric | Baseline | Split |
|---|---:|---:|
| Peak footprint | 6.21 GiB | 2.19 GiB (-64.8%) |
| Peak live | 4.70 GiB | 0.93 GiB (-80.2%) |
| Namespace phase | 291 s | 334 s (+14.7%) |
| Graph rows | 150.0M | 150.0M |

Both baseline runs reported 6.21 GiB to the same two decimals, so the reduction is unambiguous. So is
the cost: the share lands on the floor at 100k rows, which is five times as many page round trips.
That is a real throughput regression on this path, accepted because the path is the one that
OOM-kills the pod today and a kill costs the whole run, not 15% of it.


#### What was measured and dropped

Three of the six candidates were built, measured on the pipeline they target, and dropped. Attributed
runs restrict the sweep to one pipeline, which removes the scheduling variance and takes the noise
floor to 1 to 4%.

| Change | Where measured | Peak | Allocated bytes | Verdict |
|---|---|---:|---:|---|
| Plan transform strings as `Utf8` | MergeRequest, n=3, spread 1.1% | +2.2% | +1.2% | dropped, marginally worse |
| Skip the redundant transport LZ4 | Note, n=3, spread 4.0% | +1.9% | -6.4% | dropped, no peak effect |
| Zero-copy network chunk hand-off | Note, n=3, spread 4.0% | +5.0% | -5.1% | dropped, no peak effect |

The `Utf8` change is a genuine regression, small but outside its own noise floor, and the predicted
saving (a few MB per batch of tag columns) was never going to register. The other two do reduce
allocation traffic reproducibly, by removing a redundant second LZ4 pass over every inserted byte and
a copy of every decoded network chunk. Neither moves the peak, which is what this change set is for,
so they are left out rather than bundled in. The compression one has a good first-principles case on
CPU grounds and a wider blast radius (it touches every insert, including code indexing), so it
deserves evaluating on its own terms rather than riding along here.


#### What the aggregate looks like, and why it is not the headline

At production shape with every change applied, the peak does not move outside its own noise, and the
allocation traffic does:

| Metric | Config A (n=2) | Drift control (n=3) |
|---|---:|---:|
| Peak footprint | -5.6% | +1.5% |
| Allocated bytes | -4.5% | -5.5% |
| Allocation count | -3.0% | -3.4% |
| Namespace phase | -1.8% | -1.1% |

Two independent estimates of the peak effect straddle zero, so the honest reading is that the stack
does not measurably change peak residency at this shape. The allocation reduction reproduces in both
sets, and throughput is unchanged.

The reason is structural rather than a failure of the changes. Residency during the write and
read-ahead overlap is the transformed page plus the next extracted page, and for the 45 DataFusion
plans the transformed page shares its buffers with the input. Two pages per slot is what the design
costs; removing it needs the streaming restructure proposed in #798, not a small diff.

#### Full tables


#### Production shape: 500k pages, 20 slots, 2 namespaces

| metric | baseline (n=2) | pagedrop (n=2) | partsplit (n=2) | nolz4 (n=2) | zerocopy (n=2) | utf8 (n=2) | noteborrow (n=2) | all (n=2) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| peak footprint GiB | 20.31 | 20.94 (+3.1%) | 20.39 (+0.4%) | 21.31 (+4.9%) | 20.49 (+0.9%) | 19.92 (-1.9%) | 19.80 (-2.5%) | 19.18 (-5.6%) |
| peak live GiB | 16.89 | 17.40 (+3.0%) | 17.00 (+0.7%) | 17.89 (+5.9%) | 17.10 (+1.3%) | 16.47 (-2.5%) | 16.38 (-3.0%) | 15.96 (-5.5%) |
| allocs M | 854 | 856 (+0.3%) | 854 (+0.1%) | 855 (+0.2%) | 854 (+0.1%) | 834 (-2.3%) | 846 (-0.9%) | 828 (-3.0%) |
| alloc total GiB | 894 | 900 (+0.7%) | 897 (+0.4%) | 866 (-3.1%) | 873 (-2.4%) | 905 (+1.2%) | 898 (+0.5%) | 854 (-4.5%) |
| namespace phase s | 285 | 246 (-13.6%) | 264 (-7.1%) | 249 (-12.5%) | 267 (-6.2%) | 262 (-7.9%) | 279 (-1.9%) | 279 (-1.8%) |
| graph rows M | 200.0 | 200.8 (+0.4%) | 199.8 (-0.1%) | 200.6 (+0.3%) | 200.1 (+0.1%) | 200.2 (+0.1%) | 200.6 (+0.3%) | 201.0 (+0.5%) |
| run-to-run spread of peak | 11.5% | 2.2% | 9.6% | 1.7% | 6.0% | 11.6% | 1.7% | 4.5% |
| output identical | (reference) | yes | yes | yes | yes | yes | yes | yes |

#### Drift control: baseline re-run after the sweep

| metric | baseline (n=3) | all (n=3) |
|---|---:|---:|
| peak footprint GiB | 20.64 | 20.96 (+1.5%) |
| peak live GiB | 17.19 | 17.68 (+2.8%) |
| allocs M | 855 | 826 (-3.4%) |
| alloc total GiB | 901 | 851 (-5.5%) |
| namespace phase s | 275 | 272 (-1.1%) |
| graph rows M | 201.2 | 200.2 (-0.5%) |
| run-to-run spread of peak | 2.9% | 9.7% |
| output identical | (reference) | yes |

#### Mechanism: Note only (DataFusion, output shares input buffers)

| metric | baseline (n=2) | pagedrop (n=2) |
|---|---:|---:|
| peak footprint GiB | 1.97 | 2.01 (+2.1%) |
| peak live GiB | 1.29 | 1.28 (-0.6%) |
| allocs M | 3 | 3 (-0.1%) |
| alloc total GiB | 36 | 36 (-0.0%) |
| namespace phase s | 98 | 99 (+0.6%) |
| graph rows M | 8.0 | 8.0 (+0.0%) |
| run-to-run spread of peak | 3.2% | 3.3% |
| output identical | (reference) | yes |

#### Mechanism: SystemNote only (Rust transform, independent buffers)

| metric | baseline (n=2) | pagedrop (n=2) | noteborrow (n=2) |
|---|---:|---:|---:|
| peak footprint GiB | 1.06 | 0.67 (-37.2%) | 1.06 (-0.3%) |
| peak live GiB | 0.97 | 0.52 (-46.2%) | 0.97 (+0.2%) |
| allocs M | 20 | 20 (-0.0%) | 10 (-48.6%) |
| alloc total GiB | 9 | 9 (-0.1%) | 8 (-12.6%) |
| namespace phase s | 12 | 12 (-0.9%) | 10 (-14.4%) |
| graph rows M | 0.4 | 0.4 (+0.0%) | 0.4 (+0.0%) |
| run-to-run spread of peak | 1.0% | 2.5% | 0.7% |
| output identical | (reference) | yes | yes |

#### Partitioned initial load: Job only, 30M rows, five partitions

| metric | baseline (n=2) | partsplit (n=2) | all (n=2) |
|---|---:|---:|---:|
| peak footprint GiB | 6.21 | 2.19 (-64.8%) | 2.12 (-65.9%) |
| peak live GiB | 4.70 | 0.93 (-80.2%) | 0.91 (-80.6%) |
| allocs M | 2761 | 2869 (+3.9%) | 2765 (+0.2%) |
| alloc total GiB | 902 | 924 (+2.4%) | 901 (-0.1%) |
| namespace phase s | 291 | 334 (+14.7%) | 331 (+13.8%) |
| graph rows M | 150.0 | 150.0 (+0.0%) | 150.0 (+0.0%) |
| run-to-run spread of peak | 0.0% | 9.2% | 2.9% |
| output identical | (reference) | yes | yes |

#### Attribution: MergeRequest only (widest tag-column fan-out)

| metric | baseline (n=3) | utf8 (n=3) |
|---|---:|---:|
| peak footprint GiB | 5.80 | 5.92 (+2.2%) |
| peak live GiB | 4.13 | 4.30 (+4.0%) |
| allocs M | 279 | 273 (-2.1%) |
| alloc total GiB | 125 | 127 (+1.2%) |
| namespace phase s | 138 | 138 (-0.3%) |
| graph rows M | 20.0 | 20.0 (+0.0%) |
| run-to-run spread of peak | 1.1% | 1.2% |
| output identical | (reference) | yes |

#### Attribution: Note only (client-path changes)

| metric | baseline (n=3) | nolz4 (n=3) | zerocopy (n=3) |
|---|---:|---:|---:|
| peak footprint GiB | 1.91 | 1.94 (+1.9%) | 2.00 (+5.0%) |
| peak live GiB | 1.31 | 1.19 (-9.0%) | 1.28 (-1.9%) |
| allocs M | 3 | 3 (-0.2%) | 3 (-0.1%) |
| alloc total GiB | 36 | 34 (-6.4%) | 34 (-5.1%) |
| namespace phase s | 98 | 97 (-1.2%) | 99 (+0.8%) |
| graph rows M | 8.0 | 8.0 (+0.0%) | 8.0 (+0.0%) |
| run-to-run spread of peak | 4.0% | 10.4% | 2.3% |
| output identical | (reference) | yes | yes |

#### Page size, paired and alternated

| page size | n | peak footprint GiB | spread | namespace phase s | spread | graph rows M |
|---|---:|---:|---:|---:|---:|---:|
| 500,000 | 4 | 19.65 | 16.6% | 281 | 39.8% | 200.5 |
| 250,000 | 4 | 11.20 | 7.6% | 308 | 9.9% | 199.6 |

250k versus 500k: peak -43.0%, namespace phase +9.7%

#### Sensitivity of the baseline peak

| shape | peak footprint GiB | peak live GiB | namespace phase s | graph rows M |
|---|---:|---:|---:|---:|
| 500k pages, 20 slots | 20.64 | 17.19 | 275 | 201.2 |
| 250k pages, 20 slots | 12.26 | 9.89 | 285 | 199.7 |
| 500k pages, 10 slots | 12.23 | 9.84 | 342 | 199.2 |

#### Fingerprint equality across shapes

- 250k pages vs 500k pages: identical
- 10 slots vs 500k pages: identical


## Reading the numbers

- `exact_max_footprint` is the kernel's own high-water mark (`VmHWM` on Linux), so it does not depend
  on the sampler catching the right instant.
- `exact_max_alloc_live` is bytes the program asked for at its peak. The spread against the footprint
  is allocator overhead, fragmentation and retention.
- A change that writes fewer rows is not a memory win. Check the row counts and the failure warnings
  before believing any peak.
