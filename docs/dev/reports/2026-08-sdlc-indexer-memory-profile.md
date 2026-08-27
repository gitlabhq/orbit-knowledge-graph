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

PLACEHOLDER_RESULTS

## Reading the numbers

- `exact_max_footprint` is the kernel's own high-water mark (`VmHWM` on Linux), so it does not depend
  on the sampler catching the right instant.
- `exact_max_alloc_live` is bytes the program asked for at its peak. The spread against the footprint
  is allocator overhead, fragmentation and retention.
- A change that writes fewer rows is not a memory win. Check the row counts and the failure warnings
  before believing any peak.
