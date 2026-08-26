# Dispatcher peak memory and publish fairness, August 2026

The dispatcher OOM-killed four times on 2026-08-24 between 20:30 and 20:40 UTC at its 512 MiB
limit, and no code backfill was published until the limit was raised. The cause was the code
backfill sweep: it enumerated every enabled namespace's pending projects into one list and only
started publishing after the last namespace was read. On gitlab.com that list was 2.33M projects.

Publishing in batches, with each namespace taking an equal share of a batch, cuts peak memory for
that sweep from 172 MiB to 29 MiB (allocated bytes: 163 MiB to 8.7 MiB) and at the same time makes
the queue order fairer than the fleet-wide shuffle it replaces.

## What production did

| Fact | Value | Source |
|---|---|---|
| Restarts | 4 between 20:30 and 20:40 UTC | `kubernetes.io/container/restart_count` |
| Memory limit at the time | 512 MiB | `argocd-apps` chart default |
| Peak seen after the limit was raised | 270 MiB (5-minute `ALIGN_MAX`) | `kubernetes.io/container/memory/used_bytes` |
| Steady-state dispatcher memory | 60-70 MiB | same |
| Enabled root namespaces | ~2,500 | `dispatched namespace indexing requests`, `sweeping: true` |
| First successful backfill tick | 1,076,820 dispatched + 1,255,037 skipped at 21:14 UTC | `dispatched code backfill requests` |
| Time for that tick | ~27 minutes | pod start 20:47 to the log line |

The window coincided with a schema migration. That is what made the sweep fleet-wide: the new
version's `code_indexing_checkpoint` table starts empty, so every project under every enabled
namespace is pending at once. This is not a rare state, it is what every schema migration produces.

## Reproduction

`crates/dispatch-profiler` seeds a synthetic datalake and graph in a local ClickHouse
(`numbers()`-generated rows, so seeding never runs through the profiled process), then runs the
same `CodeBackfill` the `DispatchIndexing` mode runs against it with a counting NATS in place of
the broker. It samples RSS, macOS `phys_footprint` and requested bytes from a tracking allocator,
and attributes every published subject back to its namespace so publish order can be measured. See
[the runbook](../runbooks/dispatcher_memory_profiling.md).

The profiler builds with mimalloc `secure`, which is what `orbit-server` ships, so the gap between
requested bytes and footprint is production's allocator overhead and not a cheaper default's.

## Memory

macOS, mimalloc secure, 2,500 namespaces, 100,000 publish window (the default ships at 200,000, which adds about 70 bytes per extra queued project).
`footprint` is the kernel's lifetime high-water mark, `requested` is bytes the program asked the
allocator for at the peak.

| Shape | Version | Dispatched | Footprint MiB | RSS MiB | Requested MiB |
|---|---|---|---|---|---|
| 2.33M pending | before | 2,330,000 | 171.9 | 208.4 | 162.6 |
| 2.33M pending | after | 100,000 | 29.0 | 43.1 | 8.7 |
| 2.33M rows, 90% checkpointed | before | 233,000 | 34.1 | 50.5 | 17.8 |
| 2.33M rows, 90% checkpointed | after | 100,000 | 29.7 | 44.6 | 8.6 |

Wall-clock is not reported: the runs were not taken under controlled power and thermal conditions,
so their timings are not comparable. Memory figures are unaffected by that.

The "after" rows publish one batch rather than the whole backlog: each namespace takes the batch
size divided by the namespaces that still have pending projects (40 each here), and the rest of the
backlog arrives on later runs as projects are checkpointed. Drain rate stays bound by indexing
capacity, not by queue depth.

These are macOS readings, so they bound the Linux numbers from above: mimalloc's purge returns
pages with `MADV_DONTNEED`, which Linux drops out of RSS and macOS keeps in the footprint. The
requested-bytes column is platform-independent.

## Publish fairness

Batching on its own reintroduced the problem the fleet-wide shuffle was added to fix
([!1407](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1407)): with a
namespace's pending list appended whole, a namespace larger than the batch is published as one
block, and the work queue keeps that order until it drains, because the stream holds one message
per subject under `WorkQueue` retention. Later runs cannot dilute a block already in the queue.

Same fleet, but with 20% of the 2.33M projects concentrated in one namespace:

| Variant | Window | Dispatched | Footprint MiB | Requested MiB | Longest single-namespace run | Other namespaces' first queue position p50 / p95 / max |
|---|---|---|---|---|---|---|
| Fleet-wide shuffle | n/a | 2,330,000 | 181.7 | 159.9 | 8 | 2,146 / 9,185 / 28,910 |
| Batches, no per-namespace share | 50,000 | 2,330,000 | 103.9 | 40.9 | 466,000 | 1,379,133 / 2,190,859 / 2,292,273 |
| Batches with the share | 50,000 | 50,000 | 40.1 | 7.1 | 2 | 1,701 / 6,742 / 15,810 |
| Batches with the share | 100,000 | 100,000 | 36.7 | 8.6 | 2 | 1,719 / 7,372 / 19,238 |

The share keeps every one of the 2,500 namespaces present in the queue at all times, so no
namespace waits for another to finish. Window size trades queue depth against how often a run has
to top it up, not fairness.

## Where the old peak went

At 2.33M projects, roughly:

- 75 MiB for the `Vec<PendingProject>` spine (32 bytes per entry)
- 56-75 MiB for the traversal-path `String`s (one allocation each)
- the rest in per-namespace transients: the whole HTTP response body buffered as one `Vec<u8>`,
  then all its `RecordBatch`es, then a `Vec<i64>` and a `Vec<String>` extracted from them, then
  the `Vec<PendingProject>` built from those, four representations of the same rows alive at once.

## Changes

Shipped in [!2328](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/2328).

1. Publish in batches of `schedule.tasks.code-backfill.publish_window` projects (default 200,000)
   instead of accumulating the fleet.
2. Cap each namespace at its share of a batch, the window divided by the namespaces that still had
   pending projects on the previous run, so a fleet-wide sweep hands out equal slices and a lone
   straggler gets the whole batch.
3. Read the checkpoints before the projects and filter the project stream as it arrives, so an
   already-indexed namespace never materialises its project list.
4. Consume the ClickHouse responses as they stream instead of buffering the body and then all of
   its batches.
5. Visit namespaces in random order, so the remainder of the division does not always fall on the
   same ones.

## Not addressed

- The per-namespace checkpoint set is still sized by the namespace (~10-16 MiB per million indexed
  projects, held one namespace at a time). It is the remaining term that scales with a single
  namespace rather than with the window. Keyset pagination by project id is the fix if a namespace
  ever gets large enough for it to matter.
- A project that never reaches a checkpoint keeps occupying one of its namespace's share slots,
  because the next scan selects it again. It was already re-published on every run before this
  change; what is new is that it consumes quota.
- The pending-projects query does not deduplicate. `project_namespace_traversal_paths` is a
  `ReplacingMergeTree` fed one row per path change, and every other reader of it collapses versions
  with `argMax(traversal_path, version)` or `FINAL`. That is why more than half of the first
  production tick's rows were rejected as per-subject duplicates. Deduplicating in SQL would halve
  the rows read and remove the wasted round trips, but it does not change peak memory now that the
  batch bounds it, and it changes which path a moved project is dispatched under.
- The per-run floor is query latency, not publishing: one enabled-namespaces query plus two per
  namespace, issued sequentially. Against ClickHouse Cloud round trips that is the dominant cost of
  a run once publishing is capped, and it was never measured under controlled conditions here.
- The SDLC namespace-change detection path was not profiled. It materialises
  `BTreeMap<(id, path), BTreeSet<String>>` over the change-detection result, which is bounded by
  changed namespaces (~200 per tick, ~2,500 on a sweep) rather than by projects.
