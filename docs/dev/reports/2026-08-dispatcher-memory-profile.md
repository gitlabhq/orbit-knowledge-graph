# Dispatcher peak memory, August 2026

The dispatcher OOM-killed four times on 2026-08-24 between 20:30 and 20:40 UTC at its 512 MiB
limit, and no code backfill was published until the limit was raised. The cause was the code
backfill sweep: it enumerated every enabled namespace's pending projects into one list and only
started publishing after the last namespace was read. On gitlab.com that list was 2.33M projects.

Bounding the publish to a 50,000-project window cuts peak memory for that sweep from 172-185 MiB
to 26-27 MiB (allocated bytes: 163 MiB to 4.5 MiB) with no change in what gets dispatched and no
change in wall-clock time.

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
and reports the allocator's own peak commit. See
[the runbook](../runbooks/dispatcher_memory_profiling.md).

The profiler builds with mimalloc `secure`, which is what `orbit-server` ships, so the gap between
requested bytes and footprint is production's allocator overhead and not a cheaper default's.

## Measurements

macOS, mimalloc secure, 2,500 namespaces unless stated. `footprint` is the kernel's lifetime
high-water mark, `requested` is bytes the program asked the allocator for at the peak.

| Shape | Version | Dispatched | Footprint MiB | RSS MiB | Requested MiB | Allocations | ms |
|---|---|---|---|---|---|---|---|
| 2.33M pending (production) | before | 2,330,000 | 171.9 | 208.4 | 162.6 | 33.6M | 12,276 |
| 2.33M pending (production) | after | 2,330,000 | **26.3** | 38.5 | **4.5** | 33.5M | 12,122 |
| 2.33M pending, repeat run | before | 2,330,000 | 185.2 | 220.8 | 162.6 | 33.6M | 13,531 |
| 2.33M pending, repeat run | after | 2,330,000 | **27.4** | 39.8 | **4.5** | 33.5M | 13,653 |
| 2.33M, 5-segment paths | before | 2,330,000 | 221.2 | 245.7 | 202.6 | 33.6M | 12,614 |
| 2.33M, 5-segment paths | after | 2,330,000 | **29.9** | 42.1 | **5.4** | 33.5M | 12,594 |
| 2.33M, 90% checkpointed | before | 233,000 | 34.9 | 51.1 | 17.8 | 6.4M | 13,110 |
| 2.33M, 90% checkpointed | after | 233,000 | **25.4** | 38.8 | **5.6** | 4.2M | 12,764 |
| 2.33M, fully checkpointed | before | 0 | 21.4 | 33.8 | 1.8 | 3.4M | 13,525 |
| 2.33M, fully checkpointed | after | 0 | 23.3 | 35.7 | 1.8 | **1.0M** | 13,073 |
| 1M in a single namespace | before | 1,000,000 | 167.0 | 179.1 | 113.6 | 14.0M | 1,596 |
| 1M in a single namespace | after | 1,000,000 | 142.8 | 154.9 | 83.2 | 14.0M | 1,519 |

Dispatched counts are identical before and after in every shape, which is the correctness check on
the checkpoint filter's move from a `HashSet` to a sorted `Vec`.

These are macOS readings, so they bound the Linux numbers from above: mimalloc's purge returns
pages with `MADV_DONTNEED`, which Linux drops out of RSS and macOS keeps in the footprint. The
requested-bytes column is platform-independent.

Path length matters because the dispatcher holds one heap-allocated `String` per pending project:
going from 3 to 5 path segments added 40 MiB to the old peak and 3.6 MiB to the new one.

## Where the old peak went

At 2.33M projects, roughly:

- 75 MiB for the `Vec<PendingProject>` spine (32 bytes per entry)
- 56-75 MiB for the traversal-path `String`s (one allocation each)
- the rest in per-namespace transients: the whole HTTP response body buffered as one `Vec<u8>`,
  then all its `RecordBatch`es, then a `Vec<i64>` and a `Vec<String>` extracted from them, then
  the `Vec<PendingProject>` built from those — four representations of the same rows alive at once.

## Changes

Shipped separately in [!2328](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/2328);
this branch carries the harness and the measurements only.

1. **Publish in 50,000-project windows** instead of accumulating the fleet. This is the fix; the
   others are small next to it. The shuffle that interleaves namespaces now runs per window, so
   FIFO consumption still alternates between namespaces rather than draining one namespace first.
2. **Read the checkpoints before the projects and filter the project stream as it arrives.** An
   already-indexed namespace no longer materialises its project list at all, which is what drops
   the fully-checkpointed run from 3.4M allocations to 1.0M.
3. **Build `PendingProject` directly from each `RecordBatch`**, dropping the intermediate
   `Vec<i64>` and `Vec<String>`, and consume batches as they stream instead of collecting them.
4. **Checkpointed ids as a sorted `Vec` + binary search** rather than a `HashSet`: 8 bytes per id
   with no empty slots to pay for.
5. **`extend_from_slice` instead of `extend` when buffering a response body.** `Bytes` iterates
   per byte, so every `fetch_arrow` in the process was pushing one byte at a time.

## Fairness of the publish order

Bounding memory by publishing in windows reintroduced the problem the fleet-wide shuffle was added
to fix ([!1407](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1407)): a
namespace with more pending projects than one window is published as a single block, and the work
queue keeps that order until it drains. Capping each namespace's share of a window fixes both at
once. Measured on a skewed fleet, 2,500 namespaces and 2.33M pending with 20% of the projects in
one namespace:

| Variant | Dispatched | Footprint MiB | Requested MiB | Longest single-namespace run | Other namespaces' first queue position p50 / p95 / max |
|---|---|---|---|---|---|
| Fleet-wide shuffle | 2,330,000 | 181.7 | 159.9 | 8 | 2,146 / 9,185 / 28,910 |
| Window, no per-namespace cap | 2,330,000 | 103.9 | 40.9 | 466,000 | 1,379,133 / 2,190,859 / 2,292,273 |
| Window with per-namespace share | 50,000 | 39.8 | 9.3 | 2 | 1,687 / 7,015 / 19,431 |

The capped run publishes a tick's worth (one window, shared equally) rather than the whole backlog,
so the queue stays shallow and every one of the 2,500 namespaces is represented in it. Drain rate is
consumer-bound either way; what changes is that no namespace has to wait for another to finish.

## Not addressed

- **A single namespace with a very large project count is still unbounded** (1M in one namespace:
  142.8 MiB). The window only flushes between namespaces, because flushing mid-stream would hold
  the ClickHouse response open for the length of a publish batch. gitlab.com's enabled namespaces
  average ~430 projects, so this is a tail risk rather than a live one; keyset pagination on
  `project_id` is the fix if a namespace ever gets large enough to matter.
- **The pending-projects query does not deduplicate.** `project_namespace_traversal_paths` is a
  `ReplacingMergeTree` fed one row per path change, and every other reader of it (the traversal-path
  dictionaries, the namespace-deletion store) collapses it with `argMax(traversal_path, version)`
  or `FINAL`. The backfill query reads raw rows, which is why the first successful tick published
  1.08M requests and had 1.26M rejected as per-subject duplicates: over half the enumerated rows
  were older versions of a project already dispatched in the same tick. Deduplicating in SQL would
  halve the rows read and remove ~1.26M pointless NATS round trips per tick (most of that tick's
  27 minutes), and would stop dispatching stale paths for moved projects. It does not change peak
  memory now that the window bounds it, so it belongs in its own change.
- **`current_routes_under_root` in the namespace-deletion store** materialises every distinct path
  under the namespace being deleted, with the same per-namespace scaling as the point above. It
  only runs for scheduled deletions, so one namespace at a time.
- **The SDLC namespace-change detection path was not profiled.** It materialises
  `BTreeMap<(id, path), BTreeSet<String>>` over the change-detection result, which is bounded by
  changed namespaces (~200 per tick, ~2,500 on a sweep) rather than by projects. The occasional
  40 MiB spikes in the steady-state graph are the likely candidate, and they are far from the
  limit.
