# SDLC indexing memory optimization — benchmark notes

Harness: `cargo run -p indexer --features bench-sdlc --release --bin bench_sdlc`
(`crates/indexer/src/modules/sdlc/bench.rs` + `src/bin/bench_sdlc.rs`) reports
throughput/ingestion. Peak heap was measured externally via max RSS
(`/usr/bin/time -l`); the figures below come from an earlier in-process tracking
allocator and are retained for reference.

## What changed

The old pipeline collected a whole page (`LIMIT` rows) into a `Vec<RecordBatch>`, built a
DataFusion MemTable, transformed, wrote, and prefetched the next whole page — peak ≈ two
pages. The new pipeline is a producer/consumer split: a spawned task streams each page from
the datalake in small Arrow blocks, transforms each block, and feeds a bounded channel
(`WRITE_CHANNEL_CAPACITY` blocks of read-ahead); a writer drains it into one streaming
`INSERT` per page. The producer reads ahead across page boundaries to preserve cross-page
overlap. Page-level enrichment still runs server-side in the extract SQL; all DataFusion
transforms are row-wise, so per-block transformation is equivalent. Peak memory is bounded
by the read-ahead window (a few blocks), not the page.

## Result (default config: `INSERT_FLUSH_ROWS=0`, `WRITE_CHANNEL_CAPACITY=8`)

Local, 2M rows / 500k page / 400 B description, N=10–12 interleaved (baseline binary from
`main`, optimized from this branch):

| metric | baseline | optimized | delta |
|---|---|---|---|
| peak heap | ~459 MiB | ~59 MiB | **−87% (7.8×)** |
| throughput | 349k r/s | — | **+13–15%** |
| ingestion | 448k r/s | — | **+22%** |

Cloud (remote ClickHouse, 1M/500k, async, `optimize_on_insert=0`, per-rep paired, N=10 clean
after throttle-filtering):

- **Memory: ~87% reduction** (every rep, ~59 MiB vs ~365–473 MiB).
- **Throughput: median ~0.99× (parity).** Wide spread (0.75–1.8×) from a throttled instance;
  worst-case healthy-pair ~0.91×.

## Read-ahead is the throughput dial (don't under-size it)

The logical workload is unchanged (one SELECT + one INSERT per page), but streaming
interleaves reads and writes at block granularity. The writer must stay fed or the insert
stream gets gappy and loses throughput on a latency-bound backend. `WRITE_CHANNEL_CAPACITY`
controls this *and* peak memory (≈ that many blocks):

| read-ahead | cloud throughput (paired median) | peak memory |
|---|---|---|
| cap=4  | ~0.88× (writer slightly starved) | −86% |
| **cap=8**  | **~0.99× (parity)** | **−87%** |
| cap=32 | ~1.04× | −66% |

So cap=8 restores parity while keeping ~87% reduction; more read-ahead buys no throughput and
costs memory. `INSERT_FLUSH_ROWS` does **not** affect memory (the streaming writer flushes
per batch regardless); `0` (one insert/page) is optimal and is the default.

## Correction: earlier numbers were a harness bug

Earlier iterations reported a ~40–54% memory "floor" and per-config cloud throughput
regressions (~0.7–0.8×) that drove a long flush-threshold tuning exercise. Those were a
**benchmark bug**: `TimingDestination` overrode only `new_batch_writer`, so
`open_streaming_writer` fell through to the default `BufferingStreamingWriter`, which
accumulates the whole page and writes it one-shot — *not* the production path
(`ClickHouseStreamingWriter`, true per-batch streaming). The bug inflated peak memory to
~a page and turned mid-page flushing into multiple inserts (hence the phantom regressions).
Fixed in `fix(bench): exercise the real ClickHouse streaming writer`. The production code was
always streaming correctly; only the measurement was wrong.

## Guaranteeing behavior across deployments

- **Correctness — universal:** identical output (row-wise transforms, ReplacingMergeTree
  idempotency, unchanged checkpoint cadence), proven by unit + SDLC integration suites.
- **Memory — universal:** peak heap is a deterministic property of the indexer process,
  independent of ClickHouse; ~88% reduction holds everywhere. Wire the harness into CI as a
  threshold gate to prevent future regressions.
- **Throughput — backend-dependent, parity at the default:** +13–15% local; cloud median
  ~0.99× (parity) at `cap=8`. Read-ahead must be sized for the backend's latency (cap=4 was
  ~0.88×); the default 8 restores parity on the remote instance tested. Validate per
  deployment against the indexer's existing throughput / write-duration metrics with staged
  rollout; raise `WRITE_CHANNEL_CAPACITY` if a deployment is throughput-sensitive.
