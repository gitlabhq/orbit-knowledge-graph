# Dispatcher memory profiling

`crates/dispatch-profiler` measures peak memory of the dispatcher's code-backfill enumeration at a
chosen scale. It seeds a synthetic datalake and graph in ClickHouse, runs the real `CodeBackfill`
against them with a counting NATS stub, and writes a JSON report plus a sample timeline.

Findings from the first run of this harness: [dispatcher peak memory, August 2026](../reports/2026-08-dispatcher-memory-profile.md).

## Run it

Start a ClickHouse the profiler can drop and recreate databases in:

```shell
docker run -d --name gkg-memprofile-ch \
  -e CLICKHOUSE_PASSWORD=memprofile -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  -p 18123:8123 -p 19000:9000 --ulimit nofile=262144:262144 \
  clickhouse/clickhouse-server:26.2
```

Then build and run. The `memprofile` profile is release with debug info kept:

```shell
cargo build -p dispatch-profiler --profile memprofile
./target/memprofile/dispatch-profiler --projects 2330000 --namespaces 2500 --label prod
```

Results land in `.memprofile/dispatch/<label>.json` (peaks, counts, timings) and
`.memprofile/dispatch/<label>.samples.jsonl` (one line per sample, tagged with the phase).

## Shape flags

| Flag | Meaning |
|---|---|
| `--projects` | Total projects across all namespaces |
| `--namespaces` | Enabled root namespaces, projects distributed evenly |
| `--checkpointed-pct` | Share already checkpointed for the current schema version. `0` is the post-migration sweep, `90` is steady state |
| `--path-depth` | Segments in a project's traversal path. Longer paths cost one longer `String` per pending project |
| `--big-namespace-pct` | Concentrates this share of the projects in one namespace, for measuring a skewed fleet |
| `--publish-window` | Overrides the configured publish batch size |
| `--seed-only` / `--skip-seed` | Seed once, then A/B several binaries against the same data |
| `--publish-delay-us` | Sleep per publish, standing in for the JetStream ack round trip |

The report also carries a `fairness` block: the longest stretch of the queue held by one namespace,
and where each other namespace's first request landed (p50, p95, max). Those are the two numbers the
fleet-wide shuffle was judged on, so they are what to check when the publish order changes.

## Reading the numbers

- `peaks.exact_max_footprint` is the kernel's own high-water mark, so it does not depend on the
  sampler catching the right instant. Prefer it over `peaks.max_footprint`.
- `peaks.exact_max_alloc_live` is bytes the program asked for at its peak. The spread against the
  footprint is allocator overhead, fragmentation and retention.
- `settled` is measured after `mi_collect`, so it separates what is still live from what the
  allocator is holding.
- On macOS, RSS keeps decommitted pages that Linux drops, which is why the footprint and RSS
  columns differ by tens of MiB. Compare like with like across runs.
- The profiler builds with mimalloc `secure` by default because `orbit-server` does. Turning that
  off makes every number look better and none of them comparable to production.

## A/B against a change

Seed once, then run two binaries over the same data:

```shell
./target/memprofile/dispatch-profiler --seed-only --label seed --projects 2330000 --namespaces 2500
./dp-before --skip-seed --label before --projects 2330000 --namespaces 2500
./dp-after  --skip-seed --label after  --projects 2330000 --namespaces 2500
```

Check `dispatched` and `skipped` match between the two before comparing peaks: a change that
publishes fewer requests is not a memory win.
