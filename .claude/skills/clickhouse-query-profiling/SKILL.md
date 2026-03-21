---
name: clickhouse-query-profiling
description: Profile GKG queries against ClickHouse with the query-profiler CLI. For optimizing query performance, comparing query plans, investigating slow queries, or checking ClickHouse resource usage.
---

# ClickHouse query profiling

Run GKG JSON DSL queries directly against any ClickHouse instance and get back execution stats, EXPLAIN plans, CPU/memory profiling, and instance health. No gRPC server or Rails needed.

## Build

```bash
cargo build -p query-profiler
```

## Configuration

Set connection via env vars:

```bash
export CLICKHOUSE_URL=http://localhost:8123
export CLICKHOUSE_DATABASE=gkg_graph
export CLICKHOUSE_USER=<user>
export CLICKHOUSE_PASSWORD=<password>
```

Or use CLI flags: `--ch-url`, `--ch-database`, `--ch-user`, `--ch-password`.

## Running queries

Basic search with stats:

```bash
cargo run -p query-profiler -- \
  -t '1/' \
  '{"query_type":"search","node":{"id":"p","entity":"Project","columns":["name"]},"limit":5}'
```

With EXPLAIN plans:

```bash
cargo run -p query-profiler -- \
  -t '1/' --explain \
  '{"query_type":"traversal","nodes":[{"id":"mr","entity":"MergeRequest"},{"id":"p","entity":"Project"}],"relationships":[{"type":"IN_PROJECT","from":"mr","to":"p"}],"limit":10}'
```

With CPU/memory profiling from `system.query_log`:

```bash
cargo run -p query-profiler -- \
  -t '1/' --explain --profile --processors \
  '{"query_type":"aggregation","nodes":[{"id":"mr","entity":"MergeRequest"},{"id":"p","entity":"Project"}],"relationships":[{"type":"IN_PROJECT","from":"mr","to":"p"}],"aggregations":[{"function":"count","target":"mr","group_by":"p","alias":"mr_count"}],"limit":10}'
```

With instance health snapshot:

```bash
cargo run -p query-profiler -- -t '1/' --health '{"query_type":"search","node":{"id":"p","entity":"Project"},"limit":1}'
```

Multiple traversal paths:

```bash
cargo run -p query-profiler -- -t '1/2/3/' -t '1/4/5/' '{"query_type":"search","node":{"id":"p","entity":"Project"},"limit":5}'
```

Query from file:

```bash
cargo run -p query-profiler -- -t '1/' --explain @fixtures/queries/my_query.json
```

Pretty-printed output:

```bash
cargo run -p query-profiler -- -t '1/' --format pretty '...'
```

## Output structure

The JSON output has these sections:

- `query` -- the original query JSON
- `security_context` -- org_id and traversal_paths
- `compilation` -- parameterized SQL, rendered SQL, hydration plan
- `executions` -- per-query metrics (1 base + N hydration queries), each with:
  - `label` -- "base" or "hydration:Project", etc.
  - `query_id` -- ClickHouse query ID for system table correlation
  - `stats` -- read_rows, read_bytes, result_rows, result_bytes, elapsed_ns, memory_usage
  - `explain_plan` -- EXPLAIN PLAN with index usage (with `--explain`)
  - `explain_pipeline` -- EXPLAIN PIPELINE processor graph (with `--explain`)
  - `query_log` -- CPU times, cache stats, ProfileEvents (with `--profile`)
  - `processors` -- per-processor pipeline breakdown (with `--processors`)
- `summary` -- totals across all queries
- `instance_health` -- server health (with `--health`)

## What to look at

The main optimization targets:

- `read_rows` -- total rows scanned. This is the number to reduce.
- `read_bytes` -- data volume read from disk/cache.
- `memory_usage` -- RAM consumed per query. Watch for spills.
- `elapsed_ns` -- server-side wall clock time.

With `--profile`, you also get:

- `selected_parts` / `selected_marks` -- how well the primary key prunes data
- `mark_cache_hits` / `mark_cache_misses` -- mark cache hit rate
- `real_time_us` vs `user_time_us` -- tells you if a query is CPU-bound or I/O-bound
- `os_io_wait_us` -- time spent waiting on disk
- `external_sort_bytes` / `external_agg_bytes` -- nonzero means memory spilled to disk

With `--processors`:

- High `input_wait_us` means a processor is starved for input (upstream is the bottleneck)
- High `output_wait_us` means a processor is blocked on output (downstream is the bottleneck)

With `--health`:

- `active_merges` -- background merges compete with queries for I/O
- `temp_files_*` -- active memory spills across the server
- `table_parts` -- high part count per table means fragmentation, which slows reads

## A/B comparison

1. Run the query and save output: `cargo run -p query-profiler -- -t '1/' --explain QUERY > before.json`
2. Make your optimizer change
3. Rebuild: `cargo build -p query-profiler`
4. Run the same query: `cargo run -p query-profiler -- -t '1/' --explain QUERY > after.json`
5. Compare `read_rows` and `elapsed_ns` between the two files

## Things to know

- ClickHouse inlines CTEs. A CTE referenced N times runs N times. There is no materialization.
- The profiler runs the full compile, execute, extract, mock-redaction, hydration pipeline.
- All resources are auto-approved (mock redaction). No Rails connection needed.
- A single GKG query can produce multiple ClickHouse queries: 1 base + N hydration queries. Each one is profiled independently.
- `--profile` runs `SYSTEM FLUSH LOGS` before querying `system.query_log`, which adds about 100ms.
- `--profile` and `--health` require the ClickHouse user to have SELECT on system tables.
