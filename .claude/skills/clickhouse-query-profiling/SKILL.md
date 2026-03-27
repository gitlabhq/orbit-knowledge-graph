---
name: clickhouse-query-profiling
description: Profile GKG queries against ClickHouse with the query-profiler CLI. For optimizing query performance, comparing query plans, investigating slow queries, or checking ClickHouse resource usage.
---

# ClickHouse query profiling

Run GKG JSON DSL queries directly against any ClickHouse instance and get back execution stats, EXPLAIN plans, CPU/memory profiling, and instance health. No gRPC server or Rails needed.

## Build

```bash
mise build
```

## Configuration

Set connection via env vars or a `.env` file:

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
mise query:profile -- \
  -t '1/' \
  '{"query_type":"search","node":{"id":"p","entity":"Project","columns":["name"]},"limit":5}'
```

With EXPLAIN plans:

```bash
mise query:profile -- \
  -t '1/' --explain \
  '{"query_type":"traversal","nodes":[{"id":"mr","entity":"MergeRequest"},{"id":"p","entity":"Project"}],"relationships":[{"type":"IN_PROJECT","from":"mr","to":"p"}],"limit":10}'
```

With CPU/memory profiling from `system.query_log`:

```bash
mise query:profile -- \
  -t '1/' --explain --profile --processors \
  '{"query_type":"aggregation","nodes":[{"id":"mr","entity":"MergeRequest"},{"id":"p","entity":"Project"}],"relationships":[{"type":"IN_PROJECT","from":"mr","to":"p"}],"aggregations":[{"function":"count","target":"mr","group_by":"p","alias":"mr_count"}],"limit":10}'
```

With instance health snapshot:

```bash
mise query:profile -- -t '1/' --health '{"query_type":"search","node":{"id":"p","entity":"Project"},"limit":1}'
```

Multiple traversal paths:

```bash
mise query:profile -- -t '1/2/3/' -t '1/4/5/' '{"query_type":"search","node":{"id":"p","entity":"Project"},"limit":5}'
```

Query from file:

```bash
mise query:profile -- -t '1/' --explain @fixtures/queries/my_query.json
```

## Output

By default output is pretty-printed JSON to stdout. Use `--format json` for compact JSON.

Write results to a file with `--output` (`-o`). Parent directories are created automatically:

```bash
mise query:profile -- -t '1/' --explain -o fixtures/profiling/results.json @fixtures/queries/my_query.json
```

When `--output` is used, the profiler writes the JSON file and prints the path to stderr. When omitted, results go to stdout. Progress and errors always go to stderr, so they never mix with JSON output.

Result files go in `fixtures/profiling/` (gitignored).

### Workflow: run, wait, then analyze

Use `--output` so results are written to disk when the run finishes, then read the output file to analyze results:

```bash
mise query:profile -- -t '1/' --explain --profile \
  -o fixtures/profiling/showcase.json \
  @fixtures/queries/optimization_showcase.json

# Then read fixtures/profiling/showcase.json to analyze
```

### Output structure

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

## Multi-query files

The profiler can run all queries from a file where keys are query names and values are query objects. Queries are executed sequentially so profiling numbers are not polluted by concurrent load.

```bash
mise query:profile -- -t '1/' --explain @fixtures/queries/optimization_showcase.json
```

Filter to a subset by name substring:

```bash
mise query:profile -- -t '1/' --explain --filter aggregation @fixtures/queries/optimization_showcase.json
```

Write multi-query results to a file:

```bash
mise query:profile -- -t '1/' --explain \
  -o fixtures/profiling/showcase.json \
  @fixtures/queries/optimization_showcase.json
```

Progress is printed to stderr (`[3/25] query_name...`). Output is a JSON object keyed by query name, each value being the standard profiler output. Queries that fail are recorded with an `{"error": "..."}` value and the run continues with the rest.

## A/B comparison

1. Run the query and save output: `mise query:profile -- -t '1/' --explain -o fixtures/profiling/before.json QUERY`
2. Make your optimizer change
3. Rebuild: `mise build`
4. Run the same query: `mise query:profile -- -t '1/' --explain -o fixtures/profiling/after.json QUERY`
5. Diff the results: `mise query:diff -- fixtures/profiling/before.json fixtures/profiling/after.json --labels before,after`

## Diffing result files

`mise query:diff` compares two or more profiler result files and produces a markdown table.

Two-way comparison (default metric is `read_rows`):

```bash
mise query:diff -- fixtures/profiling/baseline.json fixtures/profiling/dedup.json --labels baseline,dedup
```

Compare memory usage:

```bash
mise query:diff -- fixtures/profiling/baseline.json fixtures/profiling/dedup.json --labels baseline,dedup --metric memory
```

All metrics in separate tables:

```bash
mise query:diff -- fixtures/profiling/baseline.json fixtures/profiling/dedup.json --labels baseline,dedup --all-metrics
```

N-way comparison (3+ files):

```bash
mise query:diff -- fixtures/profiling/v1.json fixtures/profiling/v2.json fixtures/profiling/v3.json --labels v1,v2,v3
```

CSV output for spreadsheets:

```bash
mise query:diff -- fixtures/profiling/baseline.json fixtures/profiling/dedup.json --format csv
```

Available metrics: `read_rows` (default), `read_bytes`, `memory`, `elapsed_ms`.

## Things to know

- ClickHouse inlines CTEs. A CTE referenced N times runs N times. There is no materialization.
- The profiler runs the full compile, execute, extract, mock-redaction, hydration pipeline.
- All resources are auto-approved (mock redaction). No Rails connection needed.
- A single GKG query can produce multiple ClickHouse queries: 1 base + N hydration queries. Each one is profiled independently.
- `--profile` runs `SYSTEM FLUSH LOGS` before querying `system.query_log`, which adds about 100ms.
- `--profile` and `--health` require the ClickHouse user to have SELECT on system tables.
- Default output format is `json` (compact). Use `--format pretty` for human-readable output.
