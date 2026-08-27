# SDLC indexer memory profiling

`crates/sdlc-profiler` measures peak memory of an SDLC **backfill** at a chosen scale. It seeds a
synthetic siphon datalake in ClickHouse, creates the graph schema from the ontology, then runs the
real SDLC handlers — extract, transform, and the ClickHouse writes — under the real worker pool,
sampling memory throughout.

It shares the sampler with `crates/dispatch-profiler` via `crates/memprofile`. See
[dispatcher memory profiling](dispatcher_memory_profiling.md) for that harness.

Findings from the first run of this harness:
[SDLC indexer backfill peak memory, August 2026](../reports/2026-08-sdlc-indexer-memory-profile.md).

## Why a separate harness

The dispatcher enumerates; the indexer materialises. A backfill's peak comes from the fan-out in
`Engine::run_handlers`: one namespace message spawns a task per registered entity handler, each
awaiting a slot in the `sdlc` concurrency group. Every running handler holds an extracted page, that
page's transformed output, and the next page read ahead of the writes. The profiler reproduces that
fan-out directly rather than standing up NATS, because the fan-out is the thing being measured.

## Run it

Start a ClickHouse the profiler can drop and recreate databases in. Give it room: 20 concurrent
page reads against a 6 GiB server will start failing with `MEMORY_LIMIT_EXCEEDED`, and a run with
handler failures is not comparable to one without.

```shell
CH_MEM_LIMIT=10g scripts/devtools/memprofile-clickhouse.sh up
```

Build and seed once, then run against the seeded data. The `memprofile` profile is release with
debug info kept:

```shell
cargo build -p sdlc-profiler --profile memprofile
./target/memprofile/sdlc-profiler --seed-only --rows-per-table 700000
./target/memprofile/sdlc-profiler --skip-seed --label base
```

Results land in `.memprofile/sdlc/<label>.json` (peaks, per-handler timings, graph row totals) and
`.memprofile/sdlc/<label>.samples.jsonl` (one line per sample, tagged with the phase).

The graph database is dropped and recreated on every run, including with `--skip-seed`, so each arm
writes into an empty database its output can be fingerprinted from.

## Shape flags

| Flag | Meaning |
|---|---|
| `--rows-per-table` | Rows seeded into each entity table. Needs to be a few multiples of the page size, or the paging loop and its read-ahead never run |
| `--namespaces` | Namespace messages dispatched concurrently. One namespace already saturates the concurrency group; more than one is for skew |
| `--projects-per-namespace` | Projects the rows are spread over, which sets how many distinct traversal paths a page carries |
| `--note-bytes` / `--description-bytes` / `--title-bytes` / `--text-bytes` | Per-column text widths. Text is what sizes a page, so these dominate peak far more than row counts do |
| `--datalake-batch-size` | Rows per page. Production derives 500k from the 32 GiB sdlc pool |
| `--sdlc-concurrency` | The `sdlc` concurrency group limit and the global worker budget. Production runs 20 |
| `--partition-min-rows` | Row count above which a plan takes the partitioned initial load. Lower it to exercise `Job`'s five partitions at a smaller scale |
| `--only` | Restrict the run to named entity targets |
| `--skip-global` | Profile the namespaced sweep alone, without the `User` and `Runner` pipelines |
| `--seed-only` / `--skip-seed` | Seed once, then A/B several binaries against the same data |
| `--seed-tables` | Reseed only these datalake tables. A partitioned plan only splits work once a partition holds more than one page, so measuring it needs one table far larger than the rest |

## What the seeder generates

Rows are produced server-side by `INSERT … SELECT … FROM numbers(N)`, so seeding never allocates in
the profiled process. Every table gets its own id space, and foreign-key columns are mapped by name
into the space of the table they reference, so the lookup joins in the generated extracts match rows
instead of quietly producing none.

Four things are shaped deliberately rather than merely being well-typed, because the pipelines that
consume them would otherwise take a cheap path:

- **Traversal paths** agree with `namespace_traversal_paths` and `project_namespace_traversal_paths`,
  so the namespaced extracts' `startsWith(traversal_path, …)` filter matches.
- **Note bodies** on system notes carry a GFM reference (`mentioned in #123`), so the `SystemNote`
  transform's parser finds references and the route-resolution lookups actually run.
- **`system_note_metadata.action`** is drawn from the actions the parser dispatches on. A random
  action is logged and dropped, which would measure the drop path.
- **`siphon_routes`** paths are the ones the seeded note bodies cite, so reference resolution
  resolves.

Half of `siphon_notes` is system notes and half user comments, so the `Note` and `SystemNote`
pipelines each get a full-width page.

## Reading the numbers

- `peaks.exact_max_footprint` is the kernel's own high-water mark, so it does not depend on the
  sampler catching the right instant. Prefer it over `peaks.max_footprint`.
- `peaks.exact_max_alloc_live` is bytes the program asked for at its peak. The spread against the
  footprint is allocator overhead, fragmentation and retention.
- `failures` must be empty. A handler that failed did less work, so its arm's peak is not comparable.
- `graph_rows` is a pre-merge part-level count and is a progress signal, not an output check. Use
  `scripts/devtools/memprofile-verify-output.py` for output equivalence.
- On macOS, RSS keeps decommitted pages that Linux drops, which is why the footprint and RSS columns
  differ. Compare like with like across runs, and confirm any steady-state RSS claim on Linux.
- The profiler builds with mimalloc `secure` by default because `orbit-server` does. Turning that
  off makes every number look better and none of them comparable to production.

## Measuring a change

Seed once, then measure each candidate on its own against one baseline:

```shell
./target/memprofile/sdlc-profiler --seed-only --rows-per-table 2000000 --namespaces 2

REPEATS=2 ROWS_PER_TABLE=2000000 scripts/devtools/memprofile-sdlc-isolate.sh \
  ./sp-baseline pagedrop=./sp-pagedrop noteborrow=./sp-noteborrow -- \
  --namespaces 2 --datalake-batch-size 500000 --sdlc-concurrency 20
```

Each arm is a binary built from the baseline plus exactly one change, so the
table it prints attributes the delta to that change rather than to a stack of
them. It ends with a metric-by-arm table and a per-arm output-identity verdict.

Every run gets an empty graph database, is fingerprinted, and has that database
dropped before the next run starts. Nothing is retained: a graph database left in
place merges tens of millions of rows in the background, and then the server, not
the indexer, is what runs out of memory — which truncates the next arm and makes
its peak meaningless. That failure is silent apart from the arm's `failures`
list, which is why the summary warns on any non-empty one. The baseline's
fingerprints are dumped to `baseline.fingerprint.json` so its database can be
dropped too, and each arm is compared against that file.

The verifier fingerprints every column of every graph table except the three that
are `Utc::now()` at index time (`_version`, and `indexed_at` and `watermark` on
the checkpoint). It reads `FINAL`, because two runs that wrote identical rows can
still differ in how much duplicate work merges have collapsed.

Check `graph rows` and the failure warnings before believing any peak: an arm
that wrote fewer rows did less work, and its peak is not a memory win.
