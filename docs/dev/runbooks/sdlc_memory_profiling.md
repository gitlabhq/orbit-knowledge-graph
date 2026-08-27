# SDLC indexer memory profiling

`crates/sdlc-profiler` measures peak memory of an SDLC **backfill** at a chosen scale. It seeds a
synthetic siphon datalake in ClickHouse, creates the graph schema from the ontology, then runs the
real SDLC handlers — extract, transform, and the ClickHouse writes — under the real worker pool,
sampling memory throughout.

It shares the sampler with `crates/dispatch-profiler` via `crates/memprofile`. See
[dispatcher memory profiling](dispatcher_memory_profiling.md) for that harness.

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

## A/B against a change

Seed once, then run two binaries over the same datalake into two graph databases:

```shell
./target/memprofile/sdlc-profiler --seed-only --rows-per-table 700000
./sp-before --skip-seed --label before --graph-database sdlc_graph_before
./sp-after  --skip-seed --label after  --graph-database sdlc_graph_after

scripts/devtools/memprofile-verify-output.py \
  --baseline-db sdlc_graph_before --candidate-db sdlc_graph_after
```

The verifier fingerprints every column of every graph table except the two that are `Utc::now()` at
index time (`_version`, and `indexed_at` on the checkpoint). It reads `FINAL`, because two runs that
wrote identical rows can still differ in how much duplicate work merges have collapsed.

Check `graph_rows` and `failures` agree between the arms before comparing peaks: a change that
writes fewer rows is not a memory win.
