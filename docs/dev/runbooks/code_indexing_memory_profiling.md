# Code Indexing Memory Profiling Runbook

Runs one repository through the production code-indexing handler on a laptop and
records where memory goes, stage by stage. The path exercised is the real one:
archive download over HTTP, streaming tar.gz extraction to a temp dir,
`code-graph` parse and resolve, Arrow conversion, and buffered writes into
ClickHouse.

Only the two Rails internal-API endpoints the indexer calls are mocked, and they
are served from pinned tarballs on disk, so consecutive runs index byte-identical
input. NATS is not involved: the profiler calls `CodeIndexingTaskHandler::handle`
directly with a mock lock service and a no-op progress notifier.

## Prerequisites

- Docker (Colima is fine; the script points `DOCKER_HOST` at the `default` profile)
- Roughly 400 MiB of disk for the corpus and 4 GiB free RAM per run

## One-time setup

```shell
mise memprofile:corpus     # fetch pinned elasticsearch and gitlab tarballs
mise memprofile:ch         # start ClickHouse on :18123
```

The corpus lives in `.memprofile/corpus/<project_id>.tar.gz`. Add a repository by
adding one `fetch` line to `scripts/devtools/memprofile-corpus.sh`; the ID is
arbitrary as long as it is unique.

| Project ID | Repository | Languages |
|---|---|---|
| 100001 | `elastic/elasticsearch` v9.0.0 | Java |
| 278964 | `gitlab-org/gitlab` v18.9.1-ee | Ruby, TypeScript, JavaScript, Go, and others |
| 100002 | `elastic/elasticsearch` `server/src/main/java` | Java, 4,145 files, sized so a dhat pass finishes |

## Running

```shell
mise memprofile:run -- --project-id 100001 --label es-baseline
mise memprofile:report .memprofile/runs/es-baseline
```

Reporting helpers over one run directory:

```shell
mise memprofile:report    .memprofile/runs/es-baseline   # timeline + peaks, writes report.md
mise memprofile:stages    .memprofile/runs/es-baseline   # per-stage structure tables
mise memprofile:vmregions .memprofile/es-baseline.footprint.txt
```

Each run drops and recreates the `gkg_memprofile` database, applies the
ontology-generated graph DDL for the current `config/SCHEMA_VERSION`, and writes
to `.memprofile/runs/<label>/`:

| file | contents |
|---|---|
| `samples.jsonl` | RSS, `phys_footprint`, live heap and cumulative allocation counters at `--sample-hz` (default 50) |
| `events.jsonl` | every `codegraph_mem` structure probe and pipeline log line, each stamped with the memory readings at that instant |
| `summary.json` | peaks, wall clock, config, ClickHouse row counts |
| `report.md` | rendered by `mise memprofile:report` |

Useful flags: `--worker-threads`, `--max-concurrent-languages` (1 makes language
families sequential so their peaks do not overlap), `--write-slice-rows`,
`--keep-schema`, `--cache-dir` (put the extraction scratch on a specific volume).

Repeat `--project-id` and raise `--indexing-lanes` to model a pod running
several repositories at once:

```shell
mise memprofile:run -- --project-id 100001 --project-id 278964 \
  --indexing-lanes 2 --label both-2lane
```

Each project gets its own traversal path, so their stale sweeps and checkpoints
stay independent.

## What each measurement means

- **live heap** is the sum of bytes the program asked the allocator for, tracked
  by a 64-shard counter wrapped around mimalloc. It excludes allocator overhead.
- **phys_footprint** is what macOS charges the process, and what jetsam kills on.
  It lags the live heap by roughly 1.5 s after a large structure is freed,
  because mimalloc holds the pages before returning them.
- **RSS** additionally counts mimalloc's decommitted-but-resident pages and
  clean file-backed pages, so it behaves as a high-water mark that never falls.
  Prefer `phys_footprint`.

The tracking allocator costs about 0-3% wall clock. Build with
`--no-default-features` to drop it and confirm that on a new workload.

Four of these numbers do not depend on the sampler being awake at the right
moment, and the report prints them next to the sampled ones:

- **exact peak live heap** comes from a high-water mark the allocator wrapper
  updates itself, every time a thread has moved 64 KiB. The residual error is one
  threshold per running thread, against a sampled peak that can miss a spike
  entirely: the sampler's real cadence is around 30 ms, not the nominal 50 Hz.
- **exact peak footprint** is `ri_lifetime_max_phys_footprint`, the kernel's own
  high-water mark for the number jetsam acts on.
- **peak commit** is mimalloc's high-water mark for bytes it asked the OS for.
  On macOS it runs well above footprint, because a commit is only charged once
  the page is touched.
- **size-class rounding** needs the `good-size` feature, which counts
  `mi_good_size` of every live block alongside the requested size. It costs
  around 2% wall clock, so it is a diagnostic mode rather than the default.

Tree-sitter allocates through C `malloc`, which no `GlobalAlloc` wrapper can see.
The profiler points it at the Rust allocator before anything parses, so parse
trees are on the same books as everything else. That is worth roughly 8M
allocations on a Python repository, or 40% of all allocation traffic, which
earlier runs did not count at all.

`orbit-server` builds mimalloc with `secure`, and the profiler does not. Use
`mise memprofile:run:secure` to measure under the configuration production
actually ships.

Runs take a lock on the ClickHouse database they use. Two profilers sharing one
database reset the schema under each other and write into each other's tables,
and the resulting row counts and timings look plausible rather than obviously
broken, so the second run refuses to start. Pass a different
`--clickhouse-database` to run two at once on purpose.

## Steady state and re-indexing

A single run measures a cold process indexing a project for the first time. It
cannot see either of the two things that set a long-lived pod's memory: the
stale sweep, which only runs when a checkpoint already exists, and whether
memory comes back between tasks.

```shell
mise memprofile:run -- --project-id 100001 --label es-steady \
  --repeat 3 --settle-secs 3 --flush-between-rounds
```

Each round uses the next task id, so rounds after the first re-index and take
the stale-sweep path. The report gains a settled-memory table with one row per
round. Read it as: live heap that climbs round on round means the pipeline is
holding something; live heap flat under a climbing footprint means the allocator
is holding pages. `--flush-between-rounds` drains the write buffer first, which
separates rows still pooled in the writer from either of those.

## Structure-level attribution

`code-graph` emits sizes on the `codegraph_mem` target at DEBUG. Every call site
is behind `memprobe::enabled()`, so a normal build never walks the graph. Stages:

| stage | what it reports |
|---|---|
| `inventory` | file inventory and per-family input lists |
| `parse_barrier` | every file's parse result held across the barrier: definitions, imports, `RefPack`, inferred returns |
| `graph_built` | the `CodeGraph` broken down by petgraph arrays, defs, imports, string pool and each resolution index, plus the `RefPack`s still carried into resolve |
| `resolve_barrier` | pending edge triples and failed chains held before merging into the graph |
| `before_convert` | the graph again, after cross-file edges are merged |
| `convert_entity` | Arrow bytes per entity as the conversion ramp builds, including the edge-routing `take` copy |
| `convert_column` | Arrow bytes per column, which is how the tag lists and the envelope columns were found |
| `converted` | Arrow bytes per destination table |

## Allocation-site attribution

```shell
mise memprofile:dhat -- --project-id 100002 --label esjava-dhat --sample-hz 10 \
  --max-concurrent-languages 1 \
  --per-file-timeout-ms 0 --per-file-parse-timeout-ms 0 \
  --per-file-walk-timeout-ms 0 --per-file-ssa-timeout-ms 0 \
  --cross-file-resolve-timeout-ms 0
```

Use project 100002, not 100001. dhat costs 113x wall clock on this workload
(3.8 s becomes 429 s on the reduced corpus), because it removes every freed
block from a live-block table and the full Java tree frees tens of millions of
them. A full-repository pass did not reach its parse barrier in 20 minutes.

This swaps the global allocator for dhat's and writes `dhat-heap.json` in the run
directory; open it at <https://nnethercote.github.io/dh_view/dh_view.html>, or
roll it up on the command line:

```shell
mise memprofile:dhat:report .memprofile/runs/es-dhat/dhat-heap.json
```

Sort by "At t-gmax" to see which call stacks hold the heap at its peak. Expect
roughly 30x slowdown and different absolute footprint numbers, since dhat wraps
the system allocator rather than mimalloc. Use it for composition, not totals.

Two things distort a dhat run if you leave them alone. Pass `0` for every
per-file timeout, or the slowdown trips the CPU budgets and most files abort
mid-parse. Pass `--max-concurrent-languages 1`, or dhat's global lock reshapes
how the two concurrent language families overlap, and the peak it reports is a
peak that never happens in production.

## Region-level attribution

Poll `footprint(1)` alongside a run to see the parts the allocator counter
cannot: thread stacks, page tables, allocator metadata.

```shell
scripts/devtools/memprofile-footprint.sh /tmp/es.footprint.txt 1 &
mise memprofile:run -- --project-id 100001 --label es-baseline
mise memprofile:vmregions /tmp/es.footprint.txt
```

## Comparing a change against a baseline

Keep the `es-baseline` and `gl-baseline` runs, then for each candidate:

```shell
mise memprofile:ab <label>       # builds, indexes both repos, diffs both
mise memprofile:regress          # unit + code-indexing + code-graph suites
```

The comparison fails on a row-count difference or a wall clock more than 7%
above baseline, and prints per-structure deltas from the probes.

Measured run-to-run spread over three repeats on an M3 Max: peak live heap +/-1%
(Elasticsearch) and +/-3% (GitLab), wall clock +/-3%, peak footprint +/-7%.
Allocation counts reproduce exactly, so they are the sharpest signal for a change
that removes allocations. Treat a live-heap move under 3% as noise.

## Proving output is unchanged

`mise memprofile:verify <project-id> <name>` indexes one corpus repository with
two driver binaries into two databases and fingerprints every row of every
table. Build the binaries first, one per revision:

```shell
git checkout <revision>
git checkout <profiler-branch> -- crates/code-index-profiler Cargo.toml Cargo.lock
cargo build -p code-index-profiler --profile memprofile
cp target/memprofile/code-index-profiler /tmp/cip-base   # or /tmp/cip-opt
```

It disables the per-file CPU budgets, and it has to. At production budgets a
large repository is not reproducible: on the Linux kernel the same binary run
twice aborted 17 files once and 15 the next, and wrote different definition,
import and edge counts. Pass `--with-budgets` to see that for yourself.

## Findings from the first run

[2026-08 code indexing memory profile](../reports/2026-08-code-indexing-memory-profile.md)
records the Elasticsearch and GitLab baselines this harness was built for, the
eight changes that took peak live heap down 56.5% and 35.6%, and the two that
were measured and reverted.

## Tearing down

```shell
mise memprofile:ch down
rm -rf .memprofile/runs .memprofile/scratch
```
