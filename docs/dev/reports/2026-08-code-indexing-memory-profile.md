# Code Indexing Memory Profile, August 2026

Where the code-indexing path spends memory, from archive download through the
ClickHouse write, measured for `elastic/elasticsearch` v9.0.0 (Java) and
`gitlab-org/gitlab` v18.9.1-ee (Ruby and TypeScript).

The first half records what was measured and how, against the code as it stood
before this work. The second half records the eight changes that came out of it,
each measured on its own, and the two that were measured and reverted.

Headline: peak live heap falls **56.5%** on Elasticsearch and **35.6%** on
GitLab, with the pipeline no slower and every row count identical.

## Tooling

Host: Apple M3 Max, 14 cores, 36 GiB, macOS 26 (Darwin 25.6), Command Line Tools
without full Xcode. Rust 1.95, `aarch64-apple-darwin`. ClickHouse 26.2 in Colima.

Two facts about this host decide the tool choice.

`orbit-server` installs mimalloc as the global allocator. mimalloc maps its own
arenas with `mmap` instead of going through libmalloc, so every macOS tool built
on the `malloc_logger` hook is blind to the Rust heap: Instruments' Allocations
template, `leaks`, `heap`, and `malloc_history` all see an almost empty process.
Instruments is doubly unavailable here because `xctrace` needs full Xcode, and
Command Line Tools alone does not ship it.

The Linux heap profilers are not options either. Valgrind has no
`aarch64-darwin` target, heaptrack and bytehound are Linux only, and jemalloc's
`prof` machinery does not work on macOS, which is why `jemalloc_pprof` documents
Linux-only support.

What is left works well.

| Tool | Available | What it gives | Cost |
|---|---|---|---|
| `dhat-rs` 0.3.3 | Yes | Bytes live at the global peak, per allocation call stack, symbolicated to file and line | 113x wall clock, measured |
| Tracking global allocator | Yes | Live and cumulative requested bytes, continuously | 0 to 3% wall clock |
| `proc_pid_rusage(RUSAGE_INFO_V4)` | Yes | `resident_size` and `phys_footprint` | About 1 microsecond per read |
| `footprint(1)` | Yes | Dirty bytes per VM region: stacks, page tables, allocator metadata | One fork per sample |
| Structure probes in `code-graph` | Added | Named domain structures and their byte counts at each pipeline barrier | Zero unless the target is enabled |
| Instruments, `leaks`, `heap`, `malloc_history` | No | Blind to mimalloc; `xctrace` also needs full Xcode | n/a |
| Valgrind DHAT, massif, heaptrack, bytehound | No | No `aarch64-darwin` support | n/a |
| jemalloc `prof` | No | Heap profiling unsupported on macOS | n/a |
| `mise perf:rss`, `/usr/bin/time -l` | Yes | One max-RSS number for the whole process | Negligible |

One detail makes `footprint(1)` usable despite mimalloc. mimalloc tags its
`mach_vm_allocate` regions with `mi_option_os_tag`, which defaults to 100, and
100 is `VM_MEMORY_IOACCELERATOR` in `mach/vm_statistics.h`. So the Rust heap
appears in `footprint` and `vmmap` output under the `IOAccelerator` category:
2,488 MiB of it at the Elasticsearch peak, against 22 MiB across all the
`MALLOC_*` categories that libmalloc-based tools can see.

The working combination is all five available rows together, because each one
answers a different question. The tracking allocator and `proc_pid_rusage` say
**when**; the structure probes say **which structure**; dhat says **which call
stack**; `footprint(1)` says **which VM region**, catching the parts that never
touch the allocator at all.

## Harness

`crates/code-index-profiler` is a dev-only binary that drives
`CodeIndexingTaskHandler::handle`, the same handler the indexer runs, against a
corpus archive server and a throwaway ClickHouse database. The full path runs:
project-info lookup, archive download over HTTP, streaming tar.gz extraction to
a temp dir with the production file filter, `code-graph` parse and resolve,
`IndexerConverter` Arrow conversion, `BufferedWriter` coalescing, Arrow-IPC
insert into ClickHouse, stale sweep, and checkpoint.

Two things are substituted, both deliberately:

- **The Rails internal API.** An axum server implements the only two endpoints
  the code indexer calls, `/api/v4/internal/orbit/project/{id}/info` and
  `/repository/archive`, streaming a pinned tarball from disk. Compared with
  running GDK and Gitaly, this makes each run index byte-identical input, which
  is what makes A/B comparison meaningful. It is the same substitution the
  existing `bench/mock-git-server` makes on GKE.
- **NATS.** The profiler calls the handler directly with `MockNatsServices`,
  `MockLockService` and a no-op progress notifier. Everything downstream of the
  message (lock, fetch, lane acquisition, index, write, checkpoint) is the
  production code path.

ClickHouse is real: a container on `:18123`, with the database dropped and
recreated from the ontology-generated DDL for the current `SCHEMA_VERSION` on
every run, so writes are measured against the real table set and the real
Arrow-IPC insert path.

Full instructions: [`docs/dev/runbooks/code_indexing_memory_profiling.md`](../runbooks/code_indexing_memory_profiling.md).

## Method and fidelity

Three signals are recorded per run.

**Continuous.** A sampler thread reads `proc_pid_rusage` and the allocator
counters at 50 Hz into `samples.jsonl`. The allocator counter is a wrapper over
mimalloc that adds the requested size to one of 64 cache-line-padded shards,
picked by thread. Sharding matters: with 14 rayon workers a single global
counter would cost more than the allocations it measures. The sampler sums the
shards, so there is no `fetch_max` on the hot path and peak resolution is one
sample interval.

**At each pipeline barrier.** `crates/code-graph/src/v2/memprobe.rs` walks the
live structures and emits their byte counts on the `codegraph_mem` tracing
target at DEBUG. Every call site is guarded by `memprobe::enabled()`, so a build
that does not enable the target never walks the graph. Sizes are computed from
`capacity()`, not `len()`, and include hashbrown control bytes, SmallVec spills
and the string arenas. The tracing layer stamps each probe with the allocator
and process readings taken at that instant, which is what lets a structure table
be read next to a point on the timeline.

**Per allocation stack.** A separate build swaps in dhat's allocator and writes
`dhat-heap.json`, which records bytes live at the global peak per stack.

Fidelity notes, all measured rather than assumed:

- The tracking allocator costs 3.1% on Elasticsearch (24.68 s versus 23.93 s)
  and 0.0% on GitLab (38.88 s versus 38.89 s). Peak footprint moved by under 3%
  in both directions, inside run-to-run noise.
- dhat runs need every per-file CPU budget set to 0. At production settings
  (`per_file_parse_timeout_ms: 100` and siblings), dhat's slowdown trips them and
  19,483 of 24,208 parseable Elasticsearch files abort mid-parse. The first dhat
  attempt reported an 84 MiB peak for a run that indexed 195k rows instead of
  4.1M. Baseline runs have zero aborts, so disabling the budgets does not change
  the baseline.
- dhat serialises allocations on a global lock, which changes how the two
  concurrent language families overlap. Its byte attribution is sound; its
  timeline and its absolute totals are not comparable to a baseline run.
- The structure probes cover the named structures only. At the Elasticsearch
  Java `before_convert` barrier they account for 582 MiB of an 893 MiB live
  heap, and at the global peak 2,237 MiB of 2,563 MiB. What is left over is
  buffered writer slices from families already converted, Arrow builder
  headroom, the ClickHouse client and the tokio runtime.
- All numbers are **per repository**. Production indexer pods run several
  indexing lanes at once and budget 2.5 GiB per in-flight lane
  (`WORKER_MEMORY_BUDGET_BYTES` in `crates/orbit-server-config/src/resources.rs`).
- The host has 14 cores against production pod requests of 4 to 8. The `-4cpu`
  runs cover that: they are 27% slower on Elasticsearch and 18% on GitLab, and
  land within 2% of the baseline peak.

## Runs

All figures in the sections that follow describe the **baseline**, before the
changes in [Confirmed reductions](#confirmed-reductions).

Six runs, one per configuration, each indexing one repository end to end into an
empty database. `worker_threads: 0` means one rayon worker per core (14 here);
`max_concurrent_languages: 0` means the pipeline default of 2.

| Run | Repository | Configuration |
|---|---|---|
| `es-baseline` | Elasticsearch | production defaults |
| `es-seq` | Elasticsearch | `max_concurrent_languages 1` |
| `es-4cpu` | Elasticsearch | `worker_threads 4` |
| `gl-baseline` | GitLab | production defaults |
| `gl-seq` | GitLab | `max_concurrent_languages 1` |
| `gl-4cpu` | GitLab | `worker_threads 4` |

| Run | Wall clock | Peak RSS | Peak footprint | Peak live heap | Cumulative allocated | Allocations |
|---|---:|---:|---:|---:|---:|---:|
| `es-baseline` | 24.9 s | 3,178 MiB | 2,756 MiB | 2,563 MiB | 14.3 GiB | 138 M |
| `es-seq` | 24.1 s | 3,157 MiB | 2,814 MiB | 2,528 MiB | 14.3 GiB | 138 M |
| `es-4cpu` | 31.6 s | 3,035 MiB | 2,810 MiB | 2,563 MiB | 14.3 GiB | 138 M |
| `gl-baseline` | 38.9 s | 1,809 MiB | 1,623 MiB | 1,238 MiB | 15.4 GiB | 116 M |
| `gl-seq` | 39.8 s | 1,527 MiB | 1,464 MiB | 1,108 MiB | 15.4 GiB | 116 M |
| `gl-4cpu` | 45.8 s | 1,632 MiB | 1,470 MiB | 1,218 MiB | 15.4 GiB | 116 M |

Elasticsearch peaks higher than GitLab despite being half the size on disk,
because Java produces far more resolved edges per file. Neither concurrency knob
moves the Elasticsearch peak: it is set by one language family's conversion, and
serialising the families or quartering the workers does not change that family's
own footprint. On GitLab the two knobs shave 10 to 11%, because there the peak
does overlap two families.

| Repository | Archive | Retained on disk | Files | Parseable | Families | Rows written |
|---|---:|---:|---:|---:|---:|---:|
| Elasticsearch v9.0.0 | 88 MiB | 282 MiB | 31,281 | 24,208 | 6 | 4,103,145 |
| GitLab v18.9.1-ee | 168 MiB | 569 MiB | 93,867 | 65,332 | 7 | 2,504,626 |

## Results: Elasticsearch

Windows, `es-baseline`:

| Window | Start | End | Peak footprint | Peak live heap |
|---|---:|---:|---:|---:|
| Download and extract | 0.8 s | 9.4 s | 33 MiB | 12 MiB |
| Parse, resolve, convert | 9.4 s | 20.6 s | 2,756 MiB | 2,563 MiB |
| Flush | 20.6 s | 24.7 s | 1,192 MiB | 966 MiB |

Downloading 88 MiB and writing 282 MiB of source to disk costs 12 MiB of heap.
The archive is streamed through `SyncIoBridge` into `extract_tar_gz`, one tar
entry at a time, and the file inventory that survives is 4 MiB.

Everything after that is one language family, Java, with 22,935 of the 24,208
parseable files. The other five families together never hold more than a few MiB.

### Parse barrier, t = 12.8 s, live heap 517 MiB

Every file's parse result is held simultaneously until the sequential graph
build consumes it.

| Structure | MiB | Share |
|---|---:|---:|
| `CanonicalDefinition` (name, FQN, metadata) | 189.3 | 39% |
| `RefPack` (packed refs plus per-file string arena) | 176.4 | 36% |
| `CanonicalImport` (path, name, alias) | 103.7 | 21% |
| Inferred returns, unresolved aliases, extensions | 17.1 | 4% |
| **Probed total** | **486.5** | 100% |

434,495 definitions and 362,752 imports across 22,935 files. That is 457 bytes
per definition and 300 bytes per import, held in owned `String`s before the
graph interns them.

### Graph built, t = 13.2 s, live heap 750 MiB

The definitions and imports have moved into the graph and been interned; the
`RefPack`s have not, and are carried into resolve.

| Structure | MiB | Share |
|---|---:|---:|
| `StringPool` (arena, offsets, intern map) | 96.3 | 20% |
| petgraph node array | 96.0 | 20% |
| `GraphDef` array | 81.2 | 17% |
| `GraphImport` array | 52.2 | 11% |
| petgraph edge array | 42.0 | 9% |
| Index: scope to member to nodes | 29.0 | 6% |
| Index: file to definition byte ranges | 28.8 | 6% |
| Boxed `GraphDefMeta` | 28.0 | 6% |
| Index: FQN to definition nodes | 19.7 | 4% |
| Index: bare name to definition nodes | 11.0 | 2% |
| **Graph total** | **484.3** | 100% |

A further 179.4 MiB of `RefPack`s and per-file node index vectors is still
carried outside the graph, waiting for resolve.

828,736 nodes and 550,342 interned strings. The petgraph node array is 96 MiB
for 828,736 nodes because capacity rounds to 1,048,576 slots of 96 bytes:
88 bytes of `GraphNode` plus petgraph's two link indices. The four resolution
indexes together are 88.5 MiB, 18% of the graph.

### Resolve barrier, t = 14.7 s, live heap 854 MiB

Every file's resolved edges and unresolved chains are held before merging.

| Structure | MiB | Share |
|---|---:|---:|
| Failed chains queued for phase 3 | 215.4 | 75% |
| Resolved `EdgeTriple`s awaiting merge | 71.4 | 25% |
| **Probed total** | **286.8** | 100% |

3,109,083 edges pending. The failed-chain queue is three times the size of the
edges it failed to produce, because each entry keeps an owned name, its
expression chain and its reaching SSA values.

### Before conversion, t = 15.6 s, live heap 893 MiB

| Structure | MiB | Share |
|---|---:|---:|
| petgraph edge array | 168.0 | 29% |
| `StringPool` | 96.3 | 17% |
| petgraph node array | 96.0 | 17% |
| `GraphDef` array | 81.2 | 14% |
| `GraphImport` array | 52.2 | 9% |
| Index: scope to member to nodes | 29.0 | 5% |
| Boxed `GraphDefMeta` | 28.0 | 5% |
| Index: FQN to definition nodes | 19.7 | 3% |
| Index: bare name to definition nodes | 11.0 | 2% |
| **Graph total** | **581.5** | 100% |

Edges grew from 1,260,550 to 4,416,851, taking the edge array from 42 to 168 MiB
as capacity doubled twice. The definition-range index has been cleared.

### Conversion, t = 15.6 s to 17.2 s: the global peak

`IndexerConverter::convert` takes the graph by value and materialises every
entity as an Arrow `RecordBatch` before any of it is written or the graph is
dropped. Each row is the reading taken at that step.

| t | Step | Rows | Arrow MiB | Live heap MiB | Footprint MiB |
|---:|---|---:|---:|---:|---:|
| 15.6 s | `assign_ids` node ID vector | 828,736 | 6.3 | 899 | 1,098 |
| 15.7 s | definitions batch | 434,495 | 167.8 | 1,067 | 1,231 |
| 15.8 s | imported symbols batch | 362,752 | 111.4 | 1,178 | 1,286 |
| 16.8 s | edges batch | 4,385,362 | 716.6 | 1,895 | 2,024 |
| 16.8 s | cast `relationship_kind` to Utf8 | 4,385,362 | 40.0 | 1,929 | 2,048 |
| 17.1 s | `take_record_batch` for `gl_code_edge` | 4,385,362 | 602.8 | **2,563** | **2,745** |
| 17.2 s | graph and source batches dropped | 0 | 0.0 | 1,059 | 2,498 |

The peak is the edge-routing copy. Every Java edge routes to one table,
`gl_code_edge`, but the single-table fast path in `IndexerConverter::convert`
only applies when that table is the default `gl_edge`, so the routing loop runs
`take_record_batch` over all 4,385,362 rows. `take` copies; Arrow's `slice` and
`project` do not. For 0.3 s the process holds the 581 MiB graph, the 168 MiB
definitions batch, the 111 MiB imports batch, the 717 MiB edges batch, the 40 MiB
cast column, the 603 MiB copy and a 17 MiB index array at once.

Those seven add to 2,237 MiB of the 2,563 MiB live heap. The remaining 326 MiB
is Arrow builder headroom, the `BufferedWriter` slices already queued from the five
small families, the ClickHouse client and the tokio runtime.

The structural graph is converted after every family, and is small: 41,739 nodes,
20 MiB of graph, 7.7 MiB of `gl_file` and 11.8 MiB of `gl_edge` Arrow. Its edges
all route to the default table, so it takes the zero-copy `project` path.

### Arrow totals

| Family | Table | Arrow MiB |
|---|---|---:|
| `java` | `v91_gl_code_edge` | 602.8 |
| `java` | `v91_gl_definition` | 167.8 |
| `java` | `v91_gl_imported_symbol` | 111.4 |
| `structural` | `v91_gl_file` | 7.7 |
| **all** | **total** | **889.7** |

### Flush

Live heap enters the flush window at 923 MiB, which is the Arrow batches still
referenced by the `BufferedWriter`. `write_slice_rows` is 1,000,000, so the
4.39M-row edge batch is submitted as five slices, but `RecordBatch::slice` is
zero-copy: all five point into the same 603 MiB of buffers and none of it is
freed until the last slice has been inserted. The insert itself streams,
`insert_arrow_streaming_with_sql` drains the Arrow-IPC writer after every batch,
so the LZ4 payload never accumulates.

## Results: GitLab

| Window | Start | End | Peak footprint | Peak live heap |
|---|---:|---:|---:|---:|
| Download and extract | 0.8 s | 20.2 s | 53 MiB | 19 MiB |
| Parse, resolve, convert | 20.2 s | 35.5 s | 1,623 MiB | 1,238 MiB |
| Flush | 35.5 s | 38.7 s | 721 MiB | 447 MiB |

Extraction is half the wall clock and 19 MiB of heap. 93,867 files land on disk
and the inventory plus per-family path lists are 16 MiB.

Two families matter and they overlap: JavaScript and TypeScript (17,666 files,
custom `JsPipeline`) and Ruby (47,282 files, `FamilyPipeline`). The remaining
five families total 384 files.

### JavaScript and TypeScript, graph built at t = 21.1 s, live heap 635 MiB

| Structure | MiB | Share |
|---|---:|---:|
| petgraph node array | 48.0 | 26% |
| `GraphDef` array | 27.6 | 15% |
| `StringPool` | 24.7 | 13% |
| Boxed `GraphDefMeta` | 19.1 | 10% |
| Index: file to definition byte ranges | 17.7 | 9% |
| `GraphImport` array | 16.5 | 9% |
| petgraph edge array | 10.5 | 6% |
| Index: FQN to definition nodes | 10.1 | 5% |
| Index: scope to member to nodes | 7.4 | 4% |
| Index: bare name to definition nodes | 5.6 | 3% |
| **Graph total** | **187.2** | 100% |

377,325 nodes, 245,223 definitions. `JsPipeline` holds its per-file analyses,
not `RefPack`s, so there is no equivalent carried-refs number; the 448 MiB
between the probed graph and the live heap at this moment is those analyses plus
the concurrently running Ruby family.

### Ruby, parse barrier at t = 24.5 s, live heap 486 MiB

| Structure | MiB | Share |
|---|---:|---:|
| `RefPack` (packed refs plus per-file string arena) | 156.4 | 63% |
| `CanonicalDefinition` | 58.8 | 24% |
| `CanonicalImport` | 22.8 | 9% |
| Inferred returns, unresolved aliases, extensions | 9.4 | 4% |
| **Probed total** | **247.4** | 100% |

The shape is inverted from Java: Ruby produces 47,282 files with only 216,871
definitions but many more references per definition, so `RefPack` dominates.

### Ruby, resolve barrier at t = 25.2 s, live heap 790 MiB

| Structure | MiB | Share |
|---|---:|---:|
| Failed chains queued for phase 3 | 331.9 | 95% |
| Resolved `EdgeTriple`s awaiting merge | 16.0 | 5% |
| **Probed total** | **347.8** | 100% |

This is the largest single transient in the GitLab run. 715,922 edges resolved,
and the chains that did not resolve carry 332 MiB into phase 3, twenty times the
size of the edges that did.

### Conversion

| t | Family | Step | Rows | Arrow MiB | Live heap MiB |
|---:|---|---|---:|---:|---:|
| 22.2 s | `java_script` | definitions | 245,223 | 72.1 | 773 |
| 22.2 s | `java_script` | imported symbols | 108,570 | 33.2 | 808 |
| 22.3 s | `java_script` | edges | 544,429 | 91.6 | 913 |
| 22.4 s | `java_script` | `take` for `gl_code_edge` | 544,429 | 74.8 | 998 |
| 26.2 s | `ruby` | definitions | 216,871 | 57.3 | 831 |
| 26.2 s | `ruby` | imported symbols | 46,444 | 10.4 | 842 |
| 26.5 s | `ruby` | edges | 1,296,632 | 210.8 | 1,052 |
| 26.6 s | `ruby` | `take` for `gl_code_edge` | 1,296,632 | 178.2 | **1,248** |
| 26.8 s | `structural` | files | 93,867 | 17.6 | 560 |
| 26.8 s | `structural` | edges | 229,518 | 30.4 | 590 |
| 26.8 s | `structural` | `take` for `gl_edge` | 114,823 | 11.9 | 605 |
| 26.8 s | `structural` | `take` for `gl_code_edge` | 114,695 | 14.7 | 617 |

Same shape, smaller numbers: the peak is the Ruby edge-routing copy. The GitLab
structural graph splits its edges across two tables, so both `take` calls run.

| Family | Table | Arrow MiB |
|---|---|---:|
| `ruby` | `v91_gl_code_edge` | 178.2 |
| `java_script` | `v91_gl_code_edge` | 74.9 |
| `java_script` | `v91_gl_definition` | 72.2 |
| `ruby` | `v91_gl_definition` | 57.3 |
| `java_script` | `v91_gl_imported_symbol` | 33.3 |
| `structural` | `v91_gl_file` | 17.6 |
| `structural` | `v91_gl_code_edge` | 14.7 |
| `ruby` | `v91_gl_imported_symbol` | 10.4 |
| **all** | **total** | **458.6** |

## Two repositories on two lanes

A production pod runs several indexing lanes at once. One run indexed both
repositories concurrently with `--indexing-lanes 2`.

| Run | Wall clock | Peak RSS | Peak footprint | Peak live heap |
|---|---:|---:|---:|---:|
| `es-baseline` alone | 24.9 s | 3,178 MiB | 2,756 MiB | 2,563 MiB |
| `gl-baseline` alone | 38.9 s | 1,809 MiB | 1,623 MiB | 1,238 MiB |
| `both-2lane` | 50.6 s | 3,753 MiB | 2,904 MiB | 2,553 MiB |

The combined peak is 2,904 MiB, not the 4,379 MiB the two solo peaks sum to,
because the two peaks do not coincide: Elasticsearch reaches its edge-routing
copy at t = 19.6 s while GitLab is still extracting its archive, and the GitLab
lane does not reach its own until t = 32.9 s.

The lanes are not independent, though. The JavaScript and Ruby conversions in
the GitLab lane run at 1,899 MiB and 2,162 MiB of live heap in the combined run,
against 998 MiB and 1,248 MiB when GitLab runs alone. The extra 900 MiB is Arrow
batches from the Elasticsearch lane, still referenced by the `BufferedWriter` and
not yet inserted. Whether
two lanes fit in one pod is a question about when their conversions land relative
to each other, not about the sum of their peaks.

## Allocation-site cross-check

The structure probes are hand-written accounting, so they were checked against
dhat, which counts what the allocator actually handed out. A full Elasticsearch
pass under dhat did not finish: the Java family alone ran for over 20 minutes
without reaching its parse barrier, because dhat has to remove every freed block
from its live-block table and this workload frees tens of millions of blocks.

The check was run on a reduced corpus instead: `server/src/main/java` from the
same tag, 4,145 Java files and 39 MiB, registered as project 100002. That run
takes 3.8 s natively and 429 s under dhat, a 113x slowdown, and peaks at 289 MiB
live heap natively against 343 MiB reported by dhat at its own t-gmax. The
difference is expected: dhat wraps the system allocator, not mimalloc, and its
lock reshapes the timing.

Live at the global peak, rolled up by the innermost frame in first-party or
Arrow code:

| MiB | Share | Blocks | Frame |
|---:|---:|---:|---|
| 77.5 | 22.6% | 25 | `GenericByteBuilder::with_capacity` (`generic_bytes_builder.rs:53`) |
| 60.3 | 17.6% | 23,706 | `CodeGraph::add_file_with_language` |
| 37.3 | 10.9% | 7 | `MutableBuffer::with_capacity` |
| 32.6 | 9.5% | 30 | `PrimitiveBuilder::with_capacity` |
| 27.1 | 7.9% | 11,015 | `hashbrown RawTable::reserve_rehash` |
| 21.5 | 6.3% | 7 | `ScalarBuffer::from_iter` |
| 16.6 | 4.9% | 3,224 | `FamilyPipeline::run` phase 2 edge vectors (`pipeline.rs:1561`) |
| 13.5 | 3.9% | 37 | `GenericByteBuilder::with_capacity` (`generic_bytes_builder.rs:50`) |
| 12.7 | 3.7% | 8 | `arrow_select::take::take_bytes` and `take_list` |
| 7.5 | 2.2% | 1 | `StringPool::alloc` |
| 6.4 | 1.9% | 75,942 | `GraphDef::from_canonical` |

Arrow construction (`GenericByteBuilder`, `PrimitiveBuilder`, `MutableBuffer`,
`ScalarBuffer`, `GenericListBuilder`) is 186.9 MiB, 54% of the peak. The graph
(`add_file_with_language`, `reserve_rehash`, `StringPool::alloc`,
`GraphDef::from_canonical`) is 101.3 MiB, 29%. `arrow_select::take` is 12.7 MiB,
which is small here only because this corpus has 29% of the full repository's
Java files and correspondingly fewer edges.

Rolled up by the outermost frame, 90.6% of the peak sits under
`registry::dispatch_family`, which is the whole per-family pipeline. The three
largest single stacks are the edge `BatchBuilder`
(`convert_semantic_edges` at `arrow_converter.rs:503`, 31.6 MiB in 2 blocks),
`take_record_batch` (31.3 MiB in 2 blocks), and the definitions `BatchBuilder`
(`convert_definitions`, 27.0 MiB in 6 blocks).

Both methods land on the same conclusion: at the peak the process is holding a
`CodeGraph` and a full Arrow copy of it at the same time.

## Cross-cutting observations

**Ingest is free, analysis is not.** Download plus extraction peaks at 12 MiB of
heap for Elasticsearch and 19 MiB for GitLab, against 282 MiB and 569 MiB of
source written to disk. Nothing in the fetch path buffers a repository.

**Memory tracks graph size, not source size.** GitLab is twice Elasticsearch on
disk and three times its file count, and peaks at less than half the heap.
4.4 M Java edges cost more than 1.3 M Ruby edges plus 0.5 M JavaScript edges.

**Three plateaus per family, then a spike.** Every family holds all of its parse
results across a barrier, then all of its graph plus the refs carried into
resolve, then all of its resolved edges and failed chains before merging. Each
plateau is 200 to 500 MiB for a large family. The spike on top is conversion.

**The peak is the edge-routing copy, in both repositories.** Live heap at the
`take_record_batch` call is 2,563 MiB (Elasticsearch) and 1,248 MiB (GitLab), and
drops by 1,400 MiB and 630 MiB the moment the graph and the source batches go out
of scope. `take` is the only copying operation in the conversion path; `slice`
and `project` share buffers.

**mimalloc returns pages about 1.5 s after the heap frees them.** On
Elasticsearch the graph is dropped at t = 17.2 s and live heap falls from
2,412 to 1,050 MiB, but `phys_footprint` stays at 2,501 MiB until t = 18.7 s,
when it drops to 925 MiB in one step. For 1.5 s the process is charged 1,593 MiB
it is no longer using. GitLab shows the same shape with a 981 MiB gap. A pod is
charged the footprint, and the footprint follows the peak with a lag.

**RSS never comes back down.** Elasticsearch RSS reaches 3,172 MiB at the peak
and stays there for the rest of the run, including after footprint has fallen to
925 MiB, because mimalloc's decommitted pages remain resident and reclaimable.
`mise perf:rss`, the measurement the repository had before this work, reads a
high-water mark that overstates the true charge by 3x.

**Thread stacks are not a factor.** Rayon workers get 8 MiB stacks and the run
peaks at 75 threads, 600 MiB reserved. Peak dirty stack is 5.5 MiB on
Elasticsearch and 2.7 MiB on GitLab, because almost none of that reservation is
touched.

**Lanes interact through the writer, not through their peaks.** Two lanes peak at
2,904 MiB rather than the 4,379 MiB their solo peaks sum to, because their
conversions land at different times. What does carry across lanes is the Arrow
batches waiting in the `BufferedWriter`: they add 900 MiB to the other lane's
conversion. Production budgets 2.5 GiB per in-flight lane, and a single
Elasticsearch-shaped repository takes 2,756 MiB of footprint on its own.

**The instrumentation combination is cheap; dhat is not.** The tracking
allocator plus the structure probes cost 0 to 3% and run on the full corpus.
dhat costs 113x and does not finish a full Elasticsearch pass, so it is a
reduced-corpus cross-check rather than a routine measurement.

## Confirmed reductions

Every change below was measured with the harness against the same two corpus
archives, one change at a time, and kept only when it moved peak live heap by
more than the measured noise band without regressing row counts or wall clock.
Run-to-run spread with the final change set is 0 MiB on Elasticsearch (1,114 MiB
three times) and 4 MiB on GitLab.

| Workload | Peak live heap | Peak footprint | Peak RSS | Wall clock |
|---|---|---|---|---|
| Elasticsearch | 2,563 -> **1,114 MiB (-56.5%)** | 2,756 -> 1,399 (-49.2%) | 3,178 -> 1,751 (-44.9%) | -6.2% |
| GitLab | 1,238 -> **798 MiB (-35.6%)** | 1,623 -> 1,089 (-32.9%) | 1,809 -> 1,360 (-24.8%) | -0.9% |
| Both, two lanes | 2,553 -> **1,373 MiB (-46.2%)** | 2,904 -> 1,863 (-35.8%) | 3,753 -> 2,216 (-41.0%) | -3.4% |

Verified on four further repositories in
[Cross-repository verification](#cross-repository-verification), including the
Linux kernel, with byte-identical output everywhere.

Cumulative allocated bytes fall 12.4% (Elasticsearch) and 6.6% (GitLab); the
allocation *count* is unchanged to within 0.3%, so the win is smaller
allocations, not fewer of them.

Ordered by measured effect on the Elasticsearch peak:

| Change | Elasticsearch | GitLab | Where |
|---|---:|---:|---:|
| Skip the edge-routing copy when one table takes every row | -550 MiB | -112 MiB | `arrow_converter.rs` |
| Dictionary-encode the denormalized tag lists | -300 MiB | -44 MiB | `arrow.rs`, `arrow_converter.rs` |
| Free resolution indexes and the pool dedup map before conversion | -141 MiB | -61 MiB | `graph.rs`, `state.rs`, `pipeline.rs` |
| `shrink_to_fit` the parse and resolve outputs | -222 MiB | -175 MiB | `pipeline.rs` |
| Box the large `GraphNode` variants | -98 MiB | -34 MiB | `graph.rs` |
| Dictionary-encode envelope-constant columns | -92 MiB | -1 MiB | `arrow_converter.rs` |
| Pre-size the graph containers from known counts | -90 MiB | -34 MiB | `graph.rs`, `pipeline.rs` |
| Pre-size dictionary key buffers | -47 MiB | -18 MiB | `arrow.rs` |

The per-change figures are each measured against the state before that change,
so they do not sum to the total exactly: freeing the resolution indexes, for
instance, is worth less once the edge-routing copy is gone.

### What each change does

**Skip the edge-routing copy.** The single-table fast path only fired when the
destination was the default `gl_edge` table. Every semantic edge kind a code
repository produces routes to `gl_code_edge`, so the routing loop ran
`take_record_batch` over all 4,385,362 rows and held a second full copy of the
edge batch while the original, the graph and the other batches were live. The
guard now fires whenever every row goes to one table, and the destination is
resolved from the `relationship_kind` dictionary's values rather than by casting
the column to Utf8, which removes the cast array as well.

**Dictionary-encode the tag lists.** `source_tags` and `target_tags` were 384 MiB
of the 717 MiB edge batch, holding the same handful of denormalized tag strings
once per row. A new `ColumnType::DictStrList` sends them as
`List<Dictionary<Int32, Utf8>>`; the ClickHouse column stays `Array(String)`.
`logical_byte_size` already counts a dictionary by its decoded length, and a new
test pins list-of-dictionary to the same byte count as list-of-Utf8, so the ETL
byte metric is unchanged.

**Free resolution indexes before conversion.** `convert` takes the `CodeGraph`
by value and holds it for the whole Arrow build, which is the peak. `by_fqn`,
`by_name`, `nested`, `ancestors` and `definition_ranges` are dead once phase 3
finishes, and the string pool's dedup map is dead once nothing will intern
again. `CodeGraph::release_resolution_state` drops all six at the point the
graph is handed to the converter.

**`shrink_to_fit` the parse and resolve outputs.** Every file's definitions and
imports cross the parse barrier, and every file's resolved edges and failed
chains are held until the merge. All four are push-grown, so each carried its
last doubling as slop, repository-wide.

**Box the large `GraphNode` variants.** The enum was sized by `CanonicalFile` at
88 bytes, so petgraph stored 96 bytes for all 828,736 nodes, including the
797,000 definition and import nodes that need 24. Boxing the directory and file
payloads takes a node slot to 32 bytes.

**Dictionary-encode envelope-constant columns.** `traversal_path` and `branch`
are one value per batch, and were 101 MiB of the edge batch as plain Utf8.

**Pre-size the graph containers.** The node array, `defs` and `imports` all grew
by doubling and kept the slop through conversion; the counts are known from the
parse results, and the pending edge count is already tallied before the merge.

**Pre-size dictionary key buffers.** `StringDictionaryBuilder::new()` left the
keys buffer to double from zero, so each of the three dictionary columns on the
edge batch carried ~15 MiB of slop.

## Cross-repository verification

The eight changes were then verified on six repositories spanning six language
pipelines, by building a driver binary from each revision, indexing into two
ClickHouse databases, and fingerprinting every row of every table.

| Repository | Language | Rows | Peak live heap | Peak footprint | Wall clock | Output |
|---|---|---:|---|---|---|---|
| `torvalds/linux` v6.12 | C | 4,838,042 | 2,623 -> 1,472 MiB (**-43.9%**) | -35.5% | -2.4% | identical |
| `elastic/elasticsearch` v9.0.0 | Java | 4,103,145 | 2,538 -> 1,114 MiB (**-56.1%**) | -52.3% | -1.7% | identical |
| `gitlab-org/gitlab` v18.9.1-ee | Ruby, TypeScript | 2,504,626 | 1,158 -> 796 MiB (**-31.2%**) | -32.4% | -2.8% | identical |
| `microsoft/vscode` 1.99.3 | TypeScript | 1,234,464 | 980 -> 839 MiB (**-14.4%**) | -14.0% | -0.9% | identical |
| `protocolbuffers/protobuf` v30.2 | C++ | 316,910 | 114 -> 69 MiB (**-39.1%**) | -8.7% | -4.3% | identical |
| `django/django` 5.2 | Python | 254,571 | 159 -> 98 MiB (**-38.3%**) | -22.3% | -3.0% | identical |

Three runs each on the two largest, to separate the timing change from noise.
The baseline and optimized ranges do not overlap on either metric, and every one
of the twelve runs was faster than every baseline run of the same repository:

| Repository | Metric | Baseline (3 runs) | Optimized (3 runs) | Median |
|---|---|---|---|---|
| `linux` | wall clock | 42.5 / 42.7 / 43.5 s | 41.4 / 41.9 / 42.6 s | **-1.9%** |
| `linux` | peak live heap | 2,623 / 2,673 / 2,673 MiB | 1,472 / 1,472 / 1,472 MiB | **-44.9%** |
| `elasticsearch` | wall clock | 23.5 / 23.9 / 24.3 s | 23.0 / 23.1 / 23.8 s | **-3.1%** |
| `elasticsearch` | peak live heap | 2,538 / 2,538 / 2,571 MiB | 1,114 / 1,114 / 1,114 MiB | **-56.1%** |

The repository's own CI benchmark also ran its full 25-scenario matrix on
`linux-x86_64` against this change with no failures, which is the only
x86_64 evidence here; every number above is from an M3 Max.

Wall clock improves on all six. VS Code gains least because it is almost
entirely JavaScript and TypeScript, which run through `JsPipeline` rather than
the shared family pipeline, so only the Arrow-side changes apply to it.

"Identical" means the row count, an additive checksum and an XOR checksum of a
per-row hash over every column all agree, for all 40 tables. Two columns are
excluded because they are `Utc::now()` at index time and differ between runs by
construction: `_version` on every graph row and `indexed_at` on the checkpoint.
Everything else, including every ID, path, name, tag array, edge endpoint and
file `reason`, is compared. Reproduce with `mise memprofile:verify`.

### Indexing output is not reproducible at production per-file budgets

The first Linux comparison failed, and the control explains why: **the baseline
binary disagrees with itself.** Run twice at the production budgets
(`per_file_parse_timeout_ms: 100` and siblings), it aborted 17 files on one run
and 15 on the next, and wrote a different number of definitions, imported
symbols and code edges each time, with different `reason` values on the
corresponding file rows.

A file whose parse lands near its CPU budget aborts or completes depending on
machine load, so on a repository the size of the Linux kernel the graph is a
function of how busy the machine was. This also means a faster indexer produces
*more* graph: the optimized binary aborted 9 files where the baseline aborted
17, which is why its first Linux run had 1,212 more code edges. That is the
budget doing its job, not a behaviour change.

Byte-identity is therefore only testable with the per-file budgets disabled,
which is what the table above does. It is worth knowing independently of this
work: any before/after comparison of indexing output on a large repository has
to disable them first.

## Measured and rejected

Two candidates were implemented, measured and reverted.

**Chunk the parse into the graph build.** Parsing 2,048 files at a time and
draining each chunk into the graph before parsing the next bounds the parse
plateau to a chunk's worth. It made GitLab **worse** by 89 MiB (+11.2%) and
Elasticsearch better by only 21 MiB, inside the noise band. Regrowing the graph's
arrays once per chunk costs a transient copy that outweighs the smaller plateau,
and after the other changes the parse plateau is no longer near the peak
(436 MiB against a 1,114 MiB peak on Elasticsearch). All 218 code-graph
resolution suites passed, so the restructure was correct; it simply did not pay.

**Acquire the write permit before spawning.** `BufferedWriter::spawn_write`
takes its semaphore permit inside the spawned task, so `write_max_concurrent`
throttles ClickHouse but not memory: queued tasks still pin their batches.
Moving the acquire into the drain loop gives real back-pressure. On the two-lane
run it changed peak live heap by +37 MiB, inside noise, because after the Arrow
changes the batches are small enough that the four permits are never all held.
It would matter with more lanes or a slower ClickHouse, but it is unproven here
and stalls the drain loop, so it was reverted.

## Reproducing

```shell
mise memprofile:corpus
mise memprofile:ch
mise memprofile:run -- --project-id 100001 --label es-baseline
mise memprofile:run -- --project-id 278964 --label gl-baseline
mise memprofile:report .memprofile/runs/es-baseline
mise memprofile:stages .memprofile/runs/es-baseline --min-mib 4
```

Two repositories on two lanes:

```shell
mise memprofile:run -- --project-id 100001 --project-id 278964 \
  --indexing-lanes 2 --label both-2lane
```

Allocation-site attribution, on the reduced corpus, with the per-file budgets off
and the language families serialised so dhat's lock cannot reshape the overlap:

```shell
mise memprofile:dhat -- --project-id 100002 --label esjava-dhat --sample-hz 10 \
  --max-concurrent-languages 1 \
  --per-file-timeout-ms 0 --per-file-parse-timeout-ms 0 \
  --per-file-walk-timeout-ms 0 --per-file-ssa-timeout-ms 0 \
  --cross-file-resolve-timeout-ms 0
mise memprofile:dhat:report .memprofile/runs/esjava-dhat/dhat-heap.json
```

Raw artifacts for every run in this report are under `.memprofile/runs/<label>/`
(`samples.jsonl`, `events.jsonl`, `summary.json`, `dhat-heap.json`), which the
repository ignores.
