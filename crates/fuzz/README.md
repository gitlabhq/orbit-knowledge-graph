# orbit-fuzz

Fuzz testing for Orbit with [Bolero](https://github.com/camshaft/bolero).

## Fuzz targets

### Query compiler

| Target | What it exercises |
|---|---|
| `fuzz_compile` | Unstructured byte input to `compile()` — tests JSON parsing edge cases |
| `fuzz_compile_structured` | Structured JSON query generation via `FuzzQuery` `TypeGenerator` — generates valid/semi-valid queries that reach deeper compiler logic (normalization, lowering, optimization, security enforcement, codegen) |

### Orbit query frontend

| Target | What it exercises |
|---|---|
| `fuzz_gql` | Unstructured text into `compile()` with the Orbit query frontend; every failure must be client-safe |
| `fuzz_gql_grammar` | Derivations of `query.pest` itself (`grammar::Grammar`), so the text is in the language by construction; the syntax tree must consume it or reject it with a lowering error, never a syntax error or pipeline invariant |

`grammar::Grammar` parses the grammar with `pest_meta` and uses input bytes to choose productions while walking the rule AST. The real parser decides whether a derivation is faithful: `gql::pair_outline` must return the same rule sequence the walk produced, which discards derivations that PEG ordered choice or greedy repetition would read differently. Leaf overrides supply nonempty identifiers.

### Language parsers

| Target | What it exercises |
|---|---|
| `fuzz_ruby` | Ruby parser + DSL extraction (`RubyDsl` spec, `parse_full_collect`) |
| `fuzz_python` | Python parser + DSL extraction (`PythonDsl` spec, `parse_full_collect`) |
| `fuzz_typescript` | TypeScript and JavaScript tree-sitter parsers (`Language::parse_ast`) |
| `fuzz_java` | Java parser + DSL extraction (`JavaDsl` spec, `parse_full_collect`) |
| `fuzz_kotlin` | Kotlin parser + DSL extraction (`KotlinDsl` spec, `parse_full_collect`) |
| `fuzz_csharp` | C# parser + DSL extraction (`CSharpDsl` spec, `parse_full_collect`) |
| `fuzz_rust_parser` | Rust tree-sitter parser (`Language::parse_ast`) |

### Indexer messages

| Target | What it exercises |
|---|---|
| `fuzz_indexer_messages` | Deserialization of all indexer NATS message types (`GlobalIndexingRequest`, `NamespaceIndexingRequest`, `CodeIndexingTaskRequest`, `NamespaceDeletionRequest`) |

## Corpus

Each target keeps its corpus in `tests/<target>/corpus.tar.gz`, tracked in Git LFS. Bolero reads the unpacked `tests/<target>/corpus/` directory, which is gitignored. Install Git LFS and run `git lfs install --skip-repo` once, so `git add` stores the archives in LFS.

```sh
mise fuzz:corpus-unpack   # wipe and re-extract every corpus/ directory
mise fuzz:corpus-pack     # pack every corpus/ directory into its archive
```

Both scripts in `scripts/fuzz/` take target names to limit the run. Unpacking deletes `corpus/` first, so pack a locally grown corpus before you run a fuzz task again. Packing writes the same bytes for the same files and skips the AppleDouble (`._*`) files that macOS `tar` adds.

## Running

With mise (recommended):

```sh
mise test:fuzz                 # replay every corpus on stable Rust, then 1s of random inputs
mise fuzz:deep                 # libFuzzer + AddressSanitizer over every target, 600s in total
mise fuzz:compile              # fuzz the query compiler (unstructured)
mise fuzz:compile-structured   # fuzz the query compiler (structured)
mise fuzz:gql                  # fuzz the Orbit query frontend (unstructured)
mise fuzz:gql-grammar          # fuzz the Orbit query frontend (grammar derivations)
mise fuzz:ruby                 # fuzz the Ruby parser
mise fuzz:python               # fuzz the Python parser
mise fuzz:typescript           # fuzz the TypeScript/JS parser
mise fuzz:java                 # fuzz the Java parser
mise fuzz:kotlin               # fuzz the Kotlin parser
mise fuzz:csharp               # fuzz the C# parser
mise fuzz:rust-parser          # fuzz the Rust parser
mise fuzz:indexer-messages     # fuzz indexer message deserialization
```

Or directly with cargo-bolero:

```sh
cargo +nightly bolero test <target_name> -p orbit-fuzz
```

If your default toolchain is nightly, you can omit `+nightly`.

On macOS, AddressSanitizer builds fail to link (`initializer pointer has no target`). Run `FUZZ_SANITIZER=NONE mise fuzz:deep`, or pass `--sanitizer NONE` to `cargo bolero test`.

## CI

| Job | When | What it runs |
|---|---|---|
| `fuzz-smoke` | Every MR and main commit, allowed to fail | `mise test:fuzz` |
| `fuzz-deep` | Daily schedule with `SCHEDULE_ONLY=fuzz-deep`; manual on MRs | `scripts/fuzz/deep.sh` in three shards |

Each `fuzz-deep` shard takes every third target and splits `FUZZ_BUDGET_SECONDS` (2700) between them. With 12 targets, each one gets 675 seconds. The shard also spends time on setup and one AddressSanitizer build, and all of it must finish inside the 90-minute job timeout. GitLab uploads no artifacts from a job that times out, so a crash found in a timed-out job is lost. A new target shortens the others instead of growing the job. If the build gets slower, lower the budget.

A crashing target fails the shard after the remaining targets have run. The job uploads `tests/*/corpus/` and `tests/*/crashes/` as artifacts on success and on failure, and keeps them for 30 days.

### Promote a corpus

1. Download the artifacts of the latest `fuzz-deep` shards.
1. Compare each artifact `corpus/` input count with the count `mise fuzz:corpus-unpack` prints for the committed archive. Look into a large jump before you keep it.
1. Copy each grown `corpus/` directory over `tests/<target>/corpus/`, run `mise fuzz:corpus-pack`, and commit the archives.

### Triage a crash

1. Download the `crashes/` artifact of the failed shard.
1. Copy the crashing input into `tests/<target>/corpus/` and reproduce it with `cargo test -p orbit-fuzz --test <target>`.
1. Fix the bug in the crate under test.
1. Keep the input in the corpus and run `scripts/fuzz/pack-corpus.sh <target>`, so every later run replays it.
1. If the harness assertion failed rather than the code, decide whether the property or the code is wrong. Don't weaken an assertion to make a run pass.

### Add a target

Add the harness under `tests/<target>/fuzz_target.rs` and its `[[test]]` block with `harness = false` to `Cargo.toml`. Without the block, Cargo never builds the file. Add a `mise fuzz:<target>` task, then seed `tests/<target>/corpus/` with a few inputs: a valid one, boundary cases, and a truncated one. Run `scripts/fuzz/pack-corpus.sh <target>`. The scripts and `fuzz-deep` find every directory under `tests/`.

See [Fuzzing](../../docs/design-documents/testing.md#fuzzing).
