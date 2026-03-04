# Fuzzing Strategy

## Motivation

The Knowledge Graph service parses untrusted input at multiple layers:

- **`code-parser`**: Source code in 7 languages via tree-sitter, SWC, and ruby-prism
- **`query-engine`**: User-submitted JSON DSL compiled to parameterized ClickHouse SQL

Fuzz testing exercises these parsers with random and mutated inputs to find panics,
memory safety violations, stack overflows, and logic errors that example-based unit
tests miss.

## Tooling

We use [`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz), which wraps LLVM's
libFuzzer. It provides coverage-guided mutation fuzzing with AddressSanitizer enabled
by default.

### Nightly requirement

`cargo-fuzz` requires the nightly Rust toolchain. Each `fuzz/` directory contains its
own `rust-toolchain.toml` pinned to nightly, so the main workspace stays on stable.

### Installation

```sh
cargo install cargo-fuzz
```

Or add to `mise.toml` tools:

```toml
"cargo:cargo-fuzz" = "latest"
```

## Crate Prioritization

| Tier | Crate | Rationale |
|------|-------|-----------|
| 1 | `code-parser` | Parses arbitrary source code in 7 languages across 3 backends |
| 1 | `query-engine` | Security-critical: untrusted JSON → SQL with authz enforcement |
| 2 | `ontology` | YAML deserialization with complex validation (developer-authored input) |
| 2 | `indexer` | Consumes decoded protobuf events, transforms to graph rows |

## Current Targets (PoC)

### code-parser

| Target | Entry point | Input | What it tests |
|--------|------------|-------|---------------|
| `fuzz_ruby_parse` | `RubyAnalyzer::parse_and_analyze()` | `&str` | ruby-prism parsing + full analysis pipeline |

### query-engine

| Target | Entry point | Input | What it tests |
|--------|------------|-------|---------------|
| `fuzz_compile_raw` | `compile()` | `&str` (raw JSON) | Full pipeline: schema validation → parse → normalize → lower → security → codegen |

## Running Locally

The project uses mise to pin Rust stable, which takes precedence over
`rust-toolchain.toml`. Set `RUSTUP_TOOLCHAIN=nightly` to use the nightly
compiler required by `cargo-fuzz`:

```sh
# Run a target indefinitely (Ctrl-C to stop)
cd crates/code-parser/fuzz
RUSTUP_TOOLCHAIN=nightly cargo fuzz run fuzz_ruby_parse

# Run for a fixed duration
RUSTUP_TOOLCHAIN=nightly cargo fuzz run fuzz_ruby_parse -- -max_total_time=300

# Query engine target
cd crates/query-engine/fuzz
RUSTUP_TOOLCHAIN=nightly cargo fuzz run fuzz_compile_raw
```

Useful flags:

```sh
# Limit memory to catch OOM
RUSTUP_TOOLCHAIN=nightly cargo fuzz run fuzz_ruby_parse -- -rss_limit_mb=2048

# Run with multiple jobs
RUSTUP_TOOLCHAIN=nightly cargo fuzz run fuzz_ruby_parse --jobs 4

# Minimize corpus after a long run
RUSTUP_TOOLCHAIN=nightly cargo fuzz cmin fuzz_ruby_parse
```

## Corpus Management

### Seeding

Each target has a `corpus/<target>/` directory seeded from existing fixtures:

- `fuzz_ruby_parse`: Ruby fixture files from `src/ruby/fixtures/`
- `fuzz_compile_raw`: Individual queries extracted from `fixtures/queries/sdlc_queries.json`

### Growth

libFuzzer automatically saves new coverage-increasing inputs to the corpus directory.
Periodically minimize with `cargo fuzz cmin <target>` to remove redundant entries.

### Storage

Corpus directories are committed to the repository (they're small text files).
If they grow large, move to CI cache artifacts instead.

## Triaging Crashes

When libFuzzer finds a crash, it saves the reproducing input to
`fuzz/artifacts/<target>/`. Artifacts are gitignored — they are a transient
working directory for libFuzzer, not a permanent record.

### Reproducing

```sh
cd crates/code-parser/fuzz
RUSTUP_TOOLCHAIN=nightly cargo fuzz run fuzz_ruby_parse fuzz/artifacts/fuzz_ruby_parse/crash-<hash>

# With backtrace
RUST_BACKTRACE=1 RUSTUP_TOOLCHAIN=nightly cargo fuzz run fuzz_ruby_parse fuzz/artifacts/fuzz_ruby_parse/crash-<hash>
```

### Crash triage workflow

1. **Reproduce** the crash locally to confirm it's real (not a flaky OOM from
   resource limits).
2. **Copy the artifact to the corpus** with a descriptive name and the
   appropriate file extension (`.rb`, `.json`, etc.) so it passes the corpus
   `.gitignore` allowlist:

   ```sh
   cp fuzz/artifacts/fuzz_ruby_parse/oom-<hash> \
      fuzz/corpus/fuzz_ruby_parse/regression_oom_description.rb
   ```

3. **Commit the regression seed** in its own commit:

   ```sh
   git add fuzz/corpus/fuzz_ruby_parse/regression_oom_description.rb
   git commit -m "test(fuzz): add regression seed for <description>"
   ```

4. **File an issue** with the crash input, backtrace, and root cause analysis.
5. **Fix the bug** in a separate commit. The regression seed stays in the corpus
   permanently — every future `cargo fuzz run` re-validates it before exploring
   new inputs, preventing regressions.

### Naming convention for regression seeds

Use the prefix `regression_` followed by the crash type and a short description:

```
regression_oom_nested_blocks.rb
regression_panic_empty_fqn.rb
regression_crash_malformed_json.json
```

### Severity guidelines

| Crate | Severity | Rationale |
|-------|----------|-----------|
| `query-engine` | P2 | Security-critical path; crashes may indicate validation bypasses |
| `code-parser` | P3 | Processes untrusted code but panics don't affect authorization |