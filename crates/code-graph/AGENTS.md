# Code-graph

Language-specific code belongs in `crates/code-graph/src/v2/langs/{generic,custom}/`, not in the shared linker, pipeline, or analyzer — both are performance-sensitive and per-language branches there add complexity for every language. Express special handling as a **generic hook the pipeline already exposes** (e.g. a per-language import/scope hook via config) or reuse an existing edge type, rather than a language branch in shared code. When adding a new language, include a `code-indexing-benchmark.yaml` entry and test fixture repos under `fixtures/code/` so the team can evaluate coverage.

`crates/code-graph/heavy/` is a standalone crate with its own workspace, excluded from the root `Cargo.toml`. It is not built, linted, tested, or shipped with the main tree, has no row in `docs/dev/agents-crate-map.md`, and CI never requires it. Build it from its own directory with `cargo build`.
