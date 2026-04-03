# integration-tests

Integration tests for the compiler and gkg-server query/redaction pipeline. This crate
exists to break the dependency cycle: it depends on `gkg-server`, `compiler`, and
`integration-testkit` without any of those needing to depend on each other.

## Structure

```plaintext
tests/
  local.rs                   # Non-Docker test binary: compiler + querying pipeline (auto-discovered)
  containers.rs              # Docker-based server test binary (auto-discovered)
  common/
    mod.rs                   # Shared helpers: MockRedactionService, test fixtures, DummyClaims
  compiler/
    mod.rs                   # Module declarations
    setup.rs                 # Shared test helpers (test_ctx, test_ontology, compile_to_ast)
    queries.rs               # Compiler tests with hand-built ontology
    ontology.rs              # Compiler tests with embedded production ontology
  canary/
    setup_test.rs            # Infrastructure canary (validates TestContext, macros, isolation)
  indexer/                   # Indexer integration tests (NATS, ClickHouse, SDLC, code, dispatcher)
  server/
    data_correctness/        # Seeds data, runs full pipeline, asserts values via ResponseView
    querying_pipeline/       # Virtual column dispatch tests
    graph_formatter.rs       # Graph formatter end-to-end tests
    health.rs                # Health/readiness endpoint tests
    hydration.rs             # Hydration pipeline tests (compile -> execute -> hydrate -> format)
    redaction.rs             # Redaction pipeline tests (fail-closed, path finding, search, etc.)
```

Test targets are auto-discovered by Cargo from `tests/*.rs` files. Shared helpers
live in `tests/common/` (subdirectory, ignored by auto-discovery).

Tests are split across two binaries:

- **`local`** — Compiler and querying pipeline tests (no Docker). Runs as part of `unit-test` in CI.
- **`containers`** — Server tests requiring ClickHouse via testcontainers. Each orchestrator
  test starts one container, seeds data once, and runs subtests in parallel.

## Running

All tasks are defined in `mise.toml` at the repo root:

```shell
mise test:local                                     # compiler + querying pipeline (no Docker)
mise colima:start                                   # start Docker runtime (12 GB RAM)
mise test:integration                               # run all server integration tests
mise test:integration:server                        # correctness, hydration, redaction, graph formatter
mise colima:stop                                    # stop when done
```

To run specific suites or tests directly:

```shell
# Local tests (no Docker needed)
cargo nextest run --test local                                           # all local tests
cargo nextest run --test local -E 'test(compiler::)'                     # compiler only
cargo nextest run --test local -E 'test(querying_pipeline::)'            # querying pipeline only

# Docker-based server tests
export DOCKER_HOST="unix://$HOME/.colima/gkg/docker.sock"
cargo nextest run --test containers                                      # all server tests
cargo nextest run --test containers -E 'test(data_correctness)'          # one suite
cargo nextest run --test containers -E 'test(infra_canary)'              # canary
```

## Test architecture

Each `server/*.rs` module follows the same structure:

1. **Seed function** — inserts known data, calls `ctx.optimize_all()` at the end.
2. **Subtests** — async functions that receive `&TestContext` and run queries.
3. **Orchestrator** — a single `#[tokio::test]` that creates the container, seeds
   once, and dispatches subtests via macros.

Read-only subtests use `run_subtests_shared!` (one shared DB). Subtests that write
additional data use `run_subtests!` (forked DB per subtest). See the
[integration-testkit README](../integration-testkit/README.md) for details on choosing
between them.

## Adding tests

1. Write an `async fn my_test(ctx: &TestContext)` in the appropriate module.
2. If it only reads seeded data, add it to the `run_subtests_shared!` block.
3. If it writes extra data, add it to the `run_subtests!` block and call the seed
   function at the top of the test body.
4. If you need a new server test module, add `pub mod foo;` to
   `containers.rs` and create `server/foo.rs`.
5. For compiler tests, add to `compiler/mod.rs` and create `compiler/foo.rs`.
6. For a new test binary, add a `tests/foo.rs` file -- Cargo auto-discovers it.

## Auto-discovery rules

Cargo treats every `.rs` file at the `tests/` root as a separate test binary.
Subdirectories (`common/`, `compiler/`, `server/`, etc.) are ignored. This is
why shared helpers live in `tests/common/mod.rs` instead of `tests/common.rs`.

When an entrypoint needs to include modules whose directory name doesn't match
the module name (e.g. `querying_pipeline` lives under `server/`), use a
`#[path]` attribute:

```rust
// local.rs — compiler/ is at the tests/ root, so standard resolution works:
mod compiler;

// querying_pipeline/ lives under server/, so we need an explicit path:
#[path = "server/querying_pipeline/mod.rs"]
mod querying_pipeline;
```

Avoid naming a `.rs` entrypoint the same as an existing subdirectory
(e.g. don't create `tests/compiler.rs` when `tests/compiler/` exists) --
Rust forbids both `foo.rs` and `foo/mod.rs` for the same module.
