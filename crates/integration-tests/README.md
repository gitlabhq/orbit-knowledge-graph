# integration-tests

Integration tests for the compiler and gkg-server query/redaction pipeline. This crate
exists to break the dependency cycle: it depends on `gkg-server`, `compiler`, and
`integration-testkit` without any of those needing to depend on each other.

## Structure

```plaintext
tests/
  common.rs                  # Shared helpers: MockRedactionService, test fixtures, DummyClaims
  entrypoints/
    compiler.rs              # Stateless compiler test binary (no Docker)
    docker.rs                # Docker-based server test binary
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
    graph_formatter.rs       # Graph formatter end-to-end tests
    health.rs                # Health/readiness endpoint tests
    hydration.rs             # Hydration pipeline tests (compile -> execute -> hydrate -> format)
    redaction.rs             # Redaction pipeline tests (fail-closed, path finding, search, etc.)
```

Tests are split across two binaries:

- **`compiler`** — Stateless compiler tests (no Docker). Runs as part of `unit-test` in CI.
- **`docker`** — Server tests requiring ClickHouse via testcontainers. Each orchestrator
  test starts one container, seeds data once, and runs subtests in parallel.

## Running

Requires Docker via Colima. All tasks are defined in `mise.toml` at the repo root:

```shell
mise colima:start                                   # start Docker runtime (12 GB RAM)
mise test:integration                               # run all integration tests
mise colima:stop                                    # stop when done
```

To run specific suites or tests directly:

```shell
# Compiler tests (no Docker needed)
cargo nextest run --test compiler

# Docker-based server tests
export DOCKER_HOST="unix://$HOME/.colima/gkg/docker.sock"
cargo nextest run --test docker                                          # all server tests
cargo nextest run --test docker -E 'test(data_correctness)'              # one suite
cargo nextest run --test docker -E 'test(infra_canary)'                  # canary
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
   `entrypoints/docker.rs` and create `server/foo.rs`.
5. For compiler tests, add to `compiler/mod.rs` and create `compiler/foo.rs`.
