# integration-tests

Integration tests for the gkg-server query and redaction pipeline. This crate exists to
break the dependency cycle: it depends on both `gkg-server` and `integration-testkit`
without either of those needing to depend on each other.

## Structure

```plaintext
tests/
  common.rs              # Shared helpers: MockRedactionService, test fixtures, DummyClaims
  entrypoint.rs          # Test binary entry point (wires modules together)
  canary/
    setup_test.rs        # Infrastructure canary (validates TestContext, macros, isolation)
  indexer/               # Indexer integration tests (NATS, ClickHouse, SDLC, code, dispatcher)
  server/
    data_correctness.rs  # Seeds data, runs full pipeline, asserts values via ResponseView
    graph_formatter.rs   # Graph formatter end-to-end tests
    health.rs            # Health/readiness endpoint tests
    hydration.rs         # Hydration pipeline tests (compile -> execute -> hydrate -> format)
    redaction.rs         # Redaction pipeline tests (fail-closed, path finding, search, etc.)
```

All tests in `server/` compile as a single test binary. Each orchestrator test starts
one ClickHouse container, seeds data once, and runs subtests in parallel.

## Running

Requires Docker via Colima. All tasks are defined in `mise.toml` at the repo root:

```shell
mise colima:start                                   # start Docker runtime (12 GB RAM)
mise test:integration                               # run all integration tests
mise colima:stop                                    # stop when done
```

To run specific suites or tests directly:

```shell
export DOCKER_HOST="unix://$HOME/.colima/gkg/docker.sock"
cargo nextest run --all-features --test '*'                              # all
cargo nextest run --all-features --test '*' -E 'test(data_correctness)'  # one suite
cargo nextest run --all-features --test '*' -E 'test(infra_canary)'      # canary
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
4. If you need a new module, add `pub mod foo;` to `entrypoint.rs` and create
   `server/foo.rs`.
