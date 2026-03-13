# integration-tests

Integration tests for the gkg-server query and redaction pipeline. This crate exists to
break the dependency cycle: it depends on both `gkg-server` and `integration-testkit`
without either of those needing to depend on each other.

## Structure

```plaintext
tests/
  common.rs            # Shared helpers: MockRedactionService, test fixtures, DummyClaims
  entrypoint.rs        # Test binary entry point (wires modules together)
  indexer/             # Indexer integration tests (NATS, ClickHouse, SDLC, code, dispatcher)
  server/
    data_correctness.rs # Seeds data, runs full pipeline, asserts values via ResponseView
    graph_formatter.rs  # Graph formatter end-to-end tests
    health.rs           # Health/readiness endpoint tests
    hydration.rs        # Hydration pipeline tests (compile -> execute -> hydrate -> format)
    redaction.rs        # Redaction pipeline tests (fail-closed, path finding, search, etc.)
```

All tests in `server/` compile as a single test binary. Each orchestrator test starts
one ClickHouse container, seeds data once, and runs subtests in parallel.

## Running

```shell
mise run test:integration                           # all integration tests
cargo nextest run -p integration-tests              # just this crate
cargo nextest run --all-features --test '*' \
  -E 'test(data_correctness)' --retries 0           # one suite
```

Requires Docker. Start with `mise colima:start`.

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
