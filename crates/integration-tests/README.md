# integration-tests

Integration tests for the gkg-server query and redaction pipeline. This crate exists to
break the dependency cycle: it depends on both `gkg-server` and `integration-testkit`
without either of those needing to depend on each other.

## Structure

```plaintext
tests/
  common.rs            # Shared helpers: MockRedactionService, test fixtures, DummyClaims
  entrypoint.rs        # Test binary entry point (wires modules together)
  server/
    redaction.rs       # Redaction pipeline tests (fail-closed, path finding, search, etc.)
    hydration.rs       # Hydration pipeline tests (compile -> execute -> hydrate -> format)
```

All tests in `server/` compile as a single test binary. Each orchestrator test
(`redaction_integration`, `hydration_integration`) starts one ClickHouse container and
uses `run_subtests!` to fork databases and run subtests in parallel.

## Running

```shell
mise run test:integration          # all integration tests across the workspace
cargo nextest run -p integration-tests  # just this crate
```

Requires Docker. Start with `mise colima:start`.

## Adding tests

1. Add async test functions to the appropriate `server/*.rs` module.
2. Register them in the `run_subtests!` invocation at the bottom of that module.
3. If you need a new module, add `pub mod foo;` to `entrypoint.rs` and create `server/foo.rs`.
