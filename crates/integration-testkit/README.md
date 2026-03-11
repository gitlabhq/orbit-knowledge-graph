# integration-testkit

Shared test infrastructure for integration tests that need a real ClickHouse instance.

## What it provides

- **`TestContext`** — Starts a ClickHouse container via testcontainers, runs schema DDL,
  and exposes `query()`, `execute()`, and `query_parameterized()` for Arrow-based results.
- **`TestContext::fork()`** — Creates an isolated database per subtest so subtests can run
  in parallel against one container.
- **`run_subtests!`** — Macro that forks a database per subtest and runs them concurrently.
- **Arrow extractors** — `get_string_column`, `get_int64_column`, `get_uint64_column`,
  `get_boolean_column` for pulling typed columns out of `RecordBatch`.

## Usage

This crate is a dependency (not dev-dependency) because test crates like
`integration-tests` and `indexer` import it as a regular dependency in their
`[dev-dependencies]` section.

```rust
use integration_testkit::{TestContext, run_subtests, SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL};

#[tokio::test]
async fn my_integration_test() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL]).await;
    run_subtests!(&ctx, subtest_a, subtest_b);
}

async fn subtest_a(ctx: &TestContext) {
    ctx.execute("INSERT INTO ...").await;
    let batches = ctx.query("SELECT ...").await;
    // assertions
}
```

Requires Docker (via Colima or native). Start with `mise colima:start` before running
integration tests.
