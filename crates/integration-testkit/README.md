# integration-testkit

Shared test infrastructure for integration tests that need a real ClickHouse instance.

## What it provides

- **`TestContext`** — Starts a ClickHouse container via testcontainers, runs schema DDL,
  and exposes `query()`, `execute()`, and `query_parameterized()` for Arrow-based results.
- **`TestContext::fork()`** — Creates an isolated database per subtest so subtests can run
  in parallel against one container.
- **`TestContext::optimize_all()`** — Queries `system.tables` for the current database and
  runs `OPTIMIZE TABLE … FINAL` concurrently on every table. Call after seeding data.
- **`run_subtests_shared!`** — Macro that runs all subtests in parallel against the same
  shared database. Use for read-only subtests.
- **`run_subtests!`** — Macro that forks a database per subtest and runs them concurrently.
  Use for subtests that write data beyond the initial seed.
- **Arrow extractors** — `get_string_column`, `get_int64_column`, `get_uint64_column`,
  `get_boolean_column` for pulling typed columns out of `RecordBatch`.
- **`ResponseView`** — Typed wrapper over `GraphResponse` for asserting query pipeline
  output. Includes assertion enforcement that catches under-tested queries.

## Prerequisites

Integration tests need a Docker-compatible runtime. The project uses Colima, managed
through `mise.toml`:

```shell
mise colima:start        # start the gkg Colima instance (12 GB RAM)
mise test:integration    # run all integration tests (sets DOCKER_HOST automatically)
mise colima:stop         # stop when done
```

See `mise.toml` for the full task definitions. The `test:integration` task sets
`DOCKER_HOST` to the Colima socket automatically. If running `cargo nextest` directly,
export it yourself:

```shell
export DOCKER_HOST="unix://$HOME/.colima/gkg/docker.sock"
cargo nextest run --test containers
```

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

## Choosing a test macro

| Macro | DB per subtest | Use when |
|---|---|---|
| `run_subtests_shared!` | No (shared) | All subtests only SELECT against seeded data |
| `run_subtests!` | Yes (forked) | Subtests INSERT/UPDATE/DELETE beyond the seed |

Most test suites are read-only. The typical pattern is:

```rust
use integration_testkit::{run_subtests, run_subtests_shared, TestContext,
                          SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL};

#[tokio::test]
async fn my_test_suite() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL]).await;
    seed(&ctx).await;

    // Read-only subtests: seed once, query many.
    run_subtests_shared!(&ctx,
        search_returns_correct_values,
        traversal_joins_are_correct,
    );

    // Mutating subtests: each gets its own forked DB.
    run_subtests!(&ctx,
        writes_extra_data_then_queries,
    );
}
```

### Writing a seed function

Seed functions insert test data and call `optimize_all()` at the end:

```rust
async fn seed(ctx: &TestContext) {
    ctx.execute("INSERT INTO gl_user ...").await;
    ctx.execute("INSERT INTO gl_edge ...").await;
    ctx.optimize_all().await;
}
```

`optimize_all()` forces ClickHouse to merge ReplacingMergeTree parts so all
inserted rows are visible to subsequent queries. Without it, concurrent reads
can intermittently miss freshly-written data.

### Writing subtests

Read-only subtests receive a shared `&TestContext` and only query:

```rust
async fn search_returns_correct_values(ctx: &TestContext) {
    let resp = run_query(ctx, "...", &allow_all()).await;
    resp.assert_node_count(5);
}
```

Mutating subtests call their own seed and do additional writes. They go in the
`run_subtests!` block so they get an isolated database:

```rust
async fn writes_extra_data_then_queries(ctx: &TestContext) {
    seed(ctx).await;
    ctx.execute("INSERT INTO gl_note ...").await;
    let resp = run_query(ctx, "...", &allow_all()).await;
    // ...
}
```

## ResponseView

`ResponseView` wraps the `GraphResponse` returned by the query pipeline and provides
typed helpers for looking up nodes, edges, and paths. It also enforces that tests
actually assert the query features they exercise.

### Creating a view

The only public constructor is `for_query`, which takes the compiled `Input` AST
and the formatted `GraphResponse`:

```rust
use integration_testkit::visitor::{ResponseView, NodeExt};
use query_engine::compiler::compile;

let compiled = compile(json, &ontology, &security_ctx).unwrap();
// ... run pipeline, get response ...
let resp = ResponseView::for_query(&compiled.input, response);
```

On construction, `for_query` validates two structural invariants:

- The response's `query_type` matches the input (e.g. a traversal query must produce
  a `"traversal"` response).
- Single-node traversal and aggregation responses have zero edges (the formatter
  never produces edges for these query shapes).

### Assertion enforcement

`for_query` inspects the `Input` AST and derives a set of requirements — one per
query feature that needs to be asserted. When the `ResponseView` is dropped, it
panics if any requirement was not satisfied.

The mapping from query features to requirements:

| Query feature | Requirement | Satisfied by |
|---|---|---|
| `order_by` | `OrderBy` | `assert_node_order` |
| `filters: {field: ...}` | `Filter { field }` (one per field) | `assert_filter(entity, field, pred)` |
| `node_ids: [...]` | `NodeIds` | `node_ids`, `assert_node_order`, `assert_node_count` |
| `query_type: aggregation` | `Aggregation` | `assert_node` |
| `aggregation_sort` | `AggregationSort` | `assert_node_order` |
| `query_type: path_finding` | `PathFinding` | `path_ids` |
| `query_type: neighbors` | `Neighbors` | `edges_of_type`, `assert_edge_exists`, `assert_edge_absent` |
| `relationships: [{type: T}]` | `Relationship { edge_type: T }` (one per type) | `edges_of_type`, `assert_edge_exists`, `assert_edge_absent` |
| `range` | `Range` | `assert_node_count` |

Requirements are granular: a query with two filter fields produces two `Filter`
requirements, and the test must call `assert_filter` for each. A traversal with
two relationship types produces two `Relationship` requirements.

If a test drops a `ResponseView` without satisfying all requirements, the drop
panics with a message listing exactly what's missing:

```plaintext
ResponseView dropped with unsatisfied assertion requirements:
Filter on 'state' (call assert_filter for 'state')
OrderBy (query has order_by — call assert_node_order)
```

### Escape hatch

For edge cases where a test intentionally skips an assertion:

```rust
resp.skip_requirement(Requirement::OrderBy);
```

### Example: single-entity traversal with filter and ordering

```rust
async fn search_filter_eq(ctx: &TestContext) {
    let resp = run_query(ctx, r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User",
                 "filters": {"state": "blocked"}},
        "order_by": {"node": "u", "property": "id"},
        "limit": 10
    }"#, &allow_all()).await;

    // Satisfies Filter{field:"state"}
    resp.assert_filter("User", "state", |n| {
        n.prop_str("state") == Some("blocked")
    });
    // Satisfies OrderBy + NodeIds
    resp.assert_node_order("User", &[5]);
}
// Drop checks: all requirements satisfied, no panic.
```

### Example: traversal with edges

```rust
async fn traversal_edges(ctx: &TestContext) {
    let resp = run_query(ctx, r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User"},
            {"id": "g", "entity": "Group"}
        ],
        "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
        "limit": 10
    }"#, &allow_all()).await;

    // Satisfies Relationship{edge_type:"MEMBER_OF"}
    resp.assert_edge_exists("User", 1, "Group", 100, "MEMBER_OF");
    resp.assert_referential_integrity();
}
```

### NodeExt

The `NodeExt` trait provides typed property access on `GraphNode`:

```rust
use integration_testkit::visitor::NodeExt;

let alice = resp.find_node("User", 1).unwrap();
alice.assert_str("username", "alice");
alice.assert_str("state", "active");
assert_eq!(alice.prop_i64("score"), Some(42));
assert_eq!(alice.prop_bool("admin"), Some(true));
```

Methods: `prop`, `prop_str`, `prop_i64`, `prop_f64`, `prop_bool`, `has_prop`,
`assert_prop`, `assert_str`.
