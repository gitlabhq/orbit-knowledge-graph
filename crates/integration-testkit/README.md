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
use compiler::{compile, Frontend};

let compiled = compile(json, Frontend::JsonDsl, &ontology, &security_ctx).unwrap();
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
| `query_type: aggregation` | `Aggregation` | `assert_aggregation_value_i64`, `assert_row_value_i64`, `assert_group_row_value_i64` |
| `group_by: ["<node>"]` | `Aggregation` | `assert_group_node_count`, `assert_group_node_ids`, `assert_group_node_row`, `assert_group_row_value_i64` |
| `group_by: ["<node>.<property>"]` | `Aggregation` | `assert_group_column`, `assert_row_count`, `assert_row_value_i64`, `assert_row_value_str` |
| `aggregation_sort` | `AggregationSort` | `assert_group_node_order` |
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
        "nodes": [{"id": "u", "entity": "User",
                 "filters": {"state": "blocked"}}],
        "order_by": "u.id",
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
            {"id": "u", "entity": "User", "node_ids": [1]},
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

## Query scenarios

Query scenarios are YAML-driven data correctness tests that replace handwritten Rust
assertion code. They are the preferred way to add new correctness coverage. Each scenario
declares a query, optional config overrides, and expected results. The harness seeds data,
runs the full pipeline (compile, execute, redact, hydrate, paginate, format), and checks
the response against the expectations.

### File structure

Scenarios live under `tests/scenarios/` organized by category:

```
tests/scenarios/
├── presets/
│   ├── security.yaml
│   ├── redaction.yaml
│   └── seed.yaml
├── traversal/
│   ├── basic.yaml
│   └── filtering.yaml
├── aggregation/
│   └── group_by.yaml
└── paths/
    └── shortest_path.yaml
```

Each YAML file contains one `QueryScenario` document.

### `QueryScenario` format

#### Top-level fields

| Field | Type | Required | Description |
|---|---|---|---|
| `description` | string | no | Human-readable summary |
| `config` | `ScenarioConfig` | no | Security, redaction, and seed overrides |
| `query` | map | **yes** | Query strings keyed by frontend (`json`, `gql`) |
| `expect` | `QueryExpect` | **yes** | Assertions on the result |

#### `config` — `ScenarioConfig`

| Field | Type | Description |
|---|---|---|
| `extra_seed` | `Seed` | Additional rows to insert; triggers a DB fork |
| `security` | preset name or inline `SecurityOverride` | Authorization context |
| `redaction` | preset name or inline `RedactionConfig` | Entity-level redaction |

#### `config.security` — `SecurityOverride`

| Field | Type | Default | Description |
|---|---|---|---|
| `admin` | bool | false | Run as admin |
| `paths` | [string] | `["1/"]` | Uniform authorized namespace paths |
| `authorized_paths` | [{path, access_level}] | — | Per-path access levels |
| `org_id` | i64 | — | Organization ID |
| `access_level` | u32 | 20 (Reporter) | Default access level |
| `scope_prefixes` | {alias: prefix} | — | Per-alias traversal scoping |

#### `config.redaction` — `RedactionConfig`

| Field | Type | Description |
|---|---|---|
| `allow` | {entity: [ids]} | Entity IDs the user can see |
| `deny` | {entity: [ids]} | Entity IDs explicitly denied |

#### `expect` — `QueryExpect`

**Result shape:**

| Field | Type | Description |
|---|---|---|
| `node_count` | N | Total node count |
| `total_edge_count` | N | Total edges across all types |
| `nodes` | {Entity: `NodeExpect`} | Per-entity assertions |
| `edges` | {TYPE: [[from, to]]} | Exact edge set |
| `edge_exists` | {TYPE: [[from, to]]} | Subset check |
| `edge_absent` | {TYPE: [[from, to]]} | Negative check |
| `edge_count` | {TYPE: N} | Per-type edge count |
| `referential_integrity` | bool | All edge endpoints have nodes |
| `has_more` | bool | Pagination flag |

**Aggregation:**

| Field | Type | Description |
|---|---|---|
| `groups` | {key: `GroupExpect`} | Per-group assertions |
| `group_columns` | {name: "node.property"} | Group column metadata |
| `empty_aggregation` | bool | Assert empty aggregation result |
| `row_count` | N | Ungrouped aggregation row count |
| `row_values` | [{col: val}] | Ungrouped aggregation values by index |

**Paths:**

| Field | Type | Description |
|---|---|---|
| `path_count` | N | Number of paths |
| `path_destinations` | {Entity: [ids]} | Path endpoint IDs |
| `path_edges` | [[{from, from_id, type, to, to_id, step}]] | Per-path edge structure (all fields optional) |
| `path_endpoint_absent` | [Entity] | Entities excluded from path edges |

**Compilation:**

| Field | Type | Description |
|---|---|---|
| `compile_only` | bool | Only compile, skip execution |
| `compile_error` | bool / string / {frontend: string} | Expect compile failure |
| `compile_error_not_contains` | [string] / {frontend: [string]} | Error must NOT contain |
| `sql_contains` | [string] | Rendered SQL must contain these |
| `sql_not_contains` | [string] | Rendered SQL must NOT contain these |

**Pagination:**

| Field | Type | Description |
|---|---|---|
| `pages` | [`QueryExpect`] | Multi-page assertions with auto cursor chaining |
| `all_pages` | `AllPagesExpect` | Cross-page assertions |

**Other:**

| Field | Type | Description |
|---|---|---|
| `repeat_count` | N | Run N times, assert identical results |
| `skip_requirements` | [string] | Skip enforcement checks |

#### `NodeExpect`

| Field | Type | Description |
|---|---|---|
| `count` | N | Expected count |
| `order` | [ids] | Exact ordered list |
| `ids` | [ids] | Unordered set |
| `absent` | [ids] | Must NOT appear |
| `filters` | {field: predicate} | Every node must match |
| `prop_present` | [fields] | Properties that must exist |
| `prop_absent` | [fields] | Properties that must not exist |
| `rows` | [{id, prop: val}] | Per-node property assertions |

Filter operators: `eq`, `in`, `starts_with`, `contains`, `ends_with`, `is_null`,
`is_not_null`, `gte`, `lte`, `lt`.

Row values support `{repeat: str, count: N, suffix: str}` for expansion.

#### `GroupExpect`

| Field | Type | Description |
|---|---|---|
| `entity` | string | Entity type |
| `count` | N | Group size |
| `order` | [ids] | Ordered IDs |
| `ids` | [ids] | Unordered IDs |
| `rows` | [{entity, id, values: {col: val}, properties: {prop: val}}] | Per-row assertions |
| `absent` | [{entity, id}] | Must NOT appear |

#### `AllPagesExpect`

| Field | Type | Description |
|---|---|---|
| `node_ids` | {Entity: [ids]} | All node IDs across pages |
| `group_node_ids` | {"key:Entity": [ids]} | All group node IDs across pages |
| `edge_count` | N | Total edges across pages |
| `page_count` | N | Expected number of pages |
| `no_duplicate_ids` | bool | Assert no ID appears twice |

### Preset system

Fields that accept `PresetOr<T>` can be either a preset name (string) or an inline
value. Presets are defined in `tests/scenarios/presets/`.

| Preset file | Default | Description |
|---|---|---|
| `presets/security.yaml` | `paths: ["1/"]`, Reporter access | Named authorization contexts |
| `presets/redaction.yaml` | `allow_all` (permits all seeded entity IDs) | Named redaction configs |
| `presets/seed.yaml` | — | Named extra seed data |

When `config.redaction` is omitted, the `allow_all` preset is loaded from the file.
When `config.security` is omitted, the harness uses a hardcoded fallback
(`org_id: 1`, `paths: ["1/"]`, Reporter access) without reading the preset file.
Writing `security: default` explicitly reads the `default` entry from
`presets/security.yaml`, which happens to match today but is not guaranteed to stay
in sync. To reference a preset by name: `security: admin`. To inline: provide the
struct fields directly.

### Running scenarios

```sh
mise test:integration:server
```

Scenarios run as part of the server integration test suite. Each scenario file is
discovered and executed automatically.

### Example

```yaml
# source: path/to/rust_file.rs::original_function_name
description: User search returns correct properties

query:
  json: |
    {
      "query_type": "traversal",
      "nodes": [{"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000},
                 "columns": ["username", "state"]}],
      "order_by": "u.id",
      "limit": 10
    }
  gql: |
    MATCH (u:User)
    WHERE u.id >= 1 AND u.id <= 10000
    RETURN u.username, u.state
    ORDER BY u.id LIMIT 10

expect:
  node_count: 7
  nodes:
    User:
      order: [1, 2, 3, 4, 5, 6, 7]
      rows:
        - { id: 1, username: alice, state: active }
        - { id: 5, username: eve, state: blocked }
```

This scenario queries users by ID range, asserts 7 nodes returned in order, and
spot-checks properties on two of them. Both JSON and GQL frontends are tested.
The default security and redaction presets apply since `config` is omitted.
