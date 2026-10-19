# integration-testkit

Shared test infrastructure for integration tests that need a real ClickHouse instance.

## What it provides

- **`TestContext`**: Connects to the shared ClickHouse test service, runs schema DDL,
  and exposes `query()`, `execute()`, and `query_parameterized()` for Arrow-based results.
- **`TestContext::fork()`**: Clones an isolated database per subtest so subtests can run
  in parallel against one container.
- **`TestContext::optimize_all()`**: Queries `system.tables` for the current database and
  runs `OPTIMIZE TABLE … FINAL` concurrently on every table. Call after seeding data.
- **`run_subtests_shared!`**: Macro that runs all subtests in parallel against the same
  shared database. Use for read-only subtests.
- **`run_subtests!`**: Macro that forks a database per subtest and runs them concurrently.
  Use for subtests that write data beyond the initial seed.
- **Arrow extractors**: `get_string_column`, `get_int64_column`, `get_uint64_column`,
  `get_boolean_column` for pulling typed columns out of `RecordBatch`.
- **`ResponseView`**: Typed wrapper over `GraphResponse` for asserting query pipeline
  output.
- **`query_scenario`**: Runs YAML query scenarios through the full query pipeline.
- **`scenario`**: Runs YAML indexer scenarios: seeds source rows, runs handlers, and checks the graph.
- **`plan_shape`**: Compiles plan-shape fixtures and checks the selected plan.

## Prerequisites

Integration tests need a Docker-compatible runtime. On macOS, use the `gkg` Colima profile:

```shell
colima start gkg --memory 12
mise test:integration
colima stop gkg
```

The `test:integration` tasks set `DOCKER_HOST` and start one ClickHouse container.
Local tasks reuse the named `gkg-clickhouse-local` container. Remove it to force a clean server.
Run container tests through the mise tasks so the shared service is configured.

## Usage

Only `integration-tests` depends on this crate, in its `[dev-dependencies]`.

```rust
use integration_testkit::{TestContext, run_subtests, GRAPH_SCHEMA_SQL};

#[tokio::test]
async fn my_integration_test() {
    let ctx = TestContext::new(&[GRAPH_SCHEMA_SQL]).await;
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
                          GRAPH_SCHEMA_SQL};

#[tokio::test]
async fn my_test_suite() {
    let ctx = TestContext::new(&[GRAPH_SCHEMA_SQL]).await;
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
typed helpers for looking up nodes, edges, and paths.

### Creating a view

Use `from_response` with the formatted `GraphResponse`:

```rust
use integration_testkit::visitor::{ResponseView, NodeExt};
let resp = ResponseView::from_response(response);
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

    resp.assert_filter("User", "state", |n| {
        n.prop_str("state") == Some("blocked")
    });
    resp.assert_node_order("User", &[5]);
}
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

Query scenarios are YAML data correctness tests. Each scenario
declares a query, optional config overrides, and expected results. The harness seeds data,
runs the full pipeline (compile, execute, redact, hydrate, paginate, format), and checks
the response against the expectations.

Every response must match the response JSON schema. Numeric metric assertions
require JSON numbers. Explicit `null` values require a present null property;
use `prop_absent` when a property may be missing.

### File structure

Scenarios are in `crates/integration-tests/tests/server/data_correctness/scenarios/<category>/`.
Presets are in the sibling `data_correctness/presets/` directory.

```plaintext
data_correctness/
├── presets/
│   ├── redaction.yaml
│   ├── security.yaml
│   └── seed.yaml
└── scenarios/
    ├── aggregation/
    │   └── count_authored_mrs.yaml
    ├── search/
    │   └── contains_case_insensitive.yaml
    └── security/
        └── admin_aggregation_compiles.yaml
```

Each YAML file contains one `QueryScenario` document, or a `cases` list of them.
In a `cases` file, every other top-level key is a default for each case. A case
key replaces the default, and nested maps such as `config` merge key by key.
A case reports failures as `<file>/<name>`, or `<file>/<index>` when it has no
`name`.

```yaml
config:
  security: { org_id: 1, paths: ["1/100/"], access_level: 20 }
cases:
  - name: not_in
    query:
      gql: MATCH (u:User {id: 1}) WHERE NOT u.username IN ['bob'] RETURN u
    expect: { node_count: 1 }
  - name: admin_not_in
    config: { security: admin_user }
    query:
      gql: MATCH (u:User {id: 2}) WHERE NOT u.username IN ['bob'] RETURN u
    expect: { node_count: 0 }
```

A scenario can set `config.ontology_overlay` to a name under
`config/seeds/overlays/`. The overlay changes the query catalog for that scenario.
Use the suite-wide `GKG_TEST_ONTOLOGY_OVERLAY` when the overlay also changes DDL.

### `QueryScenario` format

#### Top-level fields

| Field | Type | Required | Description |
|---|---|---|---|
| `description` | string | no | Human-readable summary |
| `config` | `ScenarioConfig` | no | Security, redaction, and seed overrides |
| `query` | map | one of `query`/`steps` | Query strings keyed by frontend (`json`, `gql`) |
| `expect` | `QueryExpect` | with `query` | Assertions on the result |
| `steps` | [`QueryStep`] | one of `query`/`steps` | Ordered steps, each with an optional `seed` (rows inserted before the step runs), a `query` map, and an `expect`. Use it when a later query must observe a write made after an earlier one |

#### `config` — `ScenarioConfig`

| Field | Type | Description |
|---|---|---|
| `extra_seed` | `Seed` | Additional rows to insert; triggers a DB fork |
| `unmerged_seed` | bool | Default `false`. For extra seed rows, stop background merges on their tables, disable insert optimization, insert each row as its own part, and skip `OPTIMIZE`. Uses physical table names, such as `gl_project`. |
| `security` | preset name or inline `SecurityOverride` | Authorization context |
| `redaction` | preset name or inline `RedactionConfig` | Entity-level redaction |
| `duckdb` | bool | Default `false`. Also run the scenario in local mode on an embedded DuckDB, through `run_duckdb_dir` in `cargo test --test local`. The runner creates the local tables from the ontology, inserts the preset and `extra_seed` rows of those tables, and applies the same `expect`. Local mode has no security or redaction, so use it only for scenarios on local entities, such as `Definition` and `File`, with CALLS rows in `gl_edge`. |

#### `config.security` — `SecurityOverride`

| Field | Type | Default | Description |
|---|---|---|---|
| `admin` | bool | false | Run as admin |
| `paths` | [string] | `["1/"]` | Uniform authorized namespace paths |
| `authorized_paths` | [{path, access_level}] | — | Per-path access levels |
| `org_id` | i64 | — | Organization ID |
| `access_level` | u32 | 20 (Reporter) | Default access level |

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
| `edge_exists` | {TYPE: [edge]} | Subset check; each edge is an ID pair or a typed object |
| `edge_absent` | {TYPE: [edge]} | Negative check; each edge is an ID pair or a typed object |
| `edge_count` | {TYPE: N} | Per-type edge count |
| `referential_integrity` | bool | All edge endpoints have nodes |
| `has_more` | bool | Pagination flag |
| `gql_columns` | {column: [cell]} | Exact cells of a `gql` format table column, in row order |
| `gql_column_names` | [column] | Exact `gql` format table header, in order |

Typed edge objects use `{from: User, from_id: 1, to: Group, to_id: 100}`.
They check endpoint types as well as IDs. Pair arrays such as `[1, 100]` check
only IDs and the relationship type.

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
| `path_edges` | `[[{from, from_id, type, to, to_id, step}]]` | Per-path edge structure (all fields optional) |
| `path_endpoint_absent` | [Entity] | Entities excluded from path edges |

**Compilation:**

| Field | Type | Description |
|---|---|---|
| `compile_error` | bool / string / {frontend: string} | Expect compile failure |
| `compile_error_contains` | [string] / {frontend: [string]} | Error must contain every listed fragment |
| `compile_error_not_contains` | [string] / {frontend: [string]} | Error must NOT contain |

**Pagination:**

| Field | Type | Description |
|---|---|---|
| `pages` | [`QueryExpect`] | Multi-page assertions with auto cursor chaining; the last page must exhaust the cursor |
| `all_pages` | `AllPagesExpect` | Cross-page assertions |

**Other:**

| Field | Type | Description |
|---|---|---|
| `repeat_count` | N | Recompile and run N times; compare node order within each entity type, properties, edges, aggregation rows, and pagination |

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
| `rows` | `[{id, prop: val}]` | Per-node property assertions |

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
| `rows` | `[{entity, id, values: {col: val}, properties: {prop: val}}]` | Per-row assertions |
| `absent` | `[{entity, id}]` | Must NOT appear |

#### `AllPagesExpect`

| Field | Type | Description |
|---|---|---|
| `node_ids` | {Entity: [ids]} | Sorted unique node IDs across pages |
| `node_order` | {Entity: [ids]} | Exact node ID sequence across pages |
| `group_node_ids` | {"key:Entity": [ids]} | All group node IDs across pages |
| `edge_count` | N | Total edges across pages |
| `page_count` | N | Expected number of pages |
| `no_duplicate_ids` | bool | Reject repeated node or group-node IDs across pages |
| `no_duplicate_edges` | bool | Reject repeated typed edges across pages |
| `matches_unpaginated` | bool | Compare combined typed node IDs, typed edges, group rows, and paths with the same query without a cursor |

Cursor responses must keep `has_more` and `next_cursor` consistent. The runner
decodes each token and checks its keys against the last authorized row when one
exists. Unpaginated comparisons use a limit of 1000 and fail if results exceed it.
GQL comparison queries must end with `PAGE N`.
Node properties are checked per page because excerpt lengths depend on page size.

Per-frontend error expectations must include every frontend in the scenario.

### Preset system

Fields that accept `PresetOr<T>` can be either a preset name (string) or an inline
value. Presets are defined in `tests/scenarios/presets/`.

| Preset file | Default | Description |
|---|---|---|
| `presets/security.yaml` | `paths: ["1/"]`, the Reporter role | Named authorization contexts |
| `presets/redaction.yaml` | `allow_all` (permits all seeded entity IDs) | Named redaction configs |
| `presets/seed.yaml` | — | Named extra seed data |

When `config.security` or `config.redaction` is omitted, the default preset applies.
To reference a preset by name: `security: admin_user`. To inline: provide the struct fields directly.

### Running scenarios

```shell
mise test:integration:data
SCENARIO_FILTER=search mise test:integration:data
```

The harness finds each scenario file and case. `SCENARIO_FILTER` keeps the cases whose
name contains the filter. The `query_scenario_fixtures_parse` test in the `local` binary
parses every file with no Docker.

### Example

```yaml
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
