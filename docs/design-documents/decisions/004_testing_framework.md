---
title: "GKG ADR 004: File-based testing framework for the query and indexing pipelines"
creation-date: "2026-03-10"
authors: [ "@michaelangeloio" ]
toc_hide: true
---

## Status

Proposed

## Date

2026-03-10

## Context

Two integration test suites have grown large enough that the boilerplate-to-signal ratio is a maintenance problem.

### Query pipeline tests

The query engine integration tests (`redaction_integration.rs`, `hydration_integration.rs`) total ~2,500 lines across 60+ test functions. Every test follows the same six-stage pipeline:

1. Insert seed data into ClickHouse
2. Compile a JSON query via the query engine
3. Execute the base query
4. Configure mock redaction (allow/deny entity IDs)
5. Run redaction, optionally hydration
6. Assert on results (authorized IDs, tuples, path nodes, edge kinds)

Each test is a standalone async Rust function with 30-60 lines of boilerplate. The interesting part -- the query JSON, allow/deny rules, and assertions -- is ~5 lines buried in ~40 lines of setup and teardown.

### Indexer SDLC tests

The indexer SDLC integration tests (`sdlc_indexing_integration/`) total ~2,800 lines across 35+ subtests organized by entity type (projects, groups, merge requests, labels, CI pipelines, vulnerabilities, etc.). Every test follows the same three-stage pattern:

1. Seed datalake tables with raw SQL INSERTs (Siphon tables that simulate CDC data arriving from PostgreSQL via NATS)
2. Invoke the handler (get handler from registry, create envelope, call `handler.handle()`)
3. Assert on graph tables (query `gl_*` graph tables, inspect Arrow `RecordBatch` columns, check `gl_edge` rows)

The prerequisite data setup -- namespace, traversal paths, project, project traversal paths -- is repeated identically in ~80% of tests. The handler invocation ceremony (`get_namespace_handler` + `default_test_watermark` + `TestEnvelopeFactory::simple` + `create_namespace_payload` + `create_handler_context` + `handler.handle()`) is 6-8 identical lines in every test. Node assertions are manual Arrow `RecordBatch` inspection: query the graph table, downcast columns, compare values by row index. Edge assertions are the best-factored part, using `assert_edges_have_traversal_path` helpers that reduce 6-8 lines to one call.

### Common problem

Both suites are structurally identical within their domain but each test is a standalone Rust function. Adding a test for a new entity type or a new redaction scenario requires writing Rust, recompiling, and understanding internal API types. The goal is to separate test specifications from test infrastructure so that adding a test means writing a data file, not a Rust function.

## Decision

Use KDL (KDL Document Language) with custom command runners to define integration tests as `.kdl` files. Each runner interprets commands sequentially, wiring into the existing `TestContext` / `run_subtests!` infrastructure. A single Rust `#[test]` function per suite discovers and executes all `.kdl` files in its fixtures directory.

Two separate runners share the same KDL parsing infrastructure but define different command sets for their respective pipelines.

### Query pipeline commands

| Command | Args | Children | Description |
|---|---|---|---|
| `test` | name, `seed=` | -- | Test metadata, loads seed SQL file |
| `extra-sql` | raw SQL string | -- | Additional SQL after seed |
| `compile` | query JSON, `expect=` | -- | Compile query. `expect="error"` for expected failures |
| `execute` | `expect=` (row count) | -- | Execute compiled query |
| `allow` | entity, id... | -- | Add allow rules to mock redaction |
| `deny` | entity, id... | -- | Add deny rules to mock redaction |
| `redact` | `expect=` (redacted count) | -- | Run redaction |
| `reset-redaction` | -- | -- | Reset result to pre-redaction state |
| `hydrate` | -- | -- | Run hydration |
| `hydration-plan` | expected plan name | -- | Assert hydration plan type |
| `authorized-ids` | alias, id... | -- | Assert exact authorized ID set |
| `denied-ids` | alias, id... | -- | Assert IDs absent from results |
| `authorized-tuples` | alias... | `row` children | Assert exact authorized tuples |
| `authorized-count` | expected count | -- | Assert authorized row count |
| `raw-ids` | alias, id... | -- | Assert pre-redaction IDs |
| `raw-count` | expected count | -- | Assert pre-redaction row count |
| `path-node` | index | `id`, `entity-type`, `property` children | Assert path node at index |
| `edge-kinds` | kind... | -- | Assert edge kinds on first authorized path |
| `sql-contains` | fragment | -- | Assert compiled SQL contains fragment |
| `sql-not-contains` | fragment | -- | Assert compiled SQL does not contain fragment |
| `neighbor` | -- | `id`, `entity-type`, `property` children | Assert neighbor node |
| `json-output` | -- | expected JSON children | Assert JSON serialization |

### Indexer SDLC commands

| Command | Args | Children | Description |
|---|---|---|---|
| `test` | name, `handler=`, `seed=` | -- | Test metadata. `handler` is `namespace` or `global`. `seed=` loads prerequisite SQL. |
| `seed-sql` | raw SQL string | -- | Insert rows into datalake (Siphon) tables |
| `handle` | `namespace=`, `organization=` | -- | Invoke the handler with a test envelope. Defaults: `organization=1`. |
| `expect-nodes` | table name, `count=` | column children | Assert graph table rows. Children specify column expectations. |
| `expect-edges` | relationship, source, target | -- | Assert edge count (shorthand for count=any) |
| `expect-edge-count` | relationship, source, target, count | -- | Assert exact edge count |
| `expect-edges-traversal` | relationship, source, target, path, count | -- | Assert edge count with traversal path match |
| `expect-no-edges` | relationship, source, target | -- | Assert zero edges of this type |
| `expect-column` | table, column, row-index, value | -- | Assert a specific cell value after querying the graph table |

### Query pipeline example

```kdl
test "path_finding_hydration_after_partial_redaction" seed="default"

compile """
  {
      "query_type": "path_finding",
      "nodes": [
          {"id": "start", "entity": "User", "node_ids": [1, 2]},
          {"id": "end", "entity": "Project", "node_ids": [1000, 1001]}
      ],
      "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
  }
"""

execute

// Allow user 1's path, deny user 2
allow "user" 1
deny "user" 2
allow "group" 100 101
allow "project" 1000 1001

redact expect=1
hydrate

authorized-count 1

path-node 0 {
    id 1
    entity-type "User"
    property "username" "alice"
}

path-node -1 {
    id 1000
    entity-type "Project"
    property "name" "Public Project"
}

edge-kinds "MEMBER_OF" "CONTAINS"
```

### Indexer SDLC example

The following replaces `processes_labels_with_edges` (85 lines of Rust) with ~30 lines of KDL:

```kdl
test "processes_labels_with_edges" handler="namespace" seed="namespace_with_project"

seed-sql """
  INSERT INTO siphon_labels
      (id, title, color, description, project_id, group_id, traversal_path, _siphon_replicated_at)
  VALUES
      (1, 'bug', '#ff0000', 'Bug reports', 1000, NULL, '1/100/', '2024-01-20 12:00:00'),
      (2, 'feature', '#00ff00', 'New features', 1000, NULL, '1/100/', '2024-01-20 12:00:00'),
      (3, 'priority', '#0000ff', 'Priority items', NULL, 100, '1/100/', '2024-01-20 12:00:00')
"""

handle namespace=100

expect-nodes "gl_label" count=3 {
    column "title" 0 "bug"
    column "title" 1 "feature"
    column "title" 2 "priority"
    column "color" 0 "#ff0000"
    column "color" 1 "#00ff00"
    column "color" 2 "#0000ff"
}

expect-edges-traversal "IN_PROJECT" "Label" "Project" "1/100/" 2
expect-edges-traversal "IN_GROUP" "Label" "Group" "1/100/" 1
```

The `seed="namespace_with_project"` reference loads a shared seed file containing the prerequisite namespace, traversal path, project, and project traversal path INSERTs that are currently duplicated in ~80% of SDLC tests. This single extraction eliminates the largest source of repetition in the indexer test suite.

### Indexer SDLC example: merge request edges

```kdl
test "processes_merge_requests_closing_issues" handler="namespace" seed="namespace_with_project"

seed-sql """
  INSERT INTO siphon_issues (id, title, project_id, author_id, state_id, work_item_type_id, _siphon_replicated_at)
  VALUES
      (1, 'Bug: Login fails', 1000, 1, 1, 0, '2024-01-20 12:00:00'),
      (2, 'Bug: Signup broken', 1000, 1, 1, 0, '2024-01-20 12:00:00')
"""

seed-sql """
  INSERT INTO hierarchy_work_items
      (id, title, author_id, state_id, work_item_type_id, confidential,
       namespace_id, traversal_path, version, custom_status_id, system_defined_status_id)
  VALUES
      (1, 'Bug: Login fails', 1, 1, 0, false, 100, '1/100/', '2024-01-20 12:00:00', 0, 0),
      (2, 'Bug: Signup broken', 1, 1, 0, false, 100, '1/100/', '2024-01-20 12:00:00', 0, 0)
"""

seed-sql """
  INSERT INTO hierarchy_merge_requests
      (id, iid, title, description, source_branch, target_branch, state_id, merge_status,
       draft, squash, target_project_id, author_id, traversal_path, version)
  VALUES
      (10, 101, 'Fix login bug', 'Fixes login issue', 'fix-login', 'main', 3, 'merged',
       false, false, 1000, 1, '1/100/', '2024-01-20 12:00:00'),
      (20, 102, 'Fix signup bug', 'Fixes signup issue', 'fix-signup', 'main', 3, 'merged',
       false, false, 1000, 1, '1/100/', '2024-01-20 12:00:00')
"""

seed-sql """
  INSERT INTO siphon_merge_requests_closing_issues
      (id, merge_request_id, issue_id, project_id, traversal_path,
       created_at, updated_at, _siphon_replicated_at)
  VALUES
      (1, 10, 1, 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
      (2, 20, 2, 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')
"""

handle namespace=100

expect-edges-traversal "CLOSES" "MergeRequest" "WorkItem" "1/100/" 2
```

### Compile error example

```kdl
test "limit_and_range_mutually_exclusive"

compile expect="error" """
  {"query_type": "search", "node": {"id": "u", "entity": "User"}, "limit": 10, "range": {"start": 0, "end": 5}}
"""
```

### Tests that stay as Rust

Some tests are genuinely procedural and do not benefit from a declarative format.

**Query pipeline:**

- **Hand-constructed Arrow batches:** `fail_closed_null_id_denies_row` and `fail_closed_null_type_denies_row` build `RecordBatch` objects directly without the query engine. No SQL, no pipeline.
- **Complex error pattern matching:** Tests that call `compile().unwrap_err()` and match on specific error enum variants. KDL handles `expect="error"` for simple cases, but deep pattern matching on error enums is better in Rust.
- **Heavy custom setup:** Tests like `cross_entity_id_collision_redaction` with elaborate seed data beyond `extra-sql`. If the setup logic has conditionals or loops, Rust is clearer.

**Indexer:**

- **Code indexing tests:** These involve starting a real Gitaly container, creating Git repositories via shell commands inside the container, constructing protobuf `ReplicationEvent` messages, and running multi-step reindexing workflows. The procedural logic and external container dependency do not reduce to declarative fixtures.
- **Engine, NATS, and dispatcher tests:** These test infrastructure-level concerns -- message flow, pub/sub, container lifecycle, concurrency, lock contention. They are inherently procedural.
- **Watermarking tests:** Tests that verify incremental processing semantics (re-running the handler with a newer watermark updates only changed rows) require multiple handler invocations with different state, which the sequential command model can express but Rust is clearer for.

Estimated split: 70-80% of query pipeline tests and ~80% of indexer SDLC tests convert to KDL. Engine, NATS, dispatcher, code indexing, and the procedural query tests stay as Rust.

## Why a datadriven command model

The CockroachDB `datadriven` library established the pattern: sequential command blocks in a file, each producing output compared against expected values. You define what each command means; the framework handles file parsing and output comparison. CockroachDB uses this for SQL optimizer testing (`exec-sql`, `trace-sql`, `normalize`, `opt`), Pebble uses it for storage engine testing (`define`, `ingest`, `compact`, `iter`, `get`).

The key property is that the framework is command-agnostic. Unlike `sqllogictest` -- which assumes a single-step pipeline (SQL in, rows out, compare) -- a command-based model supports intermediate stages. The query pipeline has stages that need configuration between steps: mock auth rules must be set between execution and redaction, assertions on path node properties happen after hydration, state can be reset mid-test. The indexer pipeline has a simpler shape (seed, handle, assert) but the same need for structured multi-table assertions that sqllogictest cannot express.

There is no way to express `allow "user" 1 2 3` or `expect-edges-traversal "IN_PROJECT" "Label" "Project" "1/100/" 2` in sqllogictest without encoding them as fake SQL `SET` statements inside the `DB::run()` implementation, which fights the framework rather than using it.

`pg_regress` (PostgreSQL's two-file model: input SQL + expected output) has the same fundamental limitation -- SQL-only, no structured assertions, and the two-file coupling adds maintenance overhead. RegreSQL (parameterized SQL regression) is designed for application query testing, not pipeline testing.

## Why KDL

KDL's syntax -- positional args, named properties, and child blocks on the same node -- maps directly to pipeline commands without parsing overhead:

```kdl
allow "user" 1 2 3
redact expect=2
path-node 0 {
    id 1
    entity-type "User"
    property "username" "alice"
}
```

This reads like CLI invocations. The `kdl` crate (v6.x, maintained by the KDL spec authors) handles parsing, error messages with line numbers, and node ordering. Adding a new command requires only a new match arm in the runner -- no parser changes, no serde enum variants.

The same property makes KDL work for the indexer's simpler pattern. Positional args handle the common case (`expect-edges-traversal "IN_PROJECT" "Label" "Project" "1/100/" 2`), while child blocks handle structured node assertions without requiring a separate assertion syntax.

Four file formats were evaluated:

| Criterion | Raw Text | TOML | YAML | KDL |
|---|---|---|---|---|
| Lines per simple test | ~10 | ~28 | ~14 | ~10 |
| Lines per complex test | ~40 | ~80 | ~50 | ~35 |
| Custom parser needed | ~80 lines | No (serde) | No (serde) | No (`kdl` crate) |
| Structural validation | Custom registry | serde | serde | `kdl` crate + optional schema |
| Readability | Excellent | Okay | Good | Excellent |
| Fragility | Blank-line sensitive | Robust | Indent sensitive | Robust |
| `--rewrite` difficulty | Trivial | Hard | Hard | Medium |
| Adding new commands | Match arm only | Match arm + serde enum | Match arm + serde enum | Match arm only |

TOML requires `[[steps]]` / `cmd = "allow"` / `args = "user 1 2 3"` boilerplate on every block -- roughly 2-3x the line count. Round-trip serialization that preserves comments is hard, making `--rewrite` (automatic expected-output update) impractical.

YAML's indentation sensitivity and type coercion (`no` -> `false`, bare numbers) introduce bugs that are silent until runtime. Serde untagged enums for "command name as key" are verbose in Rust types.

Raw text (the datadriven format itself) is the closest fallback. It provides nearly the same DX and trivial `--rewrite`. The downsides are blank-line sensitivity (one wrong blank line breaks parsing) and no IDE support -- no syntax highlighting, no parse-time error messages. A typed command registry closes the safety gap but requires ~80 lines of custom parsing.

KDL provides the readability of raw text with the structural safety of a real parser. It is less well-known than TOML or YAML, which is a learning cost, but the syntax is small enough that the examples in this ADR are sufficient to write tests.

## CI integration

Each suite has its own fixtures directory and test runner, but both share the same KDL parsing infrastructure (the `kdl` crate) and the `TestContext` from `integration-testkit`.

### Query pipeline

The `.kdl` test files live in `crates/query-engine/tests/fixtures/`. A single Rust `#[tokio::test]` function discovers all `.kdl` files via `glob`, parses each one, and runs the command sequence against a real ClickHouse testcontainer:

```rust
#[tokio::test]
async fn kdl_integration_tests() {
    let ctx = TestContext::new().await;
    for path in glob("tests/fixtures/**/*.kdl") {
        let doc = kdl::parse(std::fs::read_to_string(&path).unwrap()).unwrap();
        run_kdl_test(&ctx, &doc, &path).await;
    }
}
```

Seed data lives in `crates/query-engine/tests/seeds/*.sql`. The existing `setup_test_data` and `setup_indirect_auth_data` functions are extracted into `default.sql` and `with_code_entities.sql` respectively.

### Indexer SDLC

The `.kdl` test files live in `crates/indexer/tests/fixtures/sdlc/`. The runner uses the existing `TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL])` setup and the `IndexerTestExt` trait for handler creation:

```rust
#[tokio::test]
async fn kdl_sdlc_integration_tests() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL]).await;
    for path in glob("tests/fixtures/sdlc/**/*.kdl") {
        let forked = ctx.fork(&test_name_from_path(&path)).await;
        let doc = kdl::parse(std::fs::read_to_string(&path).unwrap()).unwrap();
        run_kdl_indexer_test(&forked, &doc, &path).await;
    }
}
```

Each `.kdl` file gets a forked database via `ctx.fork()`, matching the existing `run_subtests!` pattern that gives each subtest an isolated database on the same ClickHouse container.

Shared prerequisite data lives in `crates/indexer/tests/seeds/`. The most common seed -- namespace 100 with traversal path `1/100/`, project 1000 with traversal path `1/100/1000/` -- is extracted into `namespace_with_project.sql`, eliminating the INSERTs duplicated in ~80% of current tests. The `seed=` property loads this before the test's `seed-sql` blocks run.

### Shared infrastructure

Both runners run under `mise test:integration` and `cargo nextest`. No new CI job is needed -- the existing `integration-test` pipeline stage picks up the new test functions automatically.

A separate validation-only test per suite parses all `.kdl` files without executing them, catching syntax errors before the integration test stage:

```rust
#[test]
fn kdl_fixtures_parse() {
    for path in glob("tests/fixtures/**/*.kdl") {
        kdl::parse(std::fs::read_to_string(&path).unwrap())
            .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    }
}
```

This runs under `mise test:fast` / `cargo nextest --lib` as a unit test. A malformed `.kdl` file fails fast in the unit test stage, not 5 minutes into the integration test stage.

During the transition period, both the original Rust tests and the new KDL tests run in CI. Once a Rust test has a KDL equivalent that passes, the Rust version is deleted.

## Known gaps

The command sets in this ADR cover the current test suites. As new pipeline features (aggregation queries, Cypher compilation, response formatting) or new entity types are added, new commands will be needed. Adding a command requires a match arm in the runner and a row in the command table above -- low cost, but the ADR cannot enumerate future commands.

The `expect=` and `count=` properties use exact values. There is no mechanism for approximate matching, regex assertions, or ordered subset assertions. If a test needs "at least 3 rows" rather than "exactly 3 rows", it must be a Rust test. This could be addressed later with comparison operators (`expect>=3`) if the need arises.

Error message assertions are limited. `compile expect="error"` asserts that compilation fails, but does not match on the error message or error type. Tests that need to distinguish between "invalid entity type" and "mutually exclusive parameters" must stay in Rust. A future `expect="error: substring"` extension is straightforward.

`--rewrite` support (automatically updating expected values in `.kdl` files when test output changes) is not part of the initial implementation. The `kdl` crate preserves node structure but not whitespace formatting, so rewriting requires re-serializing the document. This is a medium-difficulty task, not a blocker.

There is no parallelism within a single `.kdl` file -- commands execute sequentially, sharing a single `TestContext`. Parallelism comes from nextest running multiple test files concurrently. For the indexer runner, each `.kdl` file gets a forked database (`ctx.fork()`), matching the existing `run_subtests!` model.

The indexer runner depends on the `IndexerTestExt` trait and `HandlerRegistry` internals. If the handler registration API changes (new handler types, different configuration patterns), the runner needs updating. This coupling is the same as the existing Rust tests have -- the runner does not add new coupling, it just inherits the existing one.

Column value assertions in the indexer runner compare string representations. ClickHouse returns typed data via Arrow, but the KDL format only has strings and numbers. The runner must handle type coercion (e.g., boolean columns returned as `UInt8` in Arrow, compared against `true`/`false` strings in KDL). Edge cases in type mapping could produce false failures until the coercion logic is comprehensive.

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| `sqllogictest` format | Assumes single-step SQL pipeline. No mechanism for mid-pipeline configuration or multi-stage assertions. The `sqllogictest-rs` crate's `AsyncDB` trait has a single `run(sql)` method -- there is no hook for "now configure auth rules" between steps, and no way to express "invoke this handler then check these graph tables". |
| `pg_regress` two-file model | String diffing of full output, no structured assertions. Two-file coupling (input + expected) is maintenance overhead. Platform-dependent output formatting causes false failures. |
| Raw text (datadriven-style) | Viable fallback. Blank-line sensitivity and no IDE support are the main downsides. If KDL adoption is a concern, this is the second choice -- the runner's command dispatch logic is identical, only the parser changes. |
| Keep everything as Rust | The 30-60 lines of boilerplate per test, recompilation on every change, and the Rust knowledge requirement for test authors are real costs that scale linearly with test count. At 95+ tests across both suites, the maintenance burden is already significant; as more entity types are indexed (new ontology YAML -> new indexer handlers -> new tests), it would be worse. |
| Separate frameworks per suite | Using KDL for query tests and a different approach (e.g., YAML fixtures) for indexer tests. Rejected because the same team maintains both, the same `TestContext` underlies both, and having two test DSLs doubles the learning cost for no gain. |

## Consequences

### Dev-dependency budget

Adding the `kdl` crate as a dev-dependency to both `query-engine` and `indexer`. The crate is well-maintained (v6.x, by the KDL spec authors) but younger than `toml` or `serde_yaml`. It compiles in ~2 seconds and has no transitive dependencies. It is a test-only dependency -- no production binary size impact, no `cargo audit` / `cargo deny` exposure in the release artifact.

### Test authoring workflow

Adding a new integration test changes from "write 40-85 lines of async Rust, recompile (~30s incremental), run" to "write 10-30 lines of KDL, run". The recompilation step is eliminated for test-only changes. This matters most for the indexer: when a new entity type is added to the ontology, the corresponding integration test is now a KDL file rather than a new Rust module with repeated prerequisite INSERTs.

The tradeoff is that KDL is a format most engineers have not seen. The learning curve is minimal -- KDL reads like CLI commands -- but it is nonzero. The command tables and examples in this ADR serve as the reference. The `kdl_fixtures_parse` unit test catches syntax errors immediately.

### Shared seed data

Seed SQL files become shared fixtures in both suites. For the query engine: `crates/query-engine/tests/seeds/*.sql`. For the indexer: `crates/indexer/tests/seeds/*.sql` (with `namespace_with_project.sql` eliminating the namespace/project prerequisite duplication across ~80% of SDLC tests).

Changes to seed data affect all tests that reference that seed. This is intentional -- it forces tests to be explicit about what data they need and prevents seed drift where each test has slightly different setup. The downside is that a seed change can break multiple tests at once. `extra-sql` / `seed-sql` provides an escape hatch for test-specific data without modifying the shared seed.

### Runner maintenance

Two runners (~200 lines each) are new infrastructure. The query runner depends on `QueryResult`, `SecurityContext`, and the redaction API. The indexer runner depends on `IndexerTestExt`, `HandlerRegistry`, and `TestEnvelopeFactory`. Query engine or indexer refactors will require runner updates. This couples each runner to its respective crate's internal API surface. The alternative -- a stable public test API -- would be premature abstraction; both pipelines are still evolving.

Command dispatch is a match arm per command. Adding a command is low-cost. Removing a command that existing `.kdl` files use will fail the `kdl_fixtures_parse` validation test immediately, not silently.

### Test reliability

KDL tests are deterministic in the same way the existing Rust tests are -- same seed data, same ClickHouse testcontainer, same assertions. The `.kdl` format does not introduce flakiness. However, the glob-based test discovery means adding a `.kdl` file to the fixtures directory automatically runs it in CI. There is no explicit test registration step, which is both a feature (no boilerplate to add a test) and a risk (a WIP file accidentally committed will run and fail).

## References

- [CockroachDB `datadriven`](https://github.com/cockroachdb/datadriven) -- the command-based test pattern this design follows
- [KDL Document Language](https://kdl.dev/) -- file format specification
- [KDL Rust crate](https://crates.io/crates/kdl) -- parser used by the test runner
- [`sqllogictest` original (SQLite)](https://sqlite.org/sqllogictest) -- surveyed, not adopted
- [`sqllogictest-rs` (Rust crate)](https://github.com/risinglightdb/sqllogictest-rs) -- surveyed, not adopted
- [ADR 001: gRPC Communication Protocol](001_grpc_communication.md)
- [ADR 002: Rust Core Runtime](002_rust_core_runtime.md)
