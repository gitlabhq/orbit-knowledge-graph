---
title: "GKG ADR 004: File-based testing framework for the query and indexing pipelines"
creation-date: "2026-03-10"
authors: [ "@michaelusa" ]
toc_hide: true
---

## Status

Proposed

## Date

2026-03-10

## Context

Two integration test suites have the same problem: most of the code is boilerplate.

### Query pipeline tests

The query engine integration tests (`redaction_integration.rs`, `hydration_integration.rs`) total ~2,500 lines across 60+ test functions. Every test follows the same six-stage pipeline:

1. Insert seed data into ClickHouse
2. Compile a JSON query via the query engine
3. Execute the base query
4. Configure mock redaction (allow/deny entity IDs)
5. Run redaction, optionally hydration
6. Assert on results (authorized IDs, tuples, path nodes, edge kinds)

Each test is a standalone async Rust function with 30-60 lines of boilerplate. The actual test logic -- the query JSON, allow/deny rules, and assertions -- is ~5 lines buried in ~40 lines of setup.

### Indexer SDLC tests

The indexer SDLC integration tests (`sdlc_indexing_integration/`) total ~2,800 lines across 35+ subtests organized by entity type (projects, groups, merge requests, labels, CI pipelines, vulnerabilities, etc.). Every test follows the same three-stage pattern:

1. Seed datalake tables with raw SQL INSERTs (Siphon tables that simulate CDC data arriving from PostgreSQL via NATS)
2. Invoke the handler (get handler from registry, create envelope, call `handler.handle()`)
3. Assert on graph tables (query `gl_*` graph tables, inspect Arrow `RecordBatch` columns, check `gl_edge` rows)

The prerequisite data setup -- namespace, traversal paths, project, project traversal paths -- is copy-pasted identically in ~80% of tests. The handler invocation ceremony (`get_namespace_handler` + `default_test_watermark` + `TestEnvelopeFactory::simple` + `create_namespace_payload` + `create_handler_context` + `handler.handle()`) is 6-8 identical lines in every test. Node assertions are manual Arrow `RecordBatch` inspection: query the graph table, downcast columns, compare values by row index. The edge assertion helpers (`assert_edges_have_traversal_path` etc.) are the one well-factored part -- they reduce 6-8 lines to one call.

### Common problem

Both suites are structurally identical within their domain, but each test is a standalone Rust function. Adding a test for a new entity type or a new redaction scenario means writing Rust, recompiling, and understanding internal API types. We want adding a test to mean writing a data file, not a Rust function.

## Decision

Use KDL (KDL Document Language) with custom command runners to define integration tests as `.kdl` files. Each runner interprets commands top-to-bottom, wiring into the existing `TestContext` / `run_subtests!` infrastructure. One Rust `#[test]` function per suite discovers and runs all `.kdl` files in its fixtures directory.

Two runners share the KDL parsing infrastructure but define different command sets.

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

`processes_labels_with_edges` is 85 lines of Rust. The KDL equivalent:

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

`seed="namespace_with_project"` loads a shared SQL file with the namespace, traversal path, project, and project traversal path INSERTs that are currently copy-pasted in ~80% of SDLC tests.

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

Some tests are procedural enough that a declarative format would make them harder to read, not easier.

**Query pipeline:**

- **Hand-constructed Arrow batches:** `fail_closed_null_id_denies_row` and `fail_closed_null_type_denies_row` build `RecordBatch` objects directly without the query engine. No SQL, no pipeline.
- **Complex error pattern matching:** Tests that call `compile().unwrap_err()` and match on specific error enum variants. KDL handles `expect="error"` for simple cases, but deep pattern matching on error enums is better in Rust.
- **Heavy custom setup:** Tests like `cross_entity_id_collision_redaction` with elaborate seed data beyond `extra-sql`. If the setup logic has conditionals or loops, Rust is clearer.

**Indexer:**

- **Code indexing tests:** These start a real Gitaly container, create Git repos via shell commands inside it, construct protobuf `ReplicationEvent` messages, and run multi-step reindexing workflows. Too much procedural logic and external state for declarative fixtures.
- **Engine, NATS, and dispatcher tests:** Message flow, pub/sub, container lifecycle, concurrency, lock contention. Procedural by nature.
- **Watermarking tests:** Tests that verify incremental processing semantics (re-running the handler with a newer watermark updates only changed rows) require multiple handler invocations with different state, which the sequential command model can express but Rust is clearer for.

Estimated split: 70-80% of query pipeline tests and ~80% of indexer SDLC tests convert to KDL. Engine, NATS, dispatcher, code indexing, and the procedural query tests stay as Rust.

## Why a datadriven command model

CockroachDB's `datadriven` library established this pattern: sequential command blocks in a file, each producing output compared against expected values. You define what each command means; the framework parses files and compares output. CockroachDB uses it for optimizer testing (`exec-sql`, `trace-sql`, `normalize`, `opt`); Pebble uses it for storage engine testing (`define`, `ingest`, `compact`, `iter`, `get`).

The framework is command-agnostic, which is why it fits. `sqllogictest` assumes a single-step pipeline (SQL in, rows out, compare). Our query pipeline needs configuration between steps -- mock auth rules between execution and redaction, assertions on path nodes after hydration, state resets mid-test. The indexer pipeline is simpler (seed, handle, assert) but still needs structured multi-table assertions that sqllogictest cannot express.

There is no way to express `allow "user" 1 2 3` or `expect-edges-traversal "IN_PROJECT" "Label" "Project" "1/100/" 2` in sqllogictest without encoding them as fake SQL `SET` statements inside `DB::run()`, which fights the framework.

`pg_regress` (PostgreSQL's two-file model: input SQL + expected output) has the same limitation -- SQL-only, no structured assertions, and two-file coupling. RegreSQL is for application query regression, not pipeline testing.

## Why KDL

KDL nodes have positional args, named properties, and child blocks on the same line. This maps directly to pipeline commands:

```kdl
allow "user" 1 2 3
redact expect=2
path-node 0 {
    id 1
    entity-type "User"
    property "username" "alice"
}
```

It reads like CLI invocations. The `kdl` crate (v6.x, maintained by the KDL spec authors) handles parsing, error messages with line numbers, and node ordering. Adding a command is a match arm in the runner -- no parser changes, no serde enum variants.

The same works for the indexer. Positional args handle the common case (`expect-edges-traversal "IN_PROJECT" "Label" "Project" "1/100/" 2`); child blocks handle structured node assertions.

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

TOML requires `[[steps]]` / `cmd = "allow"` / `args = "user 1 2 3"` boilerplate on every block, roughly 2-3x the line count. Round-trip serialization that preserves comments is hard, so `--rewrite` (automatic expected-output update) is impractical.

YAML's indentation sensitivity and type coercion (`no` -> `false`, bare numbers) introduce silent bugs. Serde untagged enums for "command name as key" are verbose in Rust.

Raw text (the original datadriven format) is the closest fallback. Nearly the same DX, trivial `--rewrite`. The downsides: blank-line sensitivity (one wrong blank line breaks parsing), no IDE support, no parse-time error messages. A typed command registry closes the safety gap but needs ~80 lines of custom parsing.

KDL is less well-known than TOML or YAML, but the syntax is small -- the examples in this ADR are enough to write tests. It gives you raw-text readability with a real parser underneath.

## CI integration

Each suite has its own fixtures directory and runner. Both use the `kdl` crate for parsing and `TestContext` from `integration-testkit`.

### Query pipeline

`.kdl` files live in `crates/query-engine/tests/fixtures/`. One `#[tokio::test]` function globs them and runs each against a ClickHouse testcontainer:

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

Seeds live in `crates/query-engine/tests/seeds/*.sql`. The existing `setup_test_data` and `setup_indirect_auth_data` get extracted into `default.sql` and `with_code_entities.sql`.

### Indexer SDLC

`.kdl` files live in `crates/indexer/tests/fixtures/sdlc/`. The runner uses `TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL])` and the `IndexerTestExt` trait for handler creation:

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

Each `.kdl` file gets a forked database via `ctx.fork()`, same as the existing `run_subtests!` pattern -- isolated database per subtest, shared ClickHouse container.

Shared prerequisites live in `crates/indexer/tests/seeds/`. The most common seed -- namespace 100 with traversal path `1/100/`, project 1000 with path `1/100/1000/` -- goes into `namespace_with_project.sql`. The `seed=` property loads this before `seed-sql` blocks run.

### Shared infrastructure

Both runners run under `mise test:integration` and `cargo nextest`. No new CI job -- the existing `integration-test` stage picks up the new test functions.

A validation-only test per suite parses all `.kdl` files without executing them, catching syntax errors before integration tests run:

```rust
#[test]
fn kdl_fixtures_parse() {
    for path in glob("tests/fixtures/**/*.kdl") {
        kdl::parse(std::fs::read_to_string(&path).unwrap())
            .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    }
}
```

This runs under `mise test:fast` / `cargo nextest --lib` as a unit test. Malformed `.kdl` files fail here, not 5 minutes into the integration stage.

During transition, both Rust and KDL tests run in CI. Once a KDL test passes, the Rust version gets deleted.

## Known gaps

The command sets here cover the current tests. New pipeline features (aggregation, Cypher, response formatting) or new entity types will need new commands. Adding one is a match arm and a table row, but this ADR can't enumerate what doesn't exist yet.

`expect=` and `count=` are exact values only. No approximate matching, no regex, no subset assertions. "At least 3 rows" instead of "exactly 3 rows" means a Rust test. Comparison operators (`expect>=3`) could be added later if needed.

`compile expect="error"` asserts failure but doesn't match on the message or error type. Tests that need to distinguish "invalid entity type" from "mutually exclusive parameters" stay in Rust. An `expect="error: substring"` extension would be straightforward to add.

No `--rewrite` support initially (automatically updating expected values when output changes). The `kdl` crate preserves node structure but not whitespace, so rewriting means re-serializing the whole document. Doable, just not free.

No parallelism within a `.kdl` file -- commands run sequentially on one `TestContext`. Parallelism comes from nextest running files concurrently. The indexer runner forks a database per file (`ctx.fork()`), same as `run_subtests!`.

The indexer runner depends on `IndexerTestExt` and `HandlerRegistry` internals. If the handler registration API changes, the runner needs updating. This is the same coupling the existing Rust tests already have -- the runner inherits it, doesn't add to it.

Column assertions in the indexer runner compare string representations. ClickHouse returns typed Arrow data, but KDL only has strings and numbers. The runner has to coerce types (e.g., boolean columns come back as `UInt8` in Arrow, compared against `true`/`false` in KDL). There will be edge cases in this mapping until the coercion logic covers all the column types we actually use.

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| `sqllogictest` format | Single-step SQL pipeline only. `sqllogictest-rs`'s `AsyncDB` trait has one method: `run(sql)`. No hook for "now configure auth rules" between steps, no way to say "invoke this handler then check these graph tables". |
| `pg_regress` two-file model | String diffing of full output, no structured assertions. Two-file coupling (input + expected) is annoying to maintain. Platform-dependent formatting causes false failures. |
| Raw text (datadriven-style) | Viable fallback. Blank-line sensitivity and no IDE support are the main downsides. Second choice if KDL adoption is a concern -- the dispatch logic is identical, only the parser changes. |
| Keep everything as Rust | 30-60 lines of boilerplate per test, recompilation on every change, Rust knowledge required to add a test. At 95+ tests across both suites this is already painful; it gets worse as new entity types land (ontology YAML -> handler -> test). |
| Separate frameworks per suite | KDL for query tests, something else (YAML fixtures?) for indexer. Same team, same `TestContext`, two DSLs to learn. No upside. |

## Consequences

### Dev-dependency budget

`kdl` crate added as a dev-dependency to both `query-engine` and `indexer`. v6.x, maintained by the KDL spec authors, compiles in ~2s, no transitive dependencies. Test-only -- no production binary impact, no `cargo audit` / `cargo deny` exposure in the release artifact.

### Test authoring workflow

Adding a test goes from "write 40-85 lines of async Rust, wait for incremental compile (~30s), run" to "write 10-30 lines of KDL, run". No recompilation for test-only changes. For the indexer specifically, a new entity type in the ontology now means a `.kdl` file instead of a new Rust module with the same prerequisite INSERTs copy-pasted again.

Most engineers haven't seen KDL. The syntax is small enough that the examples here are the entire learning curve, and `kdl_fixtures_parse` catches syntax errors immediately.

### Shared seed data

Seed SQL files become shared fixtures: `crates/query-engine/tests/seeds/*.sql` and `crates/indexer/tests/seeds/*.sql` (`namespace_with_project.sql` replaces the namespace/project prerequisite duplication in ~80% of SDLC tests).

A seed change can break every test that references it. That's the point -- it forces tests to be explicit about what data they need instead of each one drifting into slightly different setup. `extra-sql` / `seed-sql` is the escape hatch for test-specific data.

### Runner maintenance

Two runners, ~200 lines each. The query runner depends on `QueryResult`, `SecurityContext`, and the redaction API. The indexer runner depends on `IndexerTestExt`, `HandlerRegistry`, and `TestEnvelopeFactory`. Refactors to either pipeline will require runner updates -- each runner is coupled to its crate's internal API. A stable public test API would be premature; both pipelines are still changing.

Adding a command is a match arm. Removing one that `.kdl` files reference fails `kdl_fixtures_parse` immediately.

### Test reliability

Same determinism as the existing Rust tests -- same seed data, same ClickHouse testcontainer, same assertions. The format doesn't introduce flakiness. But glob-based discovery means dropping a `.kdl` file into the fixtures directory automatically runs it in CI. No registration step, which is convenient until someone accidentally commits a half-finished file.

## References

- [CockroachDB `datadriven`](https://github.com/cockroachdb/datadriven) -- the command-based test pattern this design follows
- [KDL Document Language](https://kdl.dev/) -- file format specification
- [KDL Rust crate](https://crates.io/crates/kdl) -- parser used by the test runner
- [`sqllogictest` original (SQLite)](https://sqlite.org/sqllogictest) -- surveyed, not adopted
- [`sqllogictest-rs` (Rust crate)](https://github.com/risinglightdb/sqllogictest-rs) -- surveyed, not adopted
- [ADR 001: gRPC Communication Protocol](001_grpc_communication.md)
- [ADR 002: Rust Core Runtime](002_rust_core_runtime.md)
