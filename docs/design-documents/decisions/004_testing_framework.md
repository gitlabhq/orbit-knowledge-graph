---
title: "GKG ADR 004: File-based testing framework for the query pipeline"
creation-date: "2026-03-10"
authors: [ "@michaelusa" ]
toc_hide: true
---

## Status

Proposed

## Date

2026-03-10

## Context

The integration test suite (`redaction_integration.rs`, `hydration_integration.rs`) has grown to ~2,500 lines of Rust across 60+ test functions. Every test follows the same six-stage pipeline:

1. Insert seed data into ClickHouse
2. Compile a JSON query via the query engine
3. Execute the base query
4. Configure mock redaction (allow/deny entity IDs)
5. Run redaction, optionally hydration
6. Assert on results (authorized IDs, tuples, path nodes, edge kinds)

Each test is a standalone async Rust function with 30-60 lines of boilerplate. The functions are structurally identical -- they differ only in the query JSON, the allow/deny rules, and the assertions. Adding a new test requires writing Rust, recompiling, and understanding internal API types (`QueryResult`, `RecordBatch`, `SecurityContext`). The signal-to-noise ratio is poor: the interesting part of each test is ~5 lines of configuration buried in ~40 lines of setup and teardown.

The problem is separating test specifications from test infrastructure so that adding a test means writing a data file, not a Rust function.

## Decision

Use KDL (KDL Document Language) with a custom command runner to define query pipeline integration tests as `.kdl` files. The runner interprets commands sequentially, wiring into the existing `TestContext` / `run_subtests!` infrastructure. A single Rust `#[test]` function discovers and executes all `.kdl` files in a directory.

### Command set

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

### Full pipeline example

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

// Surviving path should be User 1 → Group 100 → Project 1000
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

### Compile error example

```kdl
test "limit_and_range_mutually_exclusive"

compile expect="error" """
  {"query_type": "search", "node": {"id": "u", "entity": "User"}, "limit": 10, "range": {"start": 0, "end": 5}}
"""
```

### SQL assertion example

```kdl
test "column_selection_mandatory_columns" seed="default"

compile """
  {
      "query_type": "search",
      "node": {"id": "u", "entity": "User", "columns": ["username"]},
      "limit": 10
  }
"""

sql-contains "_gkg_u_id"
sql-contains "_gkg_u_type"
sql-contains "u_username"
sql-not-contains "JOIN"

execute expect=5
allow "user" 1 2 3
deny "user" 4 5
redact expect=2
authorized-ids "u" 1 2 3
```

### Tests that stay as Rust

Some tests are genuinely procedural and do not benefit from a declarative format:

- **Hand-constructed Arrow batches:** `fail_closed_null_id_denies_row` and `fail_closed_null_type_denies_row` build `RecordBatch` objects directly without the query engine. No SQL, no pipeline.
- **Complex error pattern matching:** Tests that call `compile().unwrap_err()` and match on specific error enum variants. KDL handles `expect="error"` for simple cases, but deep pattern matching on error enums is better in Rust.
- **Heavy custom setup:** Tests like `cross_entity_id_collision_redaction` with elaborate seed data beyond `extra-sql`. If the setup logic has conditionals or loops, Rust is clearer.

Estimated split: 70-80% of tests convert to KDL, 20-30% stay as Rust.

## Why a datadriven command model

The CockroachDB `datadriven` library established the pattern: sequential command blocks in a file, each producing output compared against expected values. You define what each command means; the framework handles file parsing and output comparison. CockroachDB uses this for SQL optimizer testing (`exec-sql`, `trace-sql`, `normalize`, `opt`), Pebble uses it for storage engine testing (`define`, `ingest`, `compact`, `iter`, `get`).

The key property is that the framework is command-agnostic. Unlike `sqllogictest` -- which assumes a single-step pipeline (SQL in, rows out, compare) -- a command-based model supports intermediate stages. Our pipeline has stages that need configuration between steps: mock auth rules must be set between execution and redaction, assertions on path node properties happen after hydration, state can be reset mid-test. There is no way to express `allow "user" 1 2 3` in sqllogictest without encoding it as a fake SQL `SET` statement inside the `DB::run()` implementation, which fights the framework rather than using it.

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

The `.kdl` test files live in `crates/query-engine/tests/fixtures/` alongside the existing Rust integration tests. A single Rust `#[tokio::test]` function discovers all `.kdl` files via `glob`, parses each one, and runs the command sequence against a real ClickHouse testcontainer:

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

This runs under `mise test:integration` and `cargo nextest` the same way existing integration tests do. No new CI job is needed -- the existing `integration-test` pipeline stage picks up the new test function automatically.

A separate validation-only test parses all `.kdl` files without executing them, catching syntax errors before the integration test stage:

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

During the transition period, both the original Rust tests and the new KDL tests run in CI. Once a Rust test has a KDL equivalent that passes, the Rust version is deleted. The remaining Rust tests (Arrow batch construction, error pattern matching) stay permanently.

Seed data lives in `crates/query-engine/tests/seeds/*.sql`. The existing `setup_test_data` and `setup_indirect_auth_data` functions are extracted into `default.sql` and `with_code_entities.sql` respectively. The `seed=` property on the `test` command determines which seed file to load.

## Known gaps

The command set in this ADR covers the current test suite. As new pipeline features are added (aggregation queries, Cypher compilation, response formatting), new commands will be needed. Adding a command requires a match arm in the runner and a row in the command table above -- low cost, but the ADR cannot enumerate future commands.

The `expect=` properties use exact values (`redact expect=2`). There is no mechanism for approximate matching, regex assertions, or ordered subset assertions. If a test needs "at least 3 rows" rather than "exactly 3 rows", it must be a Rust test. This could be addressed later with comparison operators (`expect>=3`) if the need arises.

Error message assertions are limited. `compile expect="error"` asserts that compilation fails, but does not match on the error message or error type. Tests that need to distinguish between "invalid entity type" and "mutually exclusive parameters" must stay in Rust. A future `expect="error: substring"` extension is straightforward.

`--rewrite` support (automatically updating expected values in `.kdl` files when test output changes) is not part of the initial implementation. The `kdl` crate preserves node structure but not whitespace formatting, so rewriting requires re-serializing the document. This is a medium-difficulty task, not a blocker.

There is no parallelism within a single `.kdl` file -- commands execute sequentially, sharing a single `TestContext`. Parallelism comes from nextest running multiple test files concurrently (each in its own testcontainer). A file with 20 command blocks runs ~20x slower than a file with 1, which incentivizes keeping test files focused rather than combining unrelated test cases.

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| `sqllogictest` format | Assumes single-step SQL pipeline. No mechanism for mid-pipeline configuration or multi-stage assertions. The `sqllogictest-rs` crate's `AsyncDB` trait has a single `run(sql)` method -- there is no hook for "now configure auth rules" between steps. |
| `pg_regress` two-file model | String diffing of full output, no structured assertions. Two-file coupling (input + expected) is maintenance overhead. Platform-dependent output formatting causes false failures. |
| Raw text (datadriven-style) | Viable fallback. Blank-line sensitivity and no IDE support are the main downsides. If KDL adoption is a concern, this is the second choice -- the runner's command dispatch logic is identical, only the parser changes. |
| Keep everything as Rust | The 30-60 lines of boilerplate per test, recompilation on every change, and the Rust knowledge requirement for test authors are real costs that scale linearly with test count. At 60+ tests, the maintenance burden is already significant; at 200+ tests (as more entity types are indexed), it would be worse. |

## Consequences

### Dev-dependency budget

Adding the `kdl` crate as a dev-dependency. The crate is well-maintained (v6.x, by the KDL spec authors) but younger than `toml` or `serde_yaml`. It compiles in ~2 seconds and has no transitive dependencies. It is a test-only dependency -- no production binary size impact, no `cargo audit` / `cargo deny` exposure in the release artifact.

### Test authoring workflow

Adding a new integration test changes from "write 40 lines of async Rust, recompile (~30s incremental), run" to "write 10 lines of KDL, run". The recompilation step is eliminated for test-only changes. This lowers the barrier for contributors who understand the query pipeline but are not fluent in Rust async patterns.

The tradeoff is that KDL is a format most engineers have not seen. The learning curve is minimal -- KDL reads like CLI commands -- but it is nonzero. The command table and examples in this ADR serve as the reference. The `kdl_fixtures_parse` unit test catches syntax errors immediately.

### Shared seed data

Seed SQL files (`tests/seeds/*.sql`) become shared fixtures. Changes to seed data affect all tests that reference that seed. This is intentional -- it forces tests to be explicit about what data they need and prevents seed drift where each test has slightly different setup. The downside is that a seed change can break multiple tests at once. `extra-sql` provides an escape hatch for test-specific data without modifying the shared seed.

### Runner maintenance

The KDL runner (~200 lines) is new infrastructure that depends on `TestContext` internals. Query engine refactors that change `QueryResult`, `SecurityContext`, or the redaction API will require runner updates. This couples the runner to the query engine's internal API surface. The alternative -- a stable public test API -- would be premature abstraction; the pipeline is still evolving.

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
