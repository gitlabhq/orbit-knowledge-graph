# integration-tests

Integration tests for the compiler and orbit-server query/redaction pipeline. This crate
exists to break the dependency cycle: it depends on `orbit-server`, `compiler`, and
`integration-testkit` without any of those needing to depend on each other.

## Structure

```plaintext
tests/
  local.rs              # local binary (no Docker)
  server_containers.rs  # server container tests
  indexer_containers.rs # indexer container tests
  cli.rs                # cli binary (the orbit binary as a subprocess)
  billing_boundary.rs   # billing_boundary binary (orbit-billing dependents)
  common/               # shared helpers
  canary/               # TestContext canary
  compiler/             # compiler, dialect, named query, and plan-shape tests
  indexer/              # indexer tests; indexer scenarios in indexer/scenarios/
  server/               # server tests; query scenarios in server/data_correctness/scenarios/
  fixtures/             # CLI fixtures
```

| Binary | Tests | CI job |
|---|---|---|
| `local` | compiler and querying pipeline, no Docker | `compiler-integration-test` |
| `server_containers` | Server tests with ClickHouse | `integration-test`, `integration-test-data-correctness`, `corpus-smoke-test` |
| `indexer_containers` | Indexer tests with ClickHouse and NATS | `integration-test`, `integration-test-data-correctness` |
| `cli` | the `orbit` CLI | `cli-integration-test` |
| `billing_boundary` | only `orbit-server` depends on `orbit-billing` | `billing-boundary-check` |

The three container-test lanes run each container test one time.
`integration-test-lane-coverage-check` fails if a test is in no lane or in two lanes.

New correctness tests are YAML suites. See
[Testing principles](../../docs/pages/engineering/testing/README.md#principles).

## Running

On macOS, start the Colima profile that the mise tasks use:

```shell
colima start gkg --memory 12
mise test:local
mise test:integration
colima stop gkg
```

For the other `mise test:integration:*` tasks, see
[Container tests](../../docs/pages/engineering/testing/infrastructure.md).

To run specific suites or tests directly:

```shell
# Local tests (no Docker needed)
cargo nextest run --test local                                           # all local tests
cargo nextest run --test local -E 'test(compiler::)'                     # compiler only
cargo nextest run --test local -E 'test(querying_pipeline::)'            # querying pipeline only

# Container tests
export DOCKER_HOST="unix://$HOME/.colima/gkg/docker.sock"
cargo nextest run --test server_containers --test indexer_containers     # all container tests
cargo nextest run --test server_containers -E 'test(data_correctness)'   # one suite
cargo nextest run --test server_containers -E 'test(infra_canary)'       # canary

# Run YAML-driven scenarios, optionally filtered by category
cargo nextest run --test server_containers -E 'test(data_correctness_scenarios)'
SCENARIO_FILTER=search cargo nextest run --test server_containers -E 'test(data_correctness_scenarios)'
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

### Data correctness (preferred)

Add a `.yaml` file under `data_correctness/scenarios/<category>/`. Each file
is a `QueryScenario` that declares seed data, a query, and expected results.
See the [integration-testkit README](../integration-testkit/README.md) for the
`QueryScenario` format reference.

### Other server tests

1. Write an `async fn my_test(ctx: &TestContext)` in the appropriate module.
2. If it only reads seeded data, add it to the `run_subtests_shared!` block.
3. If it writes extra data, add it to the `run_subtests!` block and call the seed
   function at the top of the test body.
4. If you need a new server test module, add `pub mod foo;` to
   `server_containers.rs` and create `server/foo.rs`.

### Rust-only data correctness tests

Three tests remain in [retained.rs](tests/server/data_correctness/retained.rs).
They check internal execution contracts or comparisons that YAML does not express:

| Test | Reason |
|------|--------|
| `long_node_text_is_excerpted_only_on_wide_pages` | Checks exact narrow-page text and requires the wide-page excerpt to be shorter than the original text. |
| `path_finding_code_filtered_endpoints_stay_on_same_traversal_path` | Inspects raw path rows before redaction and hydration, and excludes a cross-project decoy. |
| `semantic_aggregate_and_nested_cte_codegen_execute` | Executes a constructed compiler AST to check conditional counts, nested CTEs, and UNION arms. |

The first two share the seeded `data_correctness` test. The codegen test uses
its own table. Both entrypoints and the YAML scenarios run with
`mise test:integration:data`.

Add new query correctness cases as YAML scenarios. Extend the harness when a
response assertion is missing.

### Compiler tests

1. Add to `compiler/mod.rs` and create `compiler/foo.rs`.
2. For a new test binary, add a `tests/foo.rs` file -- Cargo auto-discovers it.

## Auto-discovery rules

Cargo treats every `.rs` file at the `tests/` root as a separate test binary.
Subdirectories (`common/`, `compiler/`, `server/`, etc.) are ignored. This is
why shared helpers live in `tests/common/mod.rs` instead of `tests/common.rs`.

When an entrypoint needs to include modules whose directory name doesn't match
the module name (e.g. `querying_pipeline` lives under `server/`), use a
`#[path]` attribute:

```rust
// local.rs — compiler/ is at the tests/ root, so standard resolution works:
mod compiler;

// querying_pipeline/ lives under server/, so we need an explicit path:
#[path = "server/querying_pipeline/mod.rs"]
mod querying_pipeline;
```

Avoid naming a `.rs` entrypoint the same as an existing subdirectory
(e.g. don't create `tests/compiler.rs` when `tests/compiler/` exists) --
Rust forbids both `foo.rs` and `foo/mod.rs` for the same module.
