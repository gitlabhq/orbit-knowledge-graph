# Testing commands

This page lists where each test layer lives and the command that runs it. The principles and the layers are in [Testing](../testing/README.md).

| Layer | Where it lives | Local command |
| --- | --- | --- |
| Build scripts | `build.rs` in each crate | `mise build` |
| Unit tests | `#[cfg(test)]` modules next to the code | `mise test:fast` |
| Compiler integration | `crates/integration-tests/tests/compiler/` | `mise test:local` |
| Code-graph suites | `crates/integration-tests-codegraph/` | `mise test:integration:codegraph` |
| CLI integration | `crates/integration-tests/tests/cli.rs` | `mise test:cli` |
| Container tests | `crates/integration-tests/tests/` | `mise test:integration` |
| E2E | `e2e/` | `e2e/scripts/setup.sh`, then `e2e/scripts/test.sh` |
| Load test | `scripts/ci/orbit-perf/`, `crates/integration-tests/tests/server/performance/scenarios/` | `bash scripts/ci/orbit-perf.sh` |
| Performance tools | `code-indexing-benchmark.yaml`, `crates/query-engine/profiler/` | `mise query:profile` |
| Fuzzing | `crates/fuzz/` | `mise test:fuzz`, `mise fuzz:<target>` |

Other commands:

- `mise test:plan-shape` runs the plan-shape fixtures.
- `mise bench:run` measures code indexing on real repositories, and `mise bench:smoke` runs one indexing pass for each repository.
- `mise query:diff` compares two query profiles.
- `bash scripts/ci/orbit-perf.sh` runs the [load test](../testing/performance/load-test.md) on your machine with the small synthetic profile. It deploys the Orbit image tag from the Caproni values. Set `GKG_IMAGE` and `GKG_IMAGE_TAG` to test another published image, and `SYNTH_CONFIG` or `ROUNDS` to change the size or the length of the run.
- `mise perf:gtime` and `mise perf:rss` record the time and memory of `orbit workspace index` for before and after comparisons.
- `mise test:fast` skips the integration-test, code-graph, fuzz, and profiler crates, which have their own jobs.

## Container tests

Run container tests through mise. The mise runner starts one ClickHouse container and passes its address to the tests. On macOS, start the Colima `gkg` profile first:

```shell
colima start gkg --memory 12
mise test:integration
```

| Task | What it runs |
| --- | --- |
| `mise test:integration` | All container tests |
| `mise test:integration:data` | Query scenarios and data correctness |
| `mise test:integration:server` | Data correctness, redaction, and graph formatting |
| `mise test:integration:indexer` | All indexer tests |
| `mise test:integration:indexer:sdlc:scenario <filter>` | SDLC indexer scenarios whose path contains the filter |
| `mise test:integration:corpus` | The query corpus and the executable docs |
| `mise test:integration:overlay <name>` | Data correctness with an ontology overlay |

## Harness references

| Harness | Reference |
| --- | --- |
| Test binaries, CI jobs, lanes, and Rust-only data tests | [`integration-tests/README.md`](../../../../crates/integration-tests/README.md) |
| Plan-shape fixtures | [`plan_shape/README.md`](../../../../crates/integration-tests/tests/compiler/plan_shape/README.md) |
| Code-graph suites | [`integration-tests-codegraph/README.md`](../../../../crates/integration-tests-codegraph/README.md) |
| Container harness and scenario format | [`integration-testkit/README.md`](../../../../crates/integration-testkit/README.md) |
| Indexer scenario schema | [`indexer_scenario.schema.json`](../../../../config/schemas/indexer_scenario.schema.json) |
| Query corpus | [`fixtures/queries/corpus`](../../../../fixtures/queries/corpus) |
| End-to-end harness | [E2E testing harness](e2e-testing.md) |
| Code-indexing benchmarks | [`code-indexing-benchmark.yaml`](../../../../code-indexing-benchmark.yaml) |
| Reference architecture bench | [`bench/README.md`](../../../../bench/README.md) |
| Fuzzing, corpus promotion, and crash triage | [`fuzz/README.md`](../../../../crates/fuzz/README.md) |
| SOX billing boundary | [SOX billing boundary](sox-billing-boundary.md) |
