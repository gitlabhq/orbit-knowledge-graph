# Testing commands

This page lists where each test layer lives and the command that runs it. The principles and the layers are in [Testing](../architecture/testing.md).

| Layer | Where it lives | Local command |
| --- | --- | --- |
| Build scripts | `build.rs` in each crate | `mise build` |
| Unit tests | `#[cfg(test)]` modules next to the code | `mise test:fast` |
| Compiler integration | `crates/integration-tests/tests/compiler/` | `mise test:local` |
| Code-graph suites | `crates/integration-tests-codegraph/` | `mise test:integration:codegraph` |
| CLI integration | `crates/integration-tests/tests/cli.rs` | `mise test:cli` |
| Container tests | `crates/integration-tests/tests/` | `mise test:integration` |
| E2E | `e2e/` | `e2e/scripts/setup.sh`, then `e2e/scripts/test.sh` |
| Performance | `code-indexing-benchmark.yaml`, `crates/query-engine/profiler/` | `mise query:profile` |
| Fuzzing | `crates/fuzz/` | `mise test:fuzz`, `mise fuzz:<target>` |

Other commands:

- `mise test:plan-shape` runs the plan-shape fixtures.
- `mise bench:run` measures code indexing on real repositories, and `mise bench:smoke` runs one indexing pass for each repository.
- `mise query:diff` compares two query profiles.

## Container tests

On macOS, start the Colima `gkg` profile before you run container tests:

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
