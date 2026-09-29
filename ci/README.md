# CI tools

Python is the default for repository checks and CI orchestration. Keep these
scripts in `ci/`, prose and narration checks in `ci/linting/`, and pipeline
automation in `ci/automation/`. Version and migration checks share `ci/skip_check.py`.

## Setup

Run commands from the repository root. Install tools with `mise install`, then
install the locked Python dependencies:

```shell
mise exec -- uv sync --frozen --project ci
```

`ci/.python-version` pins Python. `ci/pyproject.toml` declares dependencies and
`ci/uv.lock` locks their versions. After a dependency change, run
`mise exec -- uv lock --project ci` and commit both files.

Mise tasks use `uv run --frozen --project ci python`. Git hooks add `mise exec --`
so they use the same tools outside a mise shell.

## Commands

The archive parity tests need GNU tar (`tar` on Linux, `gtar` on macOS) and `gzip`.
Install GNU tar on macOS with `brew install gnu-tar`.

`repository-checks` runs schema, repository, prose, and script tests in one job.
It also checks version bumps on merge requests. `advisory-checks` runs narration
and MR-description checks. Both groups report all failures before exiting.
`rust-hygiene-check` groups dependency, formatting, and newline checks.
`generated-files-check` also checks the migration ledger on merge requests.

| Task | Checks |
|---|---|
| `mise ci:schemas` | Ontology, named queries, versions, indexer scenarios, migration ledger, and setup YAML |
| `mise ci:repository` | Agent guide sync and Rust toolchain metadata |
| `mise ci:generated` | Generated schemas, DDL, metrics, dashboards, and query docs |
| `mise ci:versions --base-ref REF` | Pinned, prompt, and skill version bumps against a local Git ref |
| `mise ci:test` | All Python tests in `ci/tests/`, using pytest settings in `ci/pyproject.toml` |

For individual checks, use this prefix:

```shell
mise exec -- uv run --frozen --project ci python ci/validate-schemas.py ontology
```

The scripts accept these arguments:

| Script | Arguments |
|---|---|
| `ci/validate-schemas.py` | `all`, `ontology`, `named-queries`, `versions`, `indexer-scenarios`, `migration-ledger`, or `setup` |
| `ci/check-repository.py` | `all` or `toolchain`; `toolchain --write` regenerates `rust-toolchain.toml` |
| `ci/check_generated.py` | `all` or `ddl` |
| `ci/check_migration_ledger.py` | `--base REF` |
| `ci/check_vendored.py` | A dependency name or `all`; dispatches through its `CHECKS` registry and reports every failure |
| `ci/check-version-bumps.py` | `all`, `pinned`, `prompts`, or `skills`; `--base-ref REF` selects the comparison base; `skills --staged` checks the staged snapshot |
| `ci/dashboards.py` | `--check` compares rendered dashboards; omit it to regenerate |
| `ci/integration_lanes.py` | `--check` validates the container test partition |

Run `mise lint:prose` with file paths. Its tests live in `ci/tests/prose_test.py`.
See the [linting guide](linting/README.md) for `check_narration.py`,
`check_mr_description.py`, and `prose_lint.py`.

## Pipeline automation

`ci/automation/open_e2e_bump_mr.py` updates e2e pins.
`ci/automation/sync_doc_principles.py` copies upstream documentation principles.
Both use `ci/automation/publish.py` to commit changes, push the automation branch,
and create or refresh an MR. `DRY_RUN=true` updates local files and shows the diff
without publishing. Run `mise docs:principles:sync` to rehearse the principles sync.

## Rust contracts

Rust owns config schema generation, ontology DDL, migration fingerprints and
archives, the metrics catalog, and query docs. The Python checks call those
`cargo xtask` commands so they use the application's types and generators.
Generated-file and migration-ledger checks therefore need the Rust toolchain.
Dashboard rendering needs Jsonnet. Integration lane checks use `cargo nextest`.

After changing the Rust pin in `mise.toml`, run `mise toolchain:generate`.
The repository check detects drift in `rust-toolchain.toml`. Generation is an
explicit task because uv might not be available during Rust installation.
