# xtask

Development task runner for the GitLab Knowledge Graph. 

## Features

- Automates E2E environment lifecycle: provisioning a local Kubernetes cluster, deploying GitLab and GKG, running tests, and tearing it all down.

## Quick start

```shell
# Full environment (Colima + GitLab + CNG setup)
GITLAB_SRC=~/path/to/gitlab cargo xtask e2e setup

# Add the GKG stack (ClickHouse, siphon, indexer, webserver)
GITLAB_SRC=~/path/to/gitlab cargo xtask e2e setup --gkg

# Run redaction tests
cargo xtask e2e test

# Rebuild after code changes
cargo xtask e2e rebuild --gkg      # GKG server image (~2-3min)
cargo xtask e2e rebuild --rails    # CNG images from GITLAB_SRC (~5-8min)

# Tear down
cargo xtask e2e teardown                # everything
cargo xtask e2e teardown --keep-colima  # keep VM, remove workloads
cargo xtask e2e teardown --gkg-only     # keep GitLab, remove GKG
```

Mise aliases are available: `mise e2e:setup`, `mise e2e:test`, `mise e2e:teardown`, `mise e2e:rebuild:gkg`, `mise e2e:rebuild:rails`, `mise e2e:rebuild:all`.

## Prerequisites

- [Colima](https://github.com/abiosoft/colima) with Kubernetes support
- Docker CLI
- Helm 3
- `GITLAB_SRC` environment variable pointing to a GitLab Rails checkout (required for `setup` and `rebuild --rails`)

## Configuration

All configurable values live in [`config/e2e.yaml`](../../config/e2e.yaml): Colima resources, Helm chart versions, image tags, namespaces, timeouts, ClickHouse settings, and pod readiness checks.

Structural constants (file paths, table lists, concurrency limits) are in `src/e2e/constants.rs`.

## Module structure

See the [E2E module README](src/e2e/README.md) for architecture details.
