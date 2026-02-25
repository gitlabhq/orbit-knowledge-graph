# E2E testing

End-to-end testing for the GitLab Knowledge Graph. Deploys a full GitLab instance with siphon, ClickHouse, and the GKG stack into a local Kubernetes cluster (Colima), creates test data, indexes it, and runs redaction/permission tests against the live graph.

## Prerequisites

- [Colima](https://github.com/abiosoft/colima) (`brew install colima`)
- Docker CLI (`brew install docker`)
- Helm 3 (`brew install helm`)
- A GitLab Rails checkout (pointed to by `GITLAB_SRC`)

Hardware: 12 GB RAM and 4 CPUs are allocated to the Colima VM by default (configurable in `config/e2e.yaml`).

## Quick start

```shell
# Deploy cluster + GitLab + configure test data (~15-20 min first run)
GITLAB_SRC=~/gdk/gitlab cargo xtask e2e setup

# Add GKG stack (ClickHouse, siphon, indexer, webserver, dispatch-indexing)
GITLAB_SRC=~/gdk/gitlab cargo xtask e2e setup --gkg-only

# Run tests (~10s)
cargo xtask e2e test

# Tear down everything
cargo xtask e2e teardown
```

Mise aliases: `mise e2e:setup`, `mise e2e:test`, `mise e2e:teardown`, `mise e2e:rebuild:gkg`, `mise e2e:rebuild:rails`, `mise e2e:rebuild:all`.

## Commands

### `setup`

Provisions the full environment. By default runs phases 1-2 (CNG deploy + CNG setup). Use flags to control which phases run.

| Flag | Phases | What it does |
|------|--------|--------------|
| *(none)* | 1-2 | Colima VM, Traefik, GitLab Helm chart, PostgreSQL credentials, Rails migrations, test data |
| `--gkg` | 1-3 | All of the above plus ClickHouse, GKG chart, siphon wait, dispatch-indexing |
| `--gkg-only` | 3 | GKG stack only (assumes phases 1-2 are done) |
| `--cng` | 1 | CNG deploy only (cluster + GitLab) |
| `--cng-setup` | 2 | CNG setup only (PostgreSQL creds, migrations, test data) |
| `--skip-build` | - | Reuse previously built CNG images |

### `test`

Copies test scripts to the toolbox pod and runs the redaction test suite. Outputs JSON results with pass/fail per assertion.

### `rebuild`

Fast iteration loop. Requires at least one flag.

| Flag | Time | What it does |
|------|------|--------------|
| `--gkg` | ~2-3 min | Rebuilds GKG server image, rollout restarts all GKG deployments |
| `--rails` | ~5-8 min | Rebuilds CNG images from `GITLAB_SRC`, runs `helm upgrade` on GitLab |

Flags can be combined. Migrations and test data persist across rebuilds.

### `teardown`

| Flag | What it keeps |
|------|---------------|
| *(none)* | Nothing. Full teardown including Colima VM. |
| `--keep-colima` | Colima VM (saves ~30s on next setup) |
| `--gkg-only` | GitLab, Traefik, and Colima |

## Pipeline walkthrough

The setup pipeline runs 25 numbered steps across three phases.

### Phase 1: CNG deploy (steps 1-6)

1. Start Colima VM with k3s
2. Pre-pull workhorse image from the CNG registry
3. Build custom CNG images (webservice, sidekiq, toolbox) by overlaying Rails code from `GITLAB_SRC` onto upstream CNG base images
4. Deploy Traefik ingress controller
5. Deploy GitLab via Helm chart (pinned version, with `--set` overrides for image repos/tags and PostgreSQL config)
6. Wait for all GitLab pods to be ready

### Phase 2: CNG setup (steps 8-13)

1. Bridge PostgreSQL credentials from the `gitlab` namespace to `default` *(step 8)*
2. Grant `REPLICATION` privilege to the `gitlab` PostgreSQL user (required for siphon) *(step 9)*
3. Run Rails `db:migrate` *(step 10)*
4. Enable the `:knowledge_graph` feature flag *(step 11)*
5. Copy test scripts to the toolbox pod *(step 12)*
6. Create test data (users, groups, projects, MRs, work items, notes) via `create_test_data.rb` *(step 13)*

### Phase 3: GKG stack (steps 15-25)

1. Deploy standalone ClickHouse (StatefulSet, rendered from `clickhouse.yaml.tmpl`) *(step 15)*
2. Run `gitlab:clickhouse:migrate` for datalake tables, materialized views, and dictionaries *(step 16)*
3. Apply GKG graph schema (`graph.sql`) *(step 17)*
4. Drop stale siphon replication slot and publication *(step 18)*
5. Verify `knowledge_graph_enabled_namespaces` rows in PostgreSQL *(step 19)*
6. Build GKG server image, create K8s secrets, deploy GKG Helm chart (with `--set` overrides) *(step 20)*
7. Wait for siphon data to flow (poll datalake tables) *(step 21)*
8. Run dispatch-indexing job, wait for indexer to process *(step 22)*
9. `OPTIMIZE TABLE FINAL` on all graph tables *(step 23)*
10. Verify graph tables have data (row counts) *(step 24)*
11. Run redaction tests (`redaction_test.rb`) *(step 25)*

## Configuration

All configurable values live in [`config/e2e.yaml`](../../config/e2e.yaml):

| Section | What it controls |
|---------|-----------------|
| `colima` | VM resources (memory, CPUs, disk, vm_type, k8s version) |
| `namespaces` | Kubernetes namespace names |
| `cng` | Base image tag, registry, local image prefix, components to build, staging dirs |
| `helm` | Chart versions, release names, repo URLs, timeouts |
| `postgres` | Secret names, database, user, service name |
| `clickhouse` | Image, service name, ports, database names, credentials |
| `siphon` | Publication/slot names, poll timeout |
| `gkg` | Server image, dispatch job name, gRPC endpoint |
| `pod_readiness` | Label selectors and timeouts for GitLab and GKG pod readiness |
| `timeouts` | ClickHouse pod, GKG chart, dispatch job, indexer poll |

Structural constants (file paths, table lists, concurrency limits) that define the shape of the harness itself are in `crates/xtask/src/e2e/constants.rs`.

## Drift prevention

Helm values files (`gitlab-values.yaml`, `traefik-values.yaml`, `helm-values.yaml`) contain only non-default overrides. Values that must stay in sync with `config/e2e.yaml` are applied at deploy time via `--set` flags rather than duplicated in YAML. The ClickHouse manifest is fully templated (`clickhouse.yaml.tmpl`) and rendered from config values.

## Test architecture

Tests run as Ruby scripts inside the GitLab toolbox pod, which has access to the Rails console and the GKG gRPC endpoint.

- **`create_test_data.rb`** -- Creates a test matrix: 5 users with varying group memberships, 4 projects across public and private groups, MRs, work items, and notes. Writes a `manifest.json` describing what was created.
- **`redaction_test.rb`** -- Queries the GKG graph as each test user and asserts that redaction is correct: users only see entities in namespaces they have access to. Outputs JSON results.
- **`test_helper.rb`** -- Shared utilities for gRPC calls, JWT signing, and assertion helpers.

## Module structure

See [`crates/xtask/src/e2e/README.md`](../../crates/xtask/src/e2e/README.md) for the source code layout.
