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

```shell
# Port-forward GitLab UI + GKG webserver
cargo xtask e2e serve
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
| `--skip-webpack` | - | Skip webpack asset compilation during CNG image build (faster Ruby-only rebuilds) |

### `test`

Copies test scripts to the toolbox pod and runs the redaction test suite. Outputs JSON results with pass/fail per assertion.

### `rebuild`

Fast iteration loop. Requires at least one flag.

| Flag | Time | What it does |
|------|------|--------------|
| `--gkg` | ~2-3 min | Rebuilds GKG server image, rollout restarts all GKG deployments |
| `--rails` | ~5-8 min | Rebuilds CNG images from `GITLAB_SRC`, runs `helm upgrade` on GitLab |
| `--skip-webpack` | - | Skip webpack asset compilation during Rails rebuild |

Flags can be combined. Migrations and test data persist across rebuilds.

### `serve`

Port-forwards the GitLab UI and GKG webserver to localhost. Runs in the foreground — Ctrl+C to stop.

| Service | Local URL |
|---------|-----------|
| GitLab UI | `http://localhost:8929` |
| GKG webserver | `http://localhost:8930` |

Login with `root` / password from `config/e2e.yaml` (`gitlab_ui.root_password`).

### `codegen`

Generates Ruby test scripts (`create_test_data.rb`, `redaction_test.rb`) from `e2e/tests/scenarios.yaml`. The generated files are committed to git.

```shell
cargo xtask e2e codegen          # regenerate .rb files
cargo xtask e2e codegen --check  # verify committed files match (CI)
```

### `teardown`

| Flag | What it keeps |
|------|---------------|
| *(none)* | Nothing. Full teardown including Colima VM. |
| `--keep-colima` | Colima VM (saves ~30s on next setup) |
| `--gkg-only` | GitLab, Traefik, and Colima |

## Pipeline walkthrough

The setup pipeline runs numbered steps across three phases.

### Phase 1: CNG deploy (steps 1-6)

1. Start Colima VM with k3s
2. Pre-pull workhorse image from the CNG registry
3. Build custom CNG images (webservice, sidekiq, toolbox) by overlaying Rails code from `GITLAB_SRC` onto upstream CNG base images, including webpack asset compilation (use `--skip-webpack` to skip)
4. Deploy Traefik ingress controller
5. Deploy GitLab via Helm chart (pinned version, with `--set` overrides for image repos/tags and PostgreSQL config)
6. Wait for all GitLab pods to be ready

### Phase 2: CNG setup (steps 7-14)

1. Bridge PostgreSQL credentials from the `gitlab` namespace to `default` *(step 8)*
2. Grant `REPLICATION` privilege to the `gitlab` PostgreSQL user (required for siphon) *(step 9)*
3. Run Rails `db:migrate` *(step 10)*
4. Enable the `:knowledge_graph` feature flag *(step 11)*
5. Patch webservice ConfigMap to enable Knowledge Graph in `gitlab.yml.erb`, restart webservice *(step 7)*
6. Copy test scripts to the toolbox pod *(step 12)*
7. Create test data (users, groups, projects, MRs, work items, notes) via `create_test_data.rb` *(step 13)*
8. Set root user password for UI access *(step 14)*

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
| `gitlab_ui` | Port-forward services/ports, root password for `serve` command |
| `pod_readiness` | Label selectors and timeouts for GitLab and GKG pod readiness |
| `timeouts` | ClickHouse pod, GKG chart, dispatch job, indexer poll |

Structural constants (file paths, table lists, concurrency limits) that define the shape of the harness itself are in `crates/xtask/src/e2e/constants.rs`.

## Drift prevention

Helm values files (`gitlab-values.yaml`, `traefik-values.yaml`, `helm-values.yaml`) contain only non-default overrides. Values that must stay in sync with `config/e2e.yaml` are applied at deploy time via `--set` flags rather than duplicated in YAML. The ClickHouse manifest is fully templated (`clickhouse.yaml.tmpl`) and rendered from config values.

## Test scenarios DSL

Test data definitions and assertions are declared in [`e2e/tests/scenarios.yaml`](../../e2e/tests/scenarios.yaml). The Ruby scripts (`create_test_data.rb`, `redaction_test.rb`) are auto-generated from this YAML. Do not edit them directly.

### YAML schema

| Section | Purpose |
|---------|---------|
| `users` | Test users. Key = logical name, optional `username` override. |
| `groups` | Group hierarchy. Recursive `children` for subgroups. `visibility` defaults to `public`. |
| `projects` | Projects with parent group reference and entity counts (milestones, labels, work items, MRs, notes). |
| `memberships` | Group memberships per user (access level). Users not listed have no memberships. |
| `assertions` | Test sections with inline GKG JSON queries and expected result counts. |

### Query format

Queries use the GKG JSON query DSL inline as strings. Single-line:

```yaml
- name: "root: all projects"
  query: '{"query_type":"search","node":{"id":"p","entity":"Project","columns":["name"]},"limit":100}'
  expect: { eq: "$total_projects" }
```

Multiline queries use YAML block scalars:

```yaml
- name: "root: MRs in smoke project"
  query: |
    {
      "query_type": "traversal",
      "nodes": [
        { "id": "p", "entity": "Project", "columns": ["name"], "node_ids": ["$proj.smoke"] },
        { "id": "mr", "entity": "MergeRequest", "columns": ["iid"] }
      ],
      "relationships": [{ "type": "IN_PROJECT", "from": "mr", "to": "p" }],
      "limit": 50
    }
  expect: { gte: 1 }
```

### Variables

`$variable` references in queries and expectations are resolved at runtime from the manifest written by `create_test_data.rb`:

| Variable pattern | Example | Resolves to |
|-----------------|---------|-------------|
| `$total_<entity>` | `$total_projects` | Global entity count |
| `$proj.<key>` | `$proj.smoke` | Project database ID |
| `$group.<key>` | `$group.redaction` | Group database ID |
| `$user_counts.<user>.<entity>` | `$user_counts.lois.projects` | Per-user visible entity count |
| `$project_counts.<proj>.<entity>` | `$project_counts.frontend.merge_requests` | Per-project entity count |

### Expectations

| Form | Meaning |
|------|---------|
| `{ eq: 3 }` | Exactly 3 result rows |
| `{ eq: "$variable" }` | Exact count resolved from manifest |
| `{ gte: 1 }` | At least 1 row |
| `{ range: [5, 10] }` | Between 5 and 10 rows (inclusive) |

### Fan-out

Use `users` (plural) to run the same tests for multiple users. Test names are automatically prefixed with the username:

```yaml
- section: "No memberships"
  users: [vickey, hanna]
  tests:
    - name: "0 projects"
      ...
# Generates: "vickey.schmidt: 0 projects", "hanna: 0 projects"
```

### Regenerating Ruby scripts

```shell
cargo xtask e2e codegen          # write generated files
cargo xtask e2e codegen --check  # verify committed files match (CI)
```

## Test architecture

Tests run as Ruby scripts inside the GitLab toolbox pod, which has access to the Rails console and the GKG gRPC endpoint.

- **`scenarios.yaml`** -- Single source of truth for test data definitions and assertions. Drives codegen.
- **`create_test_data.rb`** -- Auto-generated. Creates a test matrix: users with varying group memberships, projects across public and private groups, MRs, work items, and notes. Writes a `manifest.json` describing what was created.
- **`redaction_test.rb`** -- Auto-generated. Queries the GKG graph as each test user and asserts that redaction is correct: users only see entities in namespaces they have access to. Outputs JSON results.
- **`test_helper.rb`** -- Shared utilities for gRPC calls, manifest loading, and assertion helpers. Not generated.

## Module structure

See [`crates/xtask/src/e2e/README.md`](../../crates/xtask/src/e2e/README.md) for the source code layout.
