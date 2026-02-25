# E2E module

Orchestrates a local Kubernetes environment for end-to-end testing of the GKG stack against a real GitLab instance. Deploys Colima, GitLab (via CNG Helm chart), ClickHouse, siphon, and the GKG indexer/webserver, then runs redaction and permission tests.

## Architecture

```plaintext
pipeline/          -- orchestration steps (setup, teardown, rebuild, test)
  cng.rs           -- Colima VM, Traefik, GitLab Helm deploy (steps 1-6)
  cngsetup.rs      -- PG credentials, migrations, test data (steps 8-13)
  gkg.rs           -- ClickHouse, schema, GKG chart, siphon wait, dispatch-indexing, tests (steps 15-25)
  rebuild.rs       -- fast image rebuild + rollout restart
  teardown.rs      -- full or partial teardown
  test.rs          -- standalone test runner

infra/             -- infrastructure primitives (no domain logic)
  colima.rs        -- Colima VM lifecycle
  docker.rs        -- Bollard Docker client helpers
  helm.rs          -- Helm CLI: install, upgrade, uninstall, repo add
  kube.rs          -- kube-rs primitives: SSA apply, cp, exec, wait, delete

config.rs          -- deserializes config/e2e.yaml into Config
constants.rs       -- structural constants (paths, table lists, tool names)
template.rs        -- ${VAR} template renderer for YAML manifests
utils.rs           -- domain helpers (toolbox pod, PG queries, ClickHouse queries, secrets)
ui.rs              -- cliclack terminal output
cmd.rs             -- xshell helpers
env.rs             -- path utilities
```

## Static files

```plaintext
config/e2e.yaml                          -- all configurable values
e2e/cng/gitlab-values.yaml               -- GitLab Helm overrides
e2e/cng/traefik-values.yaml              -- Traefik Helm overrides
e2e/cng/clickhouse.yaml.tmpl             -- ClickHouse k8s manifest (templated)
e2e/cng/Dockerfile.rails                 -- CNG image overlay
e2e/helm-values.yaml                     -- GKG Helm overrides
e2e/templates/dispatch-indexing-job.yaml.tmpl
e2e/templates/rails-clickhouse-config.yml.tmpl
e2e/tests/{redaction_test,test_helper,create_test_data}.rb
```

For usage, configuration, and the full pipeline walkthrough, see [docs/dev/e2e.md](../../../../docs/dev/e2e.md).
