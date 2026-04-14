# E2E Test Environment Context

## What this is

Full e2e environment for GKG: GitLab -> Siphon CDC -> NATS -> ClickHouse -> GKG indexer/webserver.
Runs on GKE Standard cluster `gke_gl-knowledgegraph-prj-f2eec59d_us-central1-a_e2e-harness`.

## Namespace isolation

Each deployment uses a commit SHA prefix: `e2e-{sha}-{component}`.
Multiple e2e environments can coexist on the same cluster for different commits.
A `e2e-{sha}-secrets` namespace stores a master secret with all generated passwords for recovery during partial reruns.

SHA is resolved from `git rev-parse --short=7 HEAD` locally or `CI_COMMIT_SHORT_SHA` in CI. Override with `E2E_SHA` env var.

## Commands

```bash
# Deploy (uses current commit SHA by default)
scripts/setup.sh

# Deploy for a specific SHA
E2E_SHA=abc1234 scripts/setup.sh

# Test
E2E_SHA=abc1234 scripts/test.sh

# Teardown specific SHA
scripts/teardown.sh --sha=abc1234

# Teardown all e2e-* namespaces
scripts/teardown.sh

# Add -y to skip confirmation
scripts/teardown.sh --sha=abc1234 -y
```

## Directory structure

```
e2e/
  config/
    cdc-tables.yaml           # Single source of truth for CDC table definitions
  scripts/
    lib.sh                    # Shared variables (KC, NS_*, SHA) and functions
    setup.sh                  # Orchestrates all phases in order
    test.sh                   # Deploys robot-runner chart, waits, reports
    teardown.sh               # Helm uninstall + namespace deletion + PV cleanup
    phases/
      00-namespaces.sh        # Pre-create namespaces for secrets
      01-secrets.sh           # Generate passwords, create k8s secrets
      02-infra.sh             # helmfile sync phase=infra (NATS, CH, GitLab)
      03-wait-infra.sh        # Wait for pods + GitLab migrations
      04-pg-siphon.sh         # PG users, publication (from cdc-tables.yaml), alter fn
      05-ch-schema.sh         # Apply datalake + graph DDL to ClickHouse
      06-pipeline.sh          # helmfile sync phase=pipeline (Siphon, GKG)
      07-seed-toolbox.sh      # Create e2e-bot user/PAT, enable feature flags (fatal)
  charts/
    clickhouse/               # Local CH chart (StatefulSet + init scripts)
    robot-runner/             # Robot Framework test runner Job chart
  values/
    nats.yaml                 # Static NATS values
    clickhouse.yaml.gotmpl    # CH passwords from env vars
    gitlab.yaml.gotmpl        # GitLab config (knowledgeGraph, PG, certs)
    siphon.yaml.gotmpl        # Generated from config/cdc-tables.yaml
    gkg.yaml.gotmpl           # GKG 4-mode config
  helmfile.yaml.gotmpl        # Release definitions (phase=infra, phase=pipeline)
  tests/                      # Robot Framework test suites
  toolbox/                    # Ruby scripts for GitLab seeding
```

## Data-driven CDC tables

`config/cdc-tables.yaml` is the single source of truth for CDC table definitions. It drives:

1. **PG publication** (`04-pg-siphon.sh`) — `cdc_table_names()` extracts table names
2. **Siphon table_mapping** (`siphon.yaml.gotmpl`) — generates entries with column transforms
3. **Siphon streams** (`siphon.yaml.gotmpl`) — generates stream list
4. **Siphon dedup_config** (`siphon.yaml.gotmpl`) — generates dedup entries with custom keys

To add a new CDC table, add it to `cdc-tables.yaml` with optional `dedup_by`, `dedup_by_table`, and `ignore_columns` fields. All three consumers update automatically.

## Namespaces (for SHA `abc1234`)

`e2e-abc1234-secrets`, `e2e-abc1234-nats`, `e2e-abc1234-clickhouse`, `e2e-abc1234-gitlab`, `e2e-abc1234-siphon`, `e2e-abc1234-gkg`

## Components & Charts

| Component | Chart Source | Notes |
|---|---|---|
| GitLab | `gitlab-devel/gitlab` (devel channel) | Master branch chart + CNG images. Needed for `knowledgeGraph` config not yet in stable release |
| NATS | `nats/nats` 2.12.6 | JetStream with file store PVC, single node |
| ClickHouse | Local chart `charts/clickhouse/` | StatefulSet with PVC and init scripts for users/databases |
| Siphon | `siphon/siphon` 1.10.0 | Producer + consumer |
| GKG | OCI `registry.gitlab.com/gitlab-org/orbit/gkg-helm-charts/gkg:0.18.1` | All 4 modes. Image configurable via `E2E_GKG_IMAGE`/`E2E_GKG_TAG` env vars (CI builds from source to `gkg-e2e:$SHA`). TLS cert for gRPC via `extraResources` cert-manager Certificate |
| Robot Runner | Local chart `charts/robot-runner/` | Robot Framework test Job, pip-installed deps, tests mounted from ConfigMap |

## Secrets

All secrets are generated dynamically per setup run via `openssl rand`:
- JWT key, ClickHouse passwords (5 users), PostgreSQL siphon password
- All stored in `e2e-master-secrets` in the secrets namespace for recovery
- No hardcoded passwords in values files — all injected via env vars through `.gotmpl` templates

Secrets created:
- `e2e-master-secrets` in secrets namespace — all generated passwords
- `gitlab-knowledge-graph-jwt` in gitlab namespace — JWT shared key
- `gkg-secrets` in GKG namespace — JWT + ClickHouse passwords
- `gkg-grpc-tls` in GKG namespace — TLS cert/key for gRPC (issued by cert-manager `e2e-ca` ClusterIssuer)
- `gkg-grpc-ca` in gitlab namespace — root CA cert for GitLab to trust GKG gRPC TLS
- `e2e-test-credentials` in GKG namespace — GitLab PAT + service URLs

## TLS (GitLab -> GKG gRPC)

GitLab's `GrpcClient` only sends JWT auth over TLS (`tls://` scheme) or private-IP connections.
GKG gRPC is TLS-enabled using a cert issued by the cluster's cert-manager `e2e-ca` ClusterIssuer.

- GKG helm `extraResources` creates a cert-manager `Certificate` -> secret `gkg-grpc-tls`
- GKG helm `tls.enabled: true` + `tls.existingSecret: gkg-grpc-tls` mounts cert at `/etc/tls/`
- Root CA copied from `root-ca-secret` (cert-manager namespace) to `gkg-grpc-ca` in gitlab namespace
- GitLab `global.certificates.customCAs` injects the CA into the trust store
- GitLab `knowledgeGraph.grpcEndpoint` uses `tls://gkg-webserver.{ns}.svc.cluster.local:50054`

## GitLab test user and feature flags

Phase `07-seed-toolbox.sh` copies `toolbox/*.rb` scripts into the GitLab toolbox pod and runs them via `gitlab-rails runner`. Failures are fatal — setup aborts if PAT creation or feature flag enablement fails.

Scripts (executed in order):
- `create_user_and_pat.rb` — creates admin `e2e-bot` user via `Users::CreateService` (requires `organization_id: 1`) and a PAT with `api` + `read_api` scopes
- `enable_feature_flags.rb` — enables `knowledge_graph_infra` and `knowledge_graph` feature flags

## Test runner

`scripts/test.sh` installs the `robot-runner` Helm chart which creates a k8s Job running Robot Framework (`python:3.12-slim` + `robotframework` + `robotframework-requests`). Test files uploaded as ConfigMap, PAT injected via `secretKeyRef`.

Test suites:
- `connectivity.robot` — NATS monitor, ClickHouse HTTP, GitLab readiness, GKG liveness + readiness
- `api.robot` — GitLab user info, projects list, orbit status (full pipeline: GitLab -> gRPC TLS -> GKG)

On failure, prints diagnostics (pod status across all namespaces).

## Key Config Decisions

- **TLS for gRPC** — cert-manager issued, required for GitLab JWT auth
- **No TLS** for HTTP between other services (NATS, ClickHouse, GitLab HTTP)
- **GitLab certmanager disabled** — external cert-manager from harness bootstrap
- **Redis persistence disabled** — not needed for ephemeral e2e
- **Toolbox 3Gi memory** — 1Gi causes OOMKill on `gitlab-rails runner`
- **ClickHouse users/databases** created via init scripts in `docker-entrypoint-initdb.d`, passwords from helm values
- **Helmfile `.gotmpl` values** — all cross-namespace DNS and passwords templated via env vars
- **`KCTX` env var** — overrides kube context for CI (GitLab Agent) vs local (GKE direct)

## Service DNS (for SHA `abc1234`)

```
nats.e2e-abc1234-nats.svc.cluster.local:4222
clickhouse.e2e-abc1234-clickhouse.svc.cluster.local:8123
clickhouse.e2e-abc1234-clickhouse.svc.cluster.local:9000
gitlab-webservice-default.e2e-abc1234-gitlab.svc.cluster.local:8181
gitlab-postgresql.e2e-abc1234-gitlab.svc.cluster.local:5432
gkg-webserver.e2e-abc1234-gkg.svc.cluster.local:8080
gkg-webserver.e2e-abc1234-gkg.svc.cluster.local:50054 (TLS)
```

## CI Pipeline

Manual `e2e-build` job in `.gitlab-ci.yml` triggers the chain:

1. **`e2e-build`** (manual) — debug compile with sccache on `rust-builder`, builds+pushes Docker image to `$CI_REGISTRY_IMAGE/gkg-e2e:$SHA`
2. **`e2e`** (auto) — deploys full stack via helmfile using built image, runs Robot Framework tests, always tears down via `after_script`

Cluster access via GitLab Agent `e2e-harness-agent` (config project: `gitlab-org/orbit/gkg-e2e-harness`).

## Known Issues / Gotchas

- `kubectl wait --for=condition=Ready --all` hangs on Completed job pods — setup uses `--field-selector`
- GitLab stable chart lacks `knowledgeGraph` config — must use devel channel
- GitLab `Users::CreateService` requires `organization_id: 1` (not `User.create!`)
- NATS JetStream needs file-backed storage for streams/KV — memory-only mode causes `insufficient storage` errors
