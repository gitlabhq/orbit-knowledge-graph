# E2E Testing Harness

End-to-end tests deploy a full GKG stack on a shared GKE cluster and run
Robot Framework tests against it.

## Architecture

Each run gets isolated namespaces keyed by commit SHA (`e2e-{sha}-*`).
The stack includes GitLab (with ClickHouse migrations), NATS, ClickHouse,
Siphon CDC, and all four GKG modes (webserver, indexer, dispatcher, health-check).

```
┌──────────┐    CDC     ┌──────┐   stream   ┌────────────┐
│  GitLab  │──────────▶│Siphon│──────────▶│ ClickHouse │
│  (+ PG)  │           │      │   (NATS)   │ datalake   │
└──────────┘           └──────┘           └─────┬──────┘
     ▲                                          │
     │ gRPC (TLS)                          read │
     │                                          ▼
┌──────────┐                              ┌──────────┐
│  Robot   │─── Orbit query API ────────▶│   GKG    │
│  tests   │                              │ webserver│
└──────────┘                              └──────────┘
```

## Cluster

- **GKE**: `gke_gl-knowledgegraph-prj-f2eec59d_us-central1-a_e2e-harness`
- **Harness config**: [gitlab-org/orbit/gkg-e2e-harness](https://gitlab.com/gitlab-org/orbit/gkg-e2e-harness) — cluster bootstrap (cert-manager, GitLab Agent)
- **CI access**: GitLab Agent `e2e-harness-agent`

## Running

```bash
# Local
e2e/scripts/setup.sh          # deploy full stack
e2e/scripts/test.sh           # run Robot Framework tests
e2e/scripts/teardown.sh -y    # cleanup

# Specific SHA
E2E_SHA=abc1234 e2e/scripts/setup.sh
E2E_SHA=abc1234 e2e/scripts/test.sh

# CI runs automatically on MR via e2e-build + e2e jobs
```

## Test suites

| Suite | What it tests |
|---|---|
| `01_setup_and_smoke.robot` | Bootstrap e2e-bot, enable KG flags, provision the shared namespace, smoke-test the pipeline |
| `02_indexing.robot` | Create projects, issues, notes, epics; assert SDLC nodes and edges land in Orbit |
| `03_code_indexing.robot` | Push fixture repos; assert File/Definition/IMPORTS/DEFINES via Orbit |
| `04_code_backfill.robot` | Enable KG on a populated namespace and verify backfill dispatches code indexing |
| `05_role_scoped_authz.robot` | Issue #347 — aggregation queries enforce per-entity authz on the target node. Seeds a victim user with Reporter/Security Manager/Developer/Maintainer/nested-subgroup memberships and replays the original oracle matrix per role. Requires Ultimate (activated by setup) and a GitLab image past `gitlab-org/gitlab@7e57f842dada` (publishes role-tagged traversal IDs). |

## Setup phases

| Phase | Script | What it does |
|---|---|---|
| 00 | `00-namespaces.sh` | Pre-create namespaces for secrets |
| 01 | `01-secrets.sh` | Generate passwords, create k8s secrets |
| 02 | `02-infra.sh` | Deploy NATS, ClickHouse, GitLab via helmfile |
| 03 | `03-wait-infra.sh` | Wait for pods + GitLab migrations (incl. ClickHouse) |
| 04 | `04-pg-siphon.sh` | Configure PG users, publication, alter function for Siphon |
| 06 | `06-pipeline.sh` | Deploy Siphon + GKG via helmfile |
| 07 | `07-seed-toolbox.sh` | Create e2e-bot user/PAT, enable feature flags |

## Data-driven CDC tables

`e2e/config/cdc-tables.yaml` is the single source of truth for CDC table
definitions. It drives the PG publication table list and the Siphon helm values
(table mapping, streams, dedup config). Add a table once there and all
consumers update automatically.

## Key files

- `e2e/helmfile.yaml.gotmpl` — all helm releases
- `e2e/values/` — per-component helm values (`.gotmpl` for templated ones)
- `e2e/charts/` — local charts (ClickHouse, robot-runner)
