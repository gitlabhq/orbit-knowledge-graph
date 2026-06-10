# E2E Testing Harness

End-to-end tests deploy a full GKG stack on a shared GKE cluster and run
Robot Framework tests against it.

## Architecture

Each run gets isolated namespaces keyed by commit SHA (`e2e-{sha}-*`).
The stack includes GitLab (with ClickHouse migrations), NATS, ClickHouse,
Siphon CDC, and all four GKG modes (webserver, indexer, dispatcher, health-check).

```plaintext
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
- **Harness config**: [`gitlab-org/orbit/gkg-e2e-harness`](https://gitlab.com/gitlab-org/orbit/gkg-e2e-harness) — cluster bootstrap (cert-manager, GitLab Agent)
- **CI access**: GitLab Agent `e2e-harness-agent`

## Running

```shell
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
| `06_incremental_update.robot` | Rails-side note delete propagates; Orbit stops returning the tombstoned node |
| `07_namespace_lifecycle.robot` | Disable retains indexed data (30-day grace); re-enable resumes indexing |
| `08_private_redaction.robot` | Private project/issue redacted from a non-member, visible to admin |
| `09_api_surface.robot` | Read-only Orbit endpoints: schema, schema/dsl, schema/format, graph_status, tools, commands |
| `10_query_shapes.robot` | neighbors, path_finding, and llm (GOON) response format |
| `11_security_graph.robot` | Vulnerability node plus IN_PROJECT/AUTHORED/OCCURRENCE_OF edges |
| `12_membership_graph.robot` | MEMBER_OF (User→Group) and CREATOR (User→Project) edges |
| `13_cross_namespace_traversal.robot` | Scoped-query traversal-path pruning must not drop cross-namespace related entities |

## Parallel execution

The robot-runner job executes suites with [pabot](https://pabot.org/): suite
`01_setup_and_smoke` runs alone first (it bootstraps credentials, provisions
the shared namespace, and proves the pipeline reached steady state), then every
other suite runs in a parallel worker pool.

- `e2e/tests/ordering.txt` defines the barrier: suites listed before `#WAIT`
  run first; everything else is auto-discovered. A new `NN_name.robot` file
  needs no registration — it joins the parallel pool automatically.
- Suite 01 publishes the shared namespace through PabotLib parallel keys;
  downstream suites adopt it via the `Attach To Shared Fixture` suite setup
  (`gitlab.resource`), which also mints a per-suite admin bot user. The Rails
  `orbit_query` rate limit (60 req/min) is scoped per user, so suites must not
  poll through one shared PAT. A new suite that needs credentials or the
  shared namespace must declare that setup (copy the header of any existing
  suite).
- Suites must not depend on state created by other downstream suites, and
  instance-global mutations (feature flags, license) belong in 01 before the
  barrier.
- Plain `robot` runs still work for local debugging: PabotLib degrades to an
  in-process value store, so `robot tests/` executes the suites sequentially
  with identical semantics.

## Setup phases

| Phase | Script | What it does |
|---|---|---|
| 00 | `00-namespaces.sh` | Pre-create namespaces for secrets |
| 01 | `01-secrets.sh` | Generate passwords, create k8s secrets |
| 02 | `02-infra.sh` | Deploy NATS, ClickHouse, GitLab via helmfile |
| 03 | `03-wait-infra.sh` | Wait for pods + GitLab migrations (incl. ClickHouse) |
| 04 | `04-pg-siphon.sh` | Configure PostgreSQL users, publication, alter function for Siphon |
| 06 | `06-pipeline.sh` | Deploy Siphon + GKG via helmfile |
| 07 | `07-seed-toolbox.sh` | Create e2e-bot user/PAT, enable feature flags |

## Data-driven CDC tables

`e2e/config/cdc-tables.yaml` is the single source of truth for CDC table
definitions. It drives the PostgreSQL publication table list and the Siphon Helm values
(table mapping, streams, dedup config). Add a table once there and all
consumers update automatically.

## Key files

- `e2e/helmfile.yaml.gotmpl` — all Helm releases
- `e2e/values/` — per-component Helm values (`.gotmpl` for templated ones)
- `e2e/charts/` — local charts (ClickHouse, robot-runner)
