# GKG E2E with Caproni -- Plan

Self-contained E2E test environment for GKG using Caproni to manage a full
GitLab instance in-cluster. No GDK required.

## Architecture

```
                          colima k8s cluster
 +-----------------------------------------------------------------+
 |                                                                   |
 |  namespace: kube-system       namespace: gitlab                   |
 |  +----------+                 +-----------------------------+     |
 |  | traefik  |                 | webservice  (custom CNG)    |     |
 |  +----------+                 | sidekiq     (custom CNG)    |     |
 |                               | toolbox     (custom CNG)    |     |
 |                               | postgresql  (wal_level=log) |     |
 |                               | redis                       |     |
 |                               | minio                       |     |
 |                               | gitaly                      |     |
 |                               | gitlab-shell                |     |
 |                               +-----------------------------+     |
 |                                                                   |
 |  namespace: default                                               |
 |  +------------------------------------------------------------+   |
 |  | gkg-e2e-clickhouse  (datalake + graph databases)           |   |
 |  | gkg-e2e-nats                                               |   |
 |  | siphon-producer     (gitlab PG -> NATS)                    |   |
 |  | siphon-consumer     (NATS -> ClickHouse)                   |   |
 |  | gkg-indexer          (ClickHouse graph ETL)                |   |
 |  | gkg-webserver        (HTTP + gRPC API)                     |   |
 |  | gkg-health-check                                           |   |
 |  +------------------------------------------------------------+   |
 |                                                                   |
 +-----------------------------------------------------------------+
```

**Caproni manages:** cluster lifecycle, traefik ingress, GitLab Helm chart
(webservice, sidekiq, toolbox, postgresql, redis, minio, gitaly, shell).

**Tilt manages:** GKG infrastructure (ClickHouse, NATS, Siphon, GKG server).

**Tests run from:** the gitlab-toolbox pod via `kubectl exec`.

## Data flow

```
PostgreSQL (in-cluster, gitlab ns)
    |
    | logical replication (wal_level=logical)
    v
Siphon producer (default ns) --NATS--> Siphon consumer (default ns)
    |
    | writes to ClickHouse
    v
ClickHouse (default ns)
    |
    | ETL via gkg-indexer
    v
gkg-development database (gl_group, gl_project, gl_merge_request, ...)
    |
    | queried by
    v
gkg-webserver (HTTP/gRPC API)
    |
    | called by test scripts via
    v
gitlab-toolbox pod (bundle exec rails runner ...)
```

## Custom CNG images

The GitLab feature branch (`gkg-feature-branch-working-copy`) contains code
that does not exist in stock CNG releases:

- `Ai::KnowledgeGraph::AuthorizationContext` (JWT claim builder)
- `Ai::KnowledgeGraph::GrpcClient` (GKG server client)
- `Ai::KnowledgeGraph::JwtAuth` (token generation)
- DB migrations for `knowledge_graph_enabled_namespaces`
- `knowledge_graph:` config section in `gitlab.yml`
- Feature flag definitions

We build custom CNG images locally by overlaying the feature branch Rails code
onto stock CNG base images. See `Dockerfile.rails` and `build-images.sh`.

CNG image structure (all three inherit from `gitlab-rails-ee`):

| Image | Base | Rails code at |
|-------|------|---------------|
| `gitlab-webservice-ee` | `gitlab-rails-ee` | `/srv/gitlab/` |
| `gitlab-sidekiq-ee` | `gitlab-rails-ee` | `/srv/gitlab/` |
| `gitlab-toolbox-ee` | `gitlab-rails-ee` | `/srv/gitlab/` |

## Siphon bootstrap

Siphon is fully self-bootstrapping. At startup it:

1. Creates the publication if it doesn't exist (`SetupPublication()`)
2. Reconciles tables in the publication against `table_mapping` config
3. Creates the replication slot if it doesn't exist (`SetupReplicationSlot()`)
4. Begins streaming

The only prerequisite is `wal_level=logical` on the PostgreSQL server and a
user with sufficient privileges. No manual publication/slot setup needed.

Source: `siphon/pkg/siphon/publication.go`, `replication_manager.go`.

## Workflow

```shell
# 1. Build custom CNG images (~5-10 min first time)
cd ~/Desktop/Code/gkg/e2e/caproni
./build-images.sh ~/Desktop/Code/gdk/gitlab

# 2. Boot cluster + deploy GitLab (~5-10 min)
caproni up

# 3. Post-deploy setup (migrations, feature flag, test data, secrets)
./post-deploy.sh

# 4. Deploy GKG infrastructure
cd ~/Desktop/Code/gkg
GKG_E2E_CAPRONI=1 mise exec -- tilt up --file e2e/tilt/Tiltfile

# 5. Run tests
kubectl exec -n gitlab deploy/gitlab-toolbox -- \
  bundle exec rails runner /tmp/e2e/redaction_test.rb
```

## Risks and open questions

### PG user REPLICATION privilege

The `gitlab` user created by the Bitnami PG subchart is the database owner.
It should have permission to create publications (requires table ownership)
but may lack the `REPLICATION` privilege needed to create replication slots.

**Mitigation:** Add `ALTER USER gitlab REPLICATION;` in the initdb script
within `gitlab-values.yaml`.

**Status:** Untested. If Siphon fails to create the slot, check PG logs:
```shell
kubectl logs -n gitlab statefulset/gitlab-postgresql | grep -i replication
```

### bundle install in custom image

The stock CNG image may have build dependencies (gcc, make, etc.) cleaned up
to reduce image size. If `bundle install` fails with native extension errors,
the Dockerfile needs to install `build-essential` first.

**Mitigation:** The Dockerfile includes a conditional `apt-get install` for
build tools. Monitor build output for failures.

### Frontend assets

The stock CNG image contains compiled webpack assets for the stock release.
Our feature branch may have changed frontend code. If tests require the
GitLab web UI (they currently don't -- they use `rails runner`), we'd need
to compile assets in the Dockerfile with `rake gitlab:assets:compile` (adds
~10 min to build time).

**Status:** Not needed for current E2E tests (backend-only via `rails runner`).

### Memory pressure

The full GitLab Helm chart (~4GB) plus GKG infrastructure (~2GB for ClickHouse
+ misc) needs to fit in 12GiB. This is tight.

**Mitigation:** Start with 12GiB. If OOM kills occur, bump to 16GiB in
`caproni.yaml`.

### knowledge_graph config in gitlab.yml

The CNG base image has a stock `gitlab.yml` without the `knowledge_graph:`
section. The feature branch overlay adds it, but the Helm chart may overwrite
`gitlab.yml` with its own template at container startup.

**Mitigation options:**
1. Patch the ConfigMap post-deploy: `kubectl edit configmap -n gitlab gitlab-webservice`
2. Use `global.appConfig.extra` if the chart supports it
3. Inject via environment variables if the Rails code supports it

**Status:** Needs investigation. Check what the webservice entrypoint does
with `gitlab.yml`.

### Base CNG tag selection

The feature branch is based on `18.9.0-pre`. The closest stable CNG release
is `v18.8.x`. If the branch diverges significantly from the base image's
Ruby/gem environment, `bundle install` may fail or produce incompatible
results.

**Mitigation:** Use the latest `v18.8.x` tag. If issues arise, try `master`
tag (bleeding edge, may be unstable).

### dynamic_overrides not implemented in caproni

Caproni's `dynamic_overrides` mechanism (for reading `*_VERSION` files) is
a TODO in the source (`deployer/helm/manager.go:216`). We bypass it entirely
by building custom images locally and setting `imagePullPolicy: Never`.

### Test file access from toolbox pod

Test files live in the GKG repo, not in the GitLab Rails tree. The toolbox
pod won't have them by default.

**Mitigation:** `kubectl cp` the test files into the toolbox pod during
`post-deploy.sh`.

### gitlabhq_production vs gitlabhq_development

The GitLab Helm chart uses `gitlabhq_production` as the database name. Our
GDK-mode Tilt values use `gitlabhq_development`. The `values-caproni.yaml`
overlay handles this, but all test scripts that reference database names
must use the correct one for the mode.
