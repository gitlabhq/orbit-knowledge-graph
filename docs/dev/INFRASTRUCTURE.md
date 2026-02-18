# Infrastructure Overview

**GCP Project:** `gl-knowledgegraph-prj-f2eec59d`

For Kubernetes deployments, see `helm-dev/` charts (source of truth for component configuration).

## Virtual Machines

| Name | Type | Zone | Internal IP | External IP |
|------|------|------|-------------|-------------|
| vm-clickhouse | n4-standard-16 | us-central1-b | 10.128.0.13 | 136.115.179.207 |
| vm-gitlab-omnibus | n4-standard-8 | us-central1-b | 10.128.0.4 | 34.9.130.112 |

See [GITLAB_INSTANCE.md](GITLAB_INSTANCE.md) for GitLab-specific configuration.

## GKE Cluster

| Name | Location | Pod CIDR |
|------|----------|----------|
| knowledge-graph-test | us-central1 | 10.83.0.0/17 |

### Internal Services

| Service | Internal IP | Ports | GCP Address Name |
|---------|-------------|-------|------------------|
| gkg-webserver | 10.128.0.51 | 8080 (HTTP), 50051 (gRPC) | `gkg-webserver-ip` |

Static internal IP reserved in GCP ensures the address persists across service recreations.

## Secrets (GCP Secret Manager)

| Name | Purpose |
|------|---------|
| clickhouse-password | ClickHouse default user |
| gitaly-token | Gitaly gRPC authentication |
| gkg-jwt-secret | GitLab internal API JWT signing |
| postgres-password | PostgreSQL gitlab user |
| runner_authentication_token | GitLab Runner registration token (glrt-) |
| grafana-oauth-client-id | Google OAuth client ID for Grafana |
| grafana-oauth-client-secret | Google OAuth client secret for Grafana |
| grafana-gitlab-oauth-client-id | GitLab OAuth client ID for Grafana |
| grafana-gitlab-oauth-client-secret | GitLab OAuth client secret for Grafana |

## DNS

| Domain | Type | Target |
|--------|------|--------|
| grafana.gkg.dev | A | GKE Ingress Load Balancer IP |

DNS for `gkg.dev` is managed externally.

## Firewall Rules

<!-- vale gitlab_base.Substitutions = NO -->
| Rule | Source | Ports | Target |
|------|--------|-------|--------|
| allow-gke-pods-to-postgres | 10.83.0.0/17 | tcp:5432 | gitlab-omnibus-vm |
<!-- vale gitlab_base.Substitutions = YES -->
| allow-gke-pods-to-clickhouse | 10.83.0.0/17 | tcp:8123,8443,9000 | clickhouse-vm |
| allow-gke-pods-to-gitaly | 10.83.0.0/17 | tcp:8075 | gitlab-omnibus-vm |
| default-allow-internal | 10.128.0.0/9 | all | all instances |

## PostgreSQL (Linux package)

- User `gitlab` has `REPLICATION` privilege
- `pg_hba.conf` allows replication from GKE pod CIDR (10.83.0.0/17)

## ClickHouse

**Databases:**

| Database | Purpose |
|----------|---------|
| gitlab_clickhouse_main_production | Datalake - siphon replicated tables, config tables |
| gkg-sandbox | Graph database - indexed graph nodes and edges |

**Users:**

| User | Password | Access |
|------|----------|--------|
| default | (from GCP Secret Manager) | Full access |
| grafana_reader | (none) | Read-only (readonly=2), used by Grafana |

**Grafana user setup (sandbox):**

```sql
CREATE USER grafana_reader IDENTIFIED WITH no_password SETTINGS readonly = 2;
GRANT SELECT ON gitlab_clickhouse_main_production.* TO grafana_reader;
GRANT SELECT ON `gkg-sandbox`.* TO grafana_reader;
```

## Workload Identity

GCP service account `gkg-secrets-sa@gl-knowledgegraph-prj-f2eec59d.iam.gserviceaccount.com` is bound to K8s ServiceAccount `gcp-secrets-sa` in `gkg` namespace.
