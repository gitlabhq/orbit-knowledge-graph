# Infrastructure Overview

**GCP Project:** `gl-knowledgegraph-prj-f2eec59d`

For Kubernetes deployments, see `helm-dev/` charts (source of truth for component configuration).

## Virtual Machines

| Name | Type | Zone | Internal IP | External IP |
|------|------|------|-------------|-------------|
| vm-clickhouse | n4-standard-4 | us-central1-b | 10.128.0.13 | 34.61.31.19 |
| vm-gitlab-omnibus | n4-standard-8 | us-central1-b | 10.128.0.4 | 34.9.130.112 |

## GKE Cluster

| Name | Location | Pod CIDR |
|------|----------|----------|
| knowledge-graph-test | us-central1 | 10.83.0.0/17 |

## Secrets (GCP Secret Manager)

| Name | Purpose |
|------|---------|
| clickhouse-password | ClickHouse default user |
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

| Rule | Source | Ports | Target |
|------|--------|-------|--------|
| allow-gke-pods-to-postgres | 10.83.0.0/17 | tcp:5432 | gitlab-omnibus-vm |
| allow-gke-pods-to-clickhouse | 10.83.0.0/17 | tcp:8123,8443,9000 | clickhouse-vm |
| default-allow-internal | 10.128.0.0/9 | all | all instances |

## PostgreSQL (GitLab Omnibus)

- User `gitlab` has `REPLICATION` privilege
- `pg_hba.conf` allows replication from GKE pod CIDR (10.83.0.0/17)

## ClickHouse

- Database: `gitlab_clickhouse_main_production` (created by GitLab Omnibus migrations)
- Siphon tables: `siphon_projects`, `siphon_namespaces`, `siphon_users`, etc.

## Workload Identity

GCP service account `gkg-secrets-sa@gl-knowledgegraph-prj-f2eec59d.iam.gserviceaccount.com` is bound to K8s ServiceAccount `gcp-secrets-sa` in `gkg-sandbox` namespace.
