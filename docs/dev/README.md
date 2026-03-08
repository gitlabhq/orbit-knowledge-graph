# Development guides

## Getting started

| Guide | Description |
|-------|-------------|
| [Local development (GDK)](local/gdk.md) | Run GKG against GDK services on your host. No K8s required. |
| [E2E testing](e2e.md) | Self-contained E2E harness using Colima + CNG + Helm. |

## GCP sandbox

Operational docs for the shared GCP sandbox environment (`gl-knowledgegraph-prj-f2eec59d`).

| Guide | Description |
|-------|-------------|
| [Infrastructure](sandbox/INFRASTRUCTURE.md) | VMs, GKE cluster, networking, secrets. |
| [Runbook](sandbox/RUNBOOK.md) | Deployment, alerting, Siphon reset, manual dispatch. |
| [GitLab instance](sandbox/GITLAB_INSTANCE.md) | `vm-gitlab-omnibus` configuration (Gitaly, JWT, AI Gateway). |
| [K8s CI/CD setup](sandbox/K8S_CICD_SETUP.md) | Service account and CI variable configuration for GKE deploys. |
