# Sandbox Runbook

Operations guide for the GKG sandbox environment on GKE.

## Prerequisites

- `gcloud` CLI authenticated with access to `gl-knowledgegraph-prj-f2eec59d`
- `kubectl` configured for the cluster
- `helm` v3+

```bash
# Get cluster credentials
gcloud container clusters get-credentials knowledge-graph-test \
  --region=us-central1 \
  --project=gl-knowledgegraph-prj-f2eec59d
```

## Install Dependencies

### Cert-Manager

Required for TLS certificate management.

```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.19.2/cert-manager.yaml

# Wait for cert-manager to be ready
kubectl -n cert-manager wait --for=condition=Ready pods --all --timeout=120s
```

### External Secrets Operator

Required for syncing secrets from GCP Secret Manager to Kubernetes.

```bash
# Add helm repo
helm repo add external-secrets https://charts.external-secrets.io
helm repo update external-secrets

# Install
helm install external-secrets external-secrets/external-secrets \
  --namespace external-secrets \
  --create-namespace \
  --set installCRDs=true \
  --wait
```

**CRDs installed:**
- `externalsecrets.external-secrets.io`
- `secretstores.external-secrets.io`
- `clustersecretstores.external-secrets.io`

### GCP Service Account for Workload Identity

```bash
# Create GCP service account
gcloud iam service-accounts create gkg-secrets-sa \
  --display-name="GKG Secrets Service Account" \
  --project=gl-knowledgegraph-prj-f2eec59d

# Grant access to secrets
gcloud secrets add-iam-policy-binding postgres-password \
  --project=gl-knowledgegraph-prj-f2eec59d \
  --member="serviceAccount:gkg-secrets-sa@gl-knowledgegraph-prj-f2eec59d.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

gcloud secrets add-iam-policy-binding clickhouse-password \
  --project=gl-knowledgegraph-prj-f2eec59d \
  --member="serviceAccount:gkg-secrets-sa@gl-knowledgegraph-prj-f2eec59d.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

gcloud secrets add-iam-policy-binding runner_authentication_token \
  --project=gl-knowledgegraph-prj-f2eec59d \
  --member="serviceAccount:gkg-secrets-sa@gl-knowledgegraph-prj-f2eec59d.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

# Bind Workload Identity (replace PROJECT_NUMBER)
gcloud iam service-accounts add-iam-policy-binding \
  gkg-secrets-sa@gl-knowledgegraph-prj-f2eec59d.iam.gserviceaccount.com \
  --project=gl-knowledgegraph-prj-f2eec59d \
  --member="principal://iam.googleapis.com/projects/1079327125344/locations/global/workloadIdentityPools/gl-knowledgegraph-prj-f2eec59d.svc.id.goog/subject/ns/gkg-sandbox/sa/gcp-secrets-sa" \
  --role="roles/iam.workloadIdentityUser"
```

## Deploy Helm Chart

### Install

```bash
helm install gkg-sandbox ./helm-dev \
  -f ./helm-dev/values-sandbox.yaml \
  --namespace gkg-sandbox \
  --create-namespace \
  --wait
```

### Upgrade

```bash
helm upgrade gkg-sandbox ./helm-dev \
  -f ./helm-dev/values-sandbox.yaml \
  --namespace gkg-sandbox
```

### Check Status

```bash
# Pods
kubectl get pods -n gkg-sandbox

# External secrets sync status
kubectl get externalsecrets -n gkg-sandbox

# Helm release
helm list -n gkg-sandbox
```

### View Logs

```bash
# Producer
kubectl logs -n gkg-sandbox deployment/siphon-producer -f

# Consumer
kubectl logs -n gkg-sandbox deployment/siphon-consumer -f

# NATS
kubectl logs -n gkg-sandbox statefulset/nats -f

# GitLab Runner
kubectl logs -n gkg-sandbox deployment/gkg-sandbox-gitlab-runner -f
```

## GitLab Runner

The Helm chart includes a GitLab Runner (via subchart) for running CI jobs on gitlab.gkg.dev.

### Initial Setup

1. Create an instance runner in GitLab Admin → CI/CD → Runners → New instance runner
2. Enable "Run untagged jobs"
3. Click "Create runner" and copy the `glrt-` token
4. Store in GCP Secret Manager:
   ```bash
   echo -n "glrt-YOUR_TOKEN" | gcloud secrets versions add runner_authentication_token \
     --project=gl-knowledgegraph-prj-f2eec59d --data-file=-
   ```
5. Deploy/upgrade the Helm chart

### Rotate Runner Token

If the runner becomes unhealthy or is removed from GitLab:

1. Delete the old runner in GitLab Admin → CI/CD → Runners
2. Create a new instance runner and get the new token
3. Update the secret:
   ```bash
   echo -n "glrt-NEW_TOKEN" | gcloud secrets versions add runner_authentication_token \
     --project=gl-knowledgegraph-prj-f2eec59d --data-file=-
   ```
4. Force sync and restart:
   ```bash
   kubectl annotate externalsecret gitlab-runner-token -n gkg-sandbox force-sync=$(date +%s) --overwrite
   kubectl rollout restart deployment/gkg-sandbox-gitlab-runner -n gkg-sandbox
   ```

### Check Runner Status

```bash
# Runner pod
kubectl get pods -n gkg-sandbox -l app=gitlab-runner

# Runner logs
kubectl logs -n gkg-sandbox deployment/gkg-sandbox-gitlab-runner --tail=50

# Job pods (created during CI runs)
kubectl get pods -n gkg-sandbox | grep runner-
```

## Secrets

### GCP Secret Manager

| Secret Name | Purpose |
|-------------|---------|
| `postgres-password` | Password for PostgreSQL `gitlab` user |
| `clickhouse-password` | Password for ClickHouse `default` user |
| `runner_authentication_token` | GitLab Runner token (glrt-) |

### Kubernetes Secrets (synced via External Secrets)

| K8s Secret | Source | Used By |
|------------|--------|---------|
| `postgres-credentials` | `postgres-password` | siphon-producer |
| `clickhouse-credentials` | `clickhouse-password` | siphon-consumer |
| `gitlab-runner-token` | `runner_authentication_token` | gitlab-runner |

### Update a Secret

```bash
# Update in GCP Secret Manager
echo -n "new-password" | gcloud secrets versions add postgres-password --data-file=-

# Force refresh in Kubernetes (or wait for refresh interval)
kubectl annotate externalsecret postgres-credentials -n gkg-sandbox force-sync=$(date +%s) --overwrite
```

## Teardown

### Remove Helm Release

```bash
helm uninstall gkg-sandbox --namespace gkg-sandbox
```

### Remove Namespace (including PVCs)

```bash
kubectl delete namespace gkg-sandbox
```

### Remove External Secrets Operator

```bash
helm uninstall external-secrets --namespace external-secrets
kubectl delete namespace external-secrets
```

### Remove GCP Service Account

```bash
gcloud iam service-accounts delete \
  gkg-secrets-sa@gl-knowledgegraph-prj-f2eec59d.iam.gserviceaccount.com \
  --project=gl-knowledgegraph-prj-f2eec59d
```
