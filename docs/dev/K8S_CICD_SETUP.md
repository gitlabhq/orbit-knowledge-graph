# Kubernetes CI/CD Setup

This document describes the manual steps required to configure GitLab CI/CD to deploy Helm charts to GKE.

## Overview

The CI pipeline uses a GCP service account to authenticate with GKE. The service account key is stored as a base64-encoded GitLab CI variable.

| Component | Value |
|-----------|-------|
| GCP Project | `gl-knowledgegraph-prj-f2eec59d` |
| GKE Cluster | `knowledge-graph-test` |
| GKE Zone | `us-central1` |
| Service Account | `gitlab-ci-deployer` |

> **Note: Development/Reference Only**
>
> The approach documented here uses static service account keys created via `gcloud` commands. This is intended for development, testing, and reference purposes only.
>
> For production environments, consider more secure alternatives:
> - **GCP Workload Identity Federation with GitLab OIDC**: Eliminates static credentials entirely by allowing GitLab CI to authenticate using short-lived OIDC tokens. See [GitLab documentation](https://docs.gitlab.com/ee/ci/cloud_services/google_cloud/).
> - **GitLab Agent for Kubernetes**: Installs an agent in your cluster that establishes a secure tunnel to GitLab, avoiding the need for cluster credentials in CI variables.
> - **Terraform/Infrastructure as Code**: Manage service accounts, IAM bindings, and secrets through version-controlled infrastructure code with proper state management and audit trails.
>
> Static service account keys pose security risks: they don't expire automatically, can be leaked, and require manual rotation.

## Step 1: Create GCP Service Account

```bash
PROJECT_ID="gl-knowledgegraph-prj-f2eec59d"
SA_NAME="gitlab-ci-deployer"

gcloud iam service-accounts create ${SA_NAME} \
  --project=${PROJECT_ID} \
  --display-name="GitLab CI Helm Deployer"
```

## Step 2: Grant IAM Roles

The service account needs permissions to deploy workloads and manage the namespace:

```bash
PROJECT_ID="gl-knowledgegraph-prj-f2eec59d"
SA_EMAIL="gitlab-ci-deployer@${PROJECT_ID}.iam.gserviceaccount.com"

# container.developer: deploy workloads, manage pods/services/configmaps
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/container.developer"

# container.admin: create namespaces, full cluster access
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/container.admin"
```

### IAM Roles Reference

| Role | Purpose |
|------|---------|
| `roles/container.developer` | Deploy workloads, manage pods/services/configmaps |
| `roles/container.admin` | Full control including namespace creation |
| `roles/container.clusterViewer` | Read-only (for dry-run only pipelines) |

## Step 3: Create Service Account Key

```bash
PROJECT_ID="gl-knowledgegraph-prj-f2eec59d"
SA_EMAIL="gitlab-ci-deployer@${PROJECT_ID}.iam.gserviceaccount.com"

# Create JSON key
gcloud iam service-accounts keys create gke-sa-key.json \
  --iam-account="${SA_EMAIL}"
```

## Step 4: Add GitLab CI Variable

1. Navigate to your GitLab project
2. Go to **Settings** > **CI/CD** > **Variables**
3. Click **Add variable**
4. Configure the variable:

| Field | Value |
|-------|-------|
| Key | `GKE_SA_KEY` |
| Value | Upload `gke-sa-key.json` file |
| Type | File |
| Protected | Yes |
| Masked | No (file variables cannot be masked) |

5. Click **Add variable**
6. Delete the local key file:
   ```bash
   rm gke-sa-key.json
   ```

## Step 5: Protect the Main Branch

For the protected variable to work, ensure `main` is a protected branch:

1. Go to **Settings** > **Repository** > **Protected branches**
2. Verify `main` is listed as protected

## CI Jobs

After setup, the following Helm jobs are available:

| Job | Stage | Trigger | Description |
|-----|-------|---------|-------------|
| `helm-lint` | lint | MR + main | Validates chart syntax and templates |
| `helm-dry-run` | lint | MR + main | Server-side dry-run against cluster |
| `deploy-sandbox` | deploy | main (manual) | Deploys chart to GKE |

## Rotating the Service Account Key

To rotate the key:

1. Create a new key (Step 3)
2. Update the GitLab CI variable (Step 4)
3. Delete the old key:
   ```bash
   # List keys
   gcloud iam service-accounts keys list \
     --iam-account=gitlab-ci-deployer@gl-knowledgegraph-prj-f2eec59d.iam.gserviceaccount.com

   # Delete old key
   gcloud iam service-accounts keys delete KEY_ID \
     --iam-account=gitlab-ci-deployer@gl-knowledgegraph-prj-f2eec59d.iam.gserviceaccount.com
   ```
