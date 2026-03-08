# Sandbox Runbook

Operations guide for the GKG sandbox environment on GKE.

## Prerequisites

- `gcloud` CLI authenticated with access to `gl-knowledgegraph-prj-f2eec59d`
- `kubectl` configured for the cluster
- `helm` v3+

```shell
# Get cluster credentials
gcloud container clusters get-credentials knowledge-graph-test \
  --region=us-central1 \
  --project=gl-knowledgegraph-prj-f2eec59d
```

## Install Dependencies

### Cert-Manager

Required for TLS certificate management.

```shell
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.19.2/cert-manager.yaml

# Wait for cert-manager to be ready
kubectl -n cert-manager wait --for=condition=Ready pods --all --timeout=120s
```

### External Secrets Operator

Required for syncing secrets from GCP Secret Manager to Kubernetes.

```shell
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

### Prometheus Operator CRDs

Required for kube-prometheus-stack. Install before deploying the Helm chart:

```shell
PROMETHEUS_OPERATOR_VERSION=v0.88.1

for crd in alertmanagerconfigs alertmanagers podmonitors probes \
           prometheusagents prometheuses prometheusrules scrapeconfigs \
           servicemonitors thanosrulers; do
  kubectl apply --server-side -f \
    "https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/${PROMETHEUS_OPERATOR_VERSION}/example/prometheus-operator-crd/monitoring.coreos.com_${crd}.yaml"
done
```

For local development with Tilt, this is handled automatically in the Tiltfile.

### GCP Service Account for Workload Identity

```shell
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
  --member="principal://iam.googleapis.com/projects/1079327125344/locations/global/workloadIdentityPools/gl-knowledgegraph-prj-f2eec59d.svc.id.goog/subject/ns/gkg/sa/gcp-secrets-sa" \
  --role="roles/iam.workloadIdentityUser"
```

### Grafana OAuth Setup

Grafana uses Google and GitLab OAuth for authentication. Both require OAuth applications and secrets in GCP Secret Manager.

#### 1. Create Google OAuth Application

1. Go to [Google Cloud Console → APIs & Services → Credentials](https://console.cloud.google.com/apis/credentials?project=gl-knowledgegraph-prj-f2eec59d)
2. Create OAuth 2.0 Client ID (Web application)
3. Add authorized redirect URI: `https://grafana.gkg.dev/login/google`
4. Save the Client ID and Client Secret

#### 2. Create GitLab OAuth Application

1. Go to [GitLab → User Settings → Applications](https://gitlab.com/-/user_settings/applications)
2. Create new application with:
   - Name: `GKG Grafana`
   - Redirect URI: `https://grafana.gkg.dev/login/gitlab`
   - Scopes: `openid`, `email`, `profile`
3. Save the Application ID and Secret

#### 3. Ensure Secrets Exist in GCP Secret Manager

The following secrets must exist in GCP Secret Manager with `secretAccessor` role granted to `gkg-secrets-sa`:

| Secret Name | Value |
|-------------|-------|
| `grafana-oauth-client-id` | Google OAuth Client ID |
| `grafana-oauth-client-secret` | Google OAuth Client Secret |
| `grafana-gitlab-oauth-client-id` | GitLab Application ID |
| `grafana-gitlab-oauth-client-secret` | GitLab Application Secret |

### DNS Setup

DNS for `gkg.dev` is managed externally. Ensure an A record for `grafana.gkg.dev` points to the GKE Ingress load balancer IP:

```shell
# Get the load balancer IP after deploying the Helm chart
kubectl get ingress grafana -n gkg -o jsonpath='{.status.loadBalancer.ingress[0].ip}'
```

The GCP ManagedCertificate will automatically provision a TLS certificate once DNS propagates (10-30 minutes).

## Deploy Helm Charts

The deployment consists of two charts:

- `helm-dev/observability` - Prometheus, Grafana, Loki, Alloy (release name: `gkg-obs`)
- `helm-dev/gkg` - GKG indexer, webserver, siphon, NATS (release name: `gkg`)

### Install

```shell
# Build dependencies
helm dependency build ./helm-dev/observability
helm dependency build ./helm-dev/gkg

# Deploy observability first (provides OTEL endpoint for gkg)
helm install gkg-obs ./helm-dev/observability \
  -f ./helm-dev/observability/values-sandbox.yaml \
  --namespace gkg \
  --create-namespace \
  --history-max 1 \
  --wait

# Deploy gkg
helm install gkg ./helm-dev/gkg \
  -f ./helm-dev/gkg/values-sandbox.yaml \
  --namespace gkg \
  --history-max 1 \
  --wait
```

### Upgrade

```shell
helm upgrade gkg-obs ./helm-dev/observability \
  -f ./helm-dev/observability/values-sandbox.yaml \
  --namespace gkg \
  --history-max 1

helm upgrade gkg ./helm-dev/gkg \
  -f ./helm-dev/gkg/values-sandbox.yaml \
  --namespace gkg \
  --history-max 1
```

### Check Status

```shell
# Pods
kubectl get pods -n gkg

# External secrets sync status
kubectl get externalsecrets -n gkg

# Helm releases
helm list -n gkg
```

### View Logs

```shell
# Producer
kubectl logs -n gkg deployment/siphon-producer -f

# Consumer
kubectl logs -n gkg deployment/siphon-consumer -f

# NATS
kubectl logs -n gkg statefulset/gkg-nats -f

# GitLab Runner
kubectl logs -n gkg deployment/gkg-gitlab-runner -f
```

## Observability Stack

The Helm chart deploys Grafana, Prometheus, Loki, and Alertmanager.

### Access Grafana

URL: https://grafana.gkg.dev

Authentication is via Google or GitLab OAuth. Users with `@gitlab.com` emails who are members of `gitlab-com` group get access. Administrators are configured in `values-sandbox.yaml`.

### Check Observability Pods

```shell
kubectl get pods -n gkg -l "app.kubernetes.io/name in (grafana,prometheus,loki,alertmanager)"
```

### Data Sources

Grafana has two pre-configured data sources:

- **Prometheus**: Metrics at `http://gkg-obs-kube-prometheus-st-prometheus:9090`
- **Loki**: Logs at `http://gkg-obs-loki:3100`

### Certificate Status

The Grafana ingress uses a GCP ManagedCertificate for TLS:

```shell
kubectl get managedcertificate grafana-cert -n gkg -o yaml
```

Certificate provisioning requires DNS to be correctly configured and can take 10-30 minutes.

## GitLab Runner

The Helm chart includes a GitLab Runner (via subchart) for running CI jobs on gitlab.gkg.dev.

### Initial Setup

1. Create an instance runner in GitLab Admin → CI/CD → Runners → New instance runner
2. Enable "Run untagged jobs"
3. Click "Create runner" and copy the `glrt-` token
4. Store in GCP Secret Manager:

   ```shell
   echo -n "glrt-YOUR_TOKEN" | gcloud secrets versions add runner_authentication_token \
     --project=gl-knowledgegraph-prj-f2eec59d --data-file=-
   ```

5. Deploy/upgrade the Helm chart

### Rotate Runner Token

If the runner becomes unhealthy or is removed from GitLab:

1. Delete the old runner in GitLab Admin → CI/CD → Runners
2. Create a new instance runner and get the new token
3. Update the secret:

   ```shell
   echo -n "glrt-NEW_TOKEN" | gcloud secrets versions add runner_authentication_token \
     --project=gl-knowledgegraph-prj-f2eec59d --data-file=-
   ```

4. Force sync and restart:

   ```shell
   kubectl annotate externalsecret gitlab-runner-token -n gkg force-sync=$(date +%s) --overwrite
   kubectl rollout restart deployment/gkg-gitlab-runner -n gkg
   ```

### Check Runner Status

```shell
# Runner pod
kubectl get pods -n gkg -l app=gitlab-runner

# Runner logs
kubectl logs -n gkg deployment/gkg-gitlab-runner --tail=50

# Job pods (created during CI runs)
kubectl get pods -n gkg | grep runner-
```

## Alert Response

Alert rules are deployed via the `PrometheusRule` CRD in the observability Helm chart.
Check active alerts in Grafana (Alerting → Alert rules) or query Alertmanager directly:

```shell
kubectl port-forward -n gkg svc/gkg-obs-kube-prometheus-st-alertmanager 9093:9093
# then open http://localhost:9093
```

### Security Alerts

#### GKGAuthFilterMissing (Critical)

A query was processed without a valid security context — authorization filtering was bypassed.

1. Check which pod emitted the metric: search Loki for `auth_filter_missing` or `security context` errors
2. Determine whether the query returned data (check ClickHouse query log for the correlation ID)
3. If data was returned without authorization, assess the scope of exposure
4. Investigate the caller: check the request's JWT claims, source IP, and gRPC metadata
5. Escalate to the security team if unauthorized data access is confirmed

#### GKGPipelineInvariantViolated (Critical)

The query compiler reached a state that upstream validation should have prevented — the generated SQL may be incorrect or unsafe.

1. Check the `reason` label (`lowering` or `codegen`) to identify the compiler stage
2. Search Loki for the correlation ID to find the full query JSON and error details
3. Reproduce with the query JSON against a local instance to confirm the bug
4. If the invariant violation could produce unsafe SQL (missing WHERE clauses, wrong joins), consider temporarily blocking the query pattern at the validation layer
5. File a bug with the query JSON and compiler error attached

#### GKGSecurityRejected (Warning)

The pipeline rejected a request because the security context was invalid or missing.

1. Check whether the rejections come from a single caller or are widespread
2. If single caller: likely a misconfigured integration — check their JWT token validity
3. If widespread: check whether the JWT signing key was rotated without updating GKG config
4. Review the `reason` label in Prometheus for specifics

### Query Health Alerts

#### GKGQueryingErrorRateHigh (Warning)

More than 5% of all queries are failing across all error categories.

1. This is the aggregate availability signal — check which individual alerts are also firing
2. Break down by `status` label in Prometheus: `rate(qp_queries_total_total{status!="ok"}[5m])` grouped by `status`
3. If a single status dominates, follow the corresponding alert's runbook entry
4. If errors are spread across multiple statuses, check for a shared root cause (network partition, resource exhaustion, deployment rollout)
5. If no individual alert is firing, the errors are distributed below each threshold — investigate the long tail

#### GKGQueryTimeoutRateHigh (Warning)

More than 5% of queries are timing out.

1. Check which query types are timing out (use `query_type` label on `qp.pipeline_duration_ms`)
2. Check ClickHouse system tables for slow queries: `SELECT * FROM system.query_log WHERE query_duration_ms > 5000 ORDER BY event_time DESC LIMIT 20`
3. Look for pathological query patterns (large traversals, unselective filters)
4. Check ClickHouse resource utilization (CPU, memory, merge queue depth)
5. If ClickHouse is healthy, the timeout deadline may need adjustment

#### GKGValidationFailedBurst (Warning)

Sustained burst of structural validation failures.

1. Check the `reason` label breakdown (parse, schema, reference, pagination)
2. If `parse`: a client is sending malformed JSON — identify the caller
3. If `schema`/`reference`: a client is using outdated entity names — check whether the ontology was recently updated
4. If distributed across reasons: possible probing or fuzzing attempt — check source IPs

#### GKGAllowlistRejectedBurst (Warning)

Sustained rate of ontology allowlist rejections.

1. Check the `reason` label (ontology vs ontology_internal)
2. If `ontology`: a client is requesting entities/columns not in the ontology — likely schema drift after an ontology update
3. If `ontology_internal`: a bug in the ontology loader — check recent ontology YAML changes
4. Notify affected consumers if an ontology change removed previously valid entities

#### GKGExecutionFailureRate (Warning)

ClickHouse query execution is failing.

1. Check ClickHouse health: `SELECT * FROM system.errors ORDER BY last_error_time DESC LIMIT 10`
2. Check for resource exhaustion: `SELECT * FROM system.metrics WHERE metric LIKE '%Memory%'`
3. Check if the GKG role's quotas were hit (`max_memory_usage`, `max_rows_to_read`)
4. Review recent schema migrations — a missing table or column will cause execution failures
5. If ClickHouse is unreachable, check network connectivity and DNS resolution

#### GKGAuthorizationFailureRate (Warning)

The redaction exchange with Rails is failing.

1. Check Rails gRPC endpoint health from the GKG pod: `grpcurl -plaintext <rails-host>:<port> grpc.health.v1.Health/Check`
2. Check for certificate expiry if TLS is enabled
3. Check Loki for `RedactionExchangeError` details (timeout vs connection refused vs protocol error)
4. If Rails is healthy but GKG can't reach it, check NetworkPolicy and DNS resolution
5. Sustained failures mean query results are not being redacted — assess whether to pause the query service

#### GKGPipelineLatencyP95High (Warning)

p95 end-to-end pipeline latency exceeds threshold.

1. Break down by stage using Grafana: compile, execute, authorization, hydration durations
2. If `execute` is the bottleneck: ClickHouse performance issue (see GKGExecutionFailureRate)
3. If `authorization` is the bottleneck: Rails latency issue (see GKGAuthorizationFailureRate)
4. If `compile` is the bottleneck: unusually complex queries — check recent query patterns
5. If `hydration` is the bottleneck: check neighbor property lookups and ClickHouse read performance

### Capacity Alerts

#### GKGRateLimitedHigh (Warning)

High rate of throttled callers.

1. Check whether a single consumer is responsible (check caller labels if available)
2. If legitimate traffic growth: scale the webserver deployment or increase rate limits
3. If a traffic spike from one consumer: contact the consumer team
4. Review whether the rate limit configuration matches the current capacity

## Secrets

### GCP Secret Manager

| Secret Name | Purpose |
|-------------|---------|
| `postgres-password` | Password for PostgreSQL `gitlab` user |
| `clickhouse-password` | Password for ClickHouse `default` user |
| `runner_authentication_token` | GitLab Runner token (glrt-) |
| `grafana-oauth-client-id` | Google OAuth client ID |
| `grafana-oauth-client-secret` | Google OAuth client secret |
| `grafana-gitlab-oauth-client-id` | GitLab OAuth application ID |
| `grafana-gitlab-oauth-client-secret` | GitLab OAuth application secret |

### Kubernetes Secrets (synced via External Secrets)

| K8s Secret | Source | Used By |
|------------|--------|---------|
| `postgres-credentials` | `postgres-password` | siphon-producer |
| `clickhouse-credentials` | `clickhouse-password` | siphon-consumer |
| `gitlab-runner-token` | `runner_authentication_token` | gitlab-runner |
| `grafana-oauth-credentials` | grafana-oauth-* secrets | grafana |

### Update a Secret

```shell
# Update in GCP Secret Manager
echo -n "new-password" | gcloud secrets versions add postgres-password --data-file=-

# Force refresh in Kubernetes (or wait for refresh interval)
kubectl annotate externalsecret postgres-credentials -n gkg force-sync=$(date +%s) --overwrite
```

## Teardown

### Remove Helm Release

```shell
helm uninstall gkg --namespace gkg
helm uninstall gkg-obs --namespace gkg
```

### Remove Namespace (including PVCs)

```shell
kubectl delete namespace gkg
```

### Remove External Secrets Operator

```shell
helm uninstall external-secrets --namespace external-secrets
kubectl delete namespace external-secrets
```

### Remove GCP Service Account

```shell
gcloud iam service-accounts delete \
  gkg-secrets-sa@gl-knowledgegraph-prj-f2eec59d.iam.gserviceaccount.com \
  --project=gl-knowledgegraph-prj-f2eec59d
```

## Reset Siphon Replication

Full re-snapshot of all PostgreSQL tables. Deletes existing ClickHouse siphon data.

### 1. Scale down siphon

```shell
kubectl -n gkg scale deployment siphon-producer --replicas=0
kubectl -n gkg scale deployment siphon-consumer --replicas=0
```

### 2. Drop replication slot and publication

In PostgreSQL (`sudo gitlab-psql` on vm-gitlab-omnibus):

```sql
SELECT pg_drop_replication_slot('gkg_slot');
DROP PUBLICATION IF EXISTS gkg_publication;
```

### 3. Reset NATS stream

```shell
kubectl -n gkg delete pod gkg-nats-0
kubectl -n gkg delete pvc gkg-nats-js-gkg-nats-0
kubectl -n gkg wait --for=condition=ready pod/gkg-nats-0 --timeout=120s
```

### 4. Truncate ClickHouse tables

In ClickHouse (`clickhouse-client --password=...` on vm-clickhouse):

**Siphon tables (datalake):**

```sql
SELECT 'TRUNCATE TABLE ' || database || '.' || name || ';'
FROM system.tables
WHERE database = 'gitlab_clickhouse_main_production' AND name LIKE 'siphon_%';
```

**Graph tables (if re-indexing):**

```sql
SELECT 'TRUNCATE TABLE ' || database || '.' || name || ';'
FROM system.tables
WHERE database = 'gkg-sandbox';
```

Run the generated statements, or use `--multiquery` to pipe them back.

### 5. Redeploy

```shell
helm upgrade gkg ./helm-dev/gkg -n gkg -f ./helm-dev/gkg/values-sandbox.yaml
```

### 6. Verify snapshots

```shell
kubectl -n gkg logs deployment/siphon-producer --tail=50 | grep "snapshot complete"
```

Producer recreates the slot, publication, and snapshots all configured tables.

## Trigger Dispatcher Manually

Run the dispatcher to dispatch indexing requests to the indexer. The dispatcher CronJob is disabled by default in sandbox.

```shell
# Delete previous job if exists
kubectl -n gkg delete job gkg-dispatcher-manual 2>/dev/null

# Create and run dispatcher job
kubectl -n gkg apply -f - <<'EOF'
apiVersion: batch/v1
kind: Job
metadata:
  name: gkg-dispatcher-manual
spec:
  backoffLimit: 1
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: gkg-dispatcher
          image: registry.gitlab.com/gitlab-org/orbit/knowledge-graph/gkg:develop
          imagePullPolicy: Always
          args:
            - "--mode=dispatch-indexing"
          env:
            - name: LOG_FORMAT
              value: "json"
            - name: NATS_URL
              value: "gkg-nats:4222"
            - name: DATALAKE_CLICKHOUSE_URL
              value: "http://10.128.0.13:8123"
            - name: DATALAKE_CLICKHOUSE_DATABASE
              value: "gitlab_clickhouse_main_production"
            - name: DATALAKE_CLICKHOUSE_USERNAME
              value: "default"
            - name: DATALAKE_CLICKHOUSE_PASSWORD
              valueFrom:
                secretKeyRef:
                  key: password
                  name: clickhouse-credentials
EOF

# Wait for completion and check logs
kubectl -n gkg wait --for=condition=complete job/gkg-dispatcher-manual --timeout=120s
kubectl -n gkg logs job/gkg-dispatcher-manual
```
