#!/usr/bin/env bash
# Deploy the GitLab Operator with the Orbit CRD and apply an Orbit CR.
# Runs after setup.sh (namespaces exist, infra is up).
# Uses the operator image built by build-operator-image.sh.
#
# This replaces the standalone `helm install gkg` with:
#   1. kubectl apply CRDs from the operator repo
#   2. kubectl apply operator Deployment + RBAC
#   3. kubectl apply Orbit CR
#
# Temporary: goes away when the Orbit CRD ships in the published operator.

set -eo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

NS="e2e-${E2E_SHA}-gkg"

# --- 1. CRDs ---
log "Applying Orbit and GitLab CRDs"
OPERATOR_REPO="${E2E_OPERATOR_REPO:-https://gitlab.com/gitlab-org/cloud-native/gitlab-operator.git}"
OPERATOR_BRANCH="${E2E_OPERATOR_BRANCH:-michaelusa/spike-orbit-crd}"
CRD_DIR=$(mktemp -d)
git clone --depth 1 --branch "${OPERATOR_BRANCH}" --filter=blob:none --sparse "${OPERATOR_REPO}" "${CRD_DIR}" 2>/dev/null
(cd "${CRD_DIR}" && git sparse-checkout set config/crd/bases 2>/dev/null)
$KC apply -f "${CRD_DIR}/config/crd/bases/"
rm -rf "${CRD_DIR}"

# Wait for CRDs to be established.
for crd in orbits.apps.gitlab.com gitlabs.apps.gitlab.com; do
  $KC wait --for=condition=Established crd/"$crd" --timeout=30s
done

# --- 2. Operator Deployment ---
log "Deploying operator"
$KC apply -n "$NS" -f - <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: gitlab-operator
  namespace: ${NS}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: gitlab-operator-e2e-admin
subjects:
  - kind: ServiceAccount
    name: gitlab-operator
    namespace: ${NS}
roleRef:
  kind: ClusterRole
  name: cluster-admin
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gitlab-operator
  namespace: ${NS}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: gitlab-operator
  template:
    metadata:
      labels:
        app: gitlab-operator
    spec:
      serviceAccountName: gitlab-operator
      initContainers:
        - name: webhook-cert
          image: alpine/openssl:latest
          command: ["sh", "-c", "openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -keyout /certs/tls.key -out /certs/tls.crt -days 1 -nodes -subj /CN=webhook && chmod 644 /certs/tls.key /certs/tls.crt"]
          volumeMounts:
            - name: webhook-certs
              mountPath: /certs
      containers:
        - name: manager
          image: "${E2E_OPERATOR_IMAGE}:${E2E_OPERATOR_TAG}"
          args: ["--enable-leader-election=false", "--metrics-secure=false"]
          env:
            - name: WATCH_NAMESPACE
              value: "${NS}"
            - name: HELM_CHARTS
              value: /charts
          resources:
            requests: { cpu: 50m, memory: 128Mi }
            limits: { cpu: 250m, memory: 256Mi }
          volumeMounts:
            - name: webhook-certs
              mountPath: /tmp/k8s-webhook-server/serving-certs
              readOnly: true
      volumes:
        - name: webhook-certs
          emptyDir: {}
EOF

$KC rollout status deploy/gitlab-operator -n "$NS" --timeout=120s

# --- 3. Orbit CR ---
log "Applying Orbit CR"

# Read versions from the same source as helmfile.
V_FILE="${E2E_DIR}/config/versions.yaml"
GKG_CHART=$(python3 -c "import yaml; print(yaml.safe_load(open('${V_FILE}'))['gkg']['chart'])")
GKG_IMAGE="${E2E_GKG_IMAGE:-$(python3 -c "import yaml; print(yaml.safe_load(open('${V_FILE}'))['gkg']['image']['repository'])")}"
GKG_TAG="${E2E_GKG_TAG:-$(python3 -c "import yaml; print(yaml.safe_load(open('${V_FILE}'))['gkg']['image']['tag'])")}"

$KC apply -n "$NS" -f - <<EOF
apiVersion: apps.gitlab.com/v1beta1
kind: Orbit
metadata:
  name: orbit
  namespace: ${NS}
spec:
  gkg:
    version: "${GKG_CHART}"
    values:
      extraResources:
        - apiVersion: cert-manager.io/v1
          kind: Certificate
          metadata:
            name: gkg-grpc-tls
          spec:
            secretName: gkg-grpc-tls
            duration: 8760h
            issuerRef:
              name: e2e-ca
              kind: ClusterIssuer
            dnsNames:
              - orbit-gkg-webserver.${NS}.svc.cluster.local
      image:
        repository: "${GKG_IMAGE}"
        tag: "${GKG_TAG}"
        pullPolicy: IfNotPresent
      secrets:
        perKey:
          gitlabJwtVerifyingKey: { secretName: gkg-secrets }
          gitlabJwtSigningKey: { secretName: gkg-secrets }
          datalakePassword: { secretName: gkg-secrets }
          graphPassword: { secretName: gkg-secrets }
          graphReadPassword: { secretName: gkg-secrets }
      webserver:
        replicas: 1
        logLevel: info
        probes: { enabled: true }
        resources:
          requests: { cpu: 250m, memory: 512Mi }
          limits: { cpu: "1", memory: 1Gi }
      indexer:
        replicas: 1
        logLevel: info
        probes: { enabled: true }
        tmpSizeLimit: 5Gi
        resources:
          requests: { cpu: 250m, memory: 512Mi }
          limits: { cpu: "1", memory: 1Gi }
      healthCheck:
        replicas: 1
        logLevel: info
        probes: { enabled: true }
        targets:
          - deployments:
              - orbit-gkg-indexer-default
              - orbit-gkg-webserver
              - orbit-gkg-dispatcher
      clickhouse:
        datalake:
          host: clickhouse.e2e-${E2E_SHA}-clickhouse.svc.cluster.local
          httpPort: 8123
          database: datalake
          user: gkg_siphon_reader
          ssl: false
        graph:
          host: clickhouse.e2e-${E2E_SHA}-clickhouse.svc.cluster.local
          httpPort: 8123
          database: gkg
          user: gkg_writer
          readUser: gkg_reader
          ssl: false
      nats:
        url: "nats://nats.e2e-${E2E_SHA}-nats.svc.cluster.local:4222"
        consumerName: gkg-indexer
        fetchExpiresSecs: 5
        tls: { enabled: false }
      gitlab:
        baseUrl: "http://gitlab-webservice-default.e2e-${E2E_SHA}-gitlab.svc.cluster.local:8181"
      tls:
        enabled: true
        existingSecret: gkg-grpc-tls
      schedule:
        tasks:
          siphon: { events_stream_name: e2e_siphon_event_stream }
          global: { cron: "*/2 * * * * *" }
          namespace: { cron: "*/2 * * * * *", sweep_interval_secs: 10 }
          code-backfill: { cron: "*/5 * * * * *" }
          table-cleanup: { cron: "0 0 3 * * *" }
          namespace-deletion: { cron: "0 0 3 * * *" }
          stale-edge-reconciliation: { cron: "*/10 * * * * *" }
          migration-completion: { cron: "*/10 * * * * *" }
      engine:
        max_concurrent_workers: 13
        concurrency_groups: { sdlc: 10, code: 3 }
        topics:
          global-handler: { concurrency_group: sdlc, max_attempts: 1, retry_interval_secs: 60 }
          namespace-handler: { concurrency_group: sdlc, max_attempts: 1, retry_interval_secs: 60 }
          code-indexing-task: { concurrency_group: code, max_attempts: 3, retry_interval_secs: 60 }
        handlers:
          code-indexing-task:
            pipeline: { write_min_flush_rows: 1, write_buffer_age_secs: 2, write_max_flush_age_secs: 5 }
EOF

# --- 4. Wait for GKG to be healthy ---
log "Waiting for GKG Deployments..."
for i in $(seq 1 30); do
  READY=$(kubectl -n "$NS" get deploy -l app.kubernetes.io/managed-by=gitlab-operator-orbit -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)
  if [ -n "$READY" ]; then
    log "GKG Deployments found: $READY"
    break
  fi
  echo "  ($i/30)"
  sleep 10
done
if [ -z "$READY" ]; then
  log "ERROR: No GKG Deployments after 5 minutes"
  $KC logs deploy/gitlab-operator -n "$NS" || true
  exit 1
fi

$KC rollout status deploy/orbit-gkg-webserver -n "$NS" --timeout=300s || true
$KC rollout status deploy/orbit-gkg-dispatcher -n "$NS" --timeout=120s || true
$KC rollout status deploy/orbit-gkg-indexer-default -n "$NS" --timeout=120s || true

log "Waiting for /ready..."
for i in $(seq 1 60); do
  HEALTH=$($KC get --raw "/api/v1/namespaces/${NS}/services/orbit-gkg-webserver:8080/proxy/ready" 2>/dev/null || echo '{}')
  if echo "$HEALTH" | grep -q '"status":"ok"'; then
    log "GKG healthy: $HEALTH"
    break
  fi
  sleep 5
done

log "Waiting 120s for dispatcher schema init..."
sleep 120

log "Operator deployment complete"
$KC get pods -n "$NS"
