#!/usr/bin/env bash
# Deploy the GitLab Operator and apply an Orbit CR.
# Runs after setup.sh (namespaces exist, infra is up).
# Uses the operator image built by build-operator-image.sh.
#
# Temporary: goes away when the Orbit CRD ships in the published operator.

set -eo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

NS="e2e-${E2E_SHA}-gkg"
OPERATOR_REPO="${E2E_OPERATOR_REPO:-https://gitlab.com/gitlab-org/cloud-native/gitlab-operator.git}"
OPERATOR_BRANCH="${E2E_OPERATOR_BRANCH:-michaelusa/spike-orbit-crd}"

# --- 1. CRDs from the operator repo ---
log "Applying CRDs from operator repo"
CRD_DIR=$(mktemp -d)
git clone --depth 1 --branch "${OPERATOR_BRANCH}" --filter=blob:none --sparse \
  "${OPERATOR_REPO}" "${CRD_DIR}" 2>/dev/null
(cd "${CRD_DIR}" && git sparse-checkout set config/crd/bases 2>/dev/null)
$KC apply -f "${CRD_DIR}/config/crd/bases/"
rm -rf "${CRD_DIR}"

for crd in orbits.apps.gitlab.com gitlabs.apps.gitlab.com; do
  $KC wait --for=condition=Established crd/"$crd" --timeout=60s 2>/dev/null || true
done

# --- 2. Operator Deployment (inline, matching the green run approach) ---
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
log "Waiting 30s for operator manager to start..."
sleep 30

# --- 3. Orbit CR ---
log "Applying Orbit CR from e2e/orbit-cr.yaml"

V_FILE="${E2E_DIR}/config/versions.yaml"
GKG_CHART=$(awk '/^gkg:/{found=1} found && /chart:/{print $2; exit}' "$V_FILE" | tr -d '"')
GKG_IMAGE="${E2E_GKG_IMAGE:-$(awk '/^gkg:/{found=1} found && /repository:/{print $2; exit}' "$V_FILE")}"
GKG_TAG="${E2E_GKG_TAG:-$(awk '/^gkg:/{found=1} found && /tag:/{print $2; exit}' "$V_FILE" | tr -d '"')}"
CLICKHOUSE_NS="e2e-${E2E_SHA}-clickhouse"
NATS_NS="e2e-${E2E_SHA}-nats"
GITLAB_NS="e2e-${E2E_SHA}-gitlab"

sed -e "s|\${NS}|${NS}|g" \
    -e "s|\${GKG_CHART}|${GKG_CHART}|g" \
    -e "s|\${GKG_IMAGE}|${GKG_IMAGE}|g" \
    -e "s|\${GKG_TAG}|${GKG_TAG}|g" \
    -e "s|\${CLICKHOUSE_NS}|${CLICKHOUSE_NS}|g" \
    -e "s|\${NATS_NS}|${NATS_NS}|g" \
    -e "s|\${GITLAB_NS}|${GITLAB_NS}|g" \
    "${E2E_DIR}/orbit-cr.yaml" | $KC apply -n "$NS" -f -

# --- 4. Wait for GKG to come up ---
log "Waiting for GKG Deployments..."
for i in $(seq 1 60); do
  READY=$($KC get deploy -n "$NS" -l app.kubernetes.io/managed-by=gitlab-operator-orbit \
    -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)
  if [ -n "$READY" ]; then
    log "GKG Deployments: $READY"
    break
  fi
  echo "  ($i/60)"
  sleep 10
done
if [ -z "$READY" ]; then
  log "ERROR: No GKG Deployments after 10 minutes"
  $KC logs deploy/gitlab-operator -n "$NS" --tail=50 || true
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
