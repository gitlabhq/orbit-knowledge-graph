#!/usr/bin/env bash
# Deploy the GitLab Operator and apply an Orbit CR.
# Runs after setup.sh (namespaces exist, infra is up).
# Uses the operator image built by build-operator-image.sh.
#
# Deploys via the operator's own deploy chart (deploy/chart/) with no
# inline YAML or hand-written RBAC. The chart handles CRDs, RBAC,
# webhook certs, and the operator Deployment.
#
# Temporary: goes away when the Orbit CRD ships in the published operator.

set -eo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

NS="e2e-${E2E_SHA}-gkg"
OPERATOR_REPO="${E2E_OPERATOR_REPO:-https://gitlab.com/gitlab-org/cloud-native/gitlab-operator.git}"
OPERATOR_BRANCH="${E2E_OPERATOR_BRANCH:-michaelusa/spike-orbit-crd}"

# Uninstall any previous operator release (its cluster-scoped resources
# survive namespace teardown and block helm install with ownership errors).
helm uninstall gitlab-operator -n "$NS" --kube-context "$KCTX" 2>/dev/null || true

# --- 1. Clone operator repo ---
log "Cloning operator repo"
OPERATOR_DIR=$(mktemp -d)
trap 'rm -rf "$OPERATOR_DIR"' EXIT
git clone --depth 1 --branch "${OPERATOR_BRANCH}" --filter=blob:none --sparse \
  "${OPERATOR_REPO}" "${OPERATOR_DIR}" 2>/dev/null
(cd "${OPERATOR_DIR}" && git sparse-checkout set deploy/chart config/crd/bases 2>/dev/null)

# Apply CRDs via kubectl (the agent SA has permissions; Helm's CRD install
# path uses a different API call that the agent may not support).
log "Applying CRDs"
$KC apply -f "${OPERATOR_DIR}/config/crd/bases/"
for crd in orbits.apps.gitlab.com gitlabs.apps.gitlab.com; do
  $KC wait --for=condition=Established crd/"$crd" --timeout=60s 2>/dev/null || true
done

# --- 2. Deploy operator via its own Helm chart ---
log "Installing operator via deploy chart"
(cd "${OPERATOR_DIR}/deploy/chart" && helm dependency build 2>/dev/null)

helm install gitlab-operator "${OPERATOR_DIR}/deploy/chart" \
  --skip-crds \
  --namespace "$NS" \
  --set watchCluster=false \
  --set manager.leaderElection.enabled=false \
  --set image.registry="registry.gitlab.com" \
  --set image.repository="gitlab-org/orbit/knowledge-graph" \
  --set image.name="operator-spike" \
  --set image.tag="${E2E_OPERATOR_TAG}" \
  --set resources.requests.cpu=50m \
  --set resources.requests.memory=128Mi \
  --set resources.limits.cpu=250m \
  --set resources.limits.memory=256Mi \
  --wait --timeout=120s \
  --kube-context "$KCTX"

rm -rf "$OPERATOR_DIR"
trap - EXIT

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
  $KC logs deploy/gitlab-controller-manager -n "$NS" --tail=50 || true
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
