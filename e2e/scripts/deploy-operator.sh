#!/usr/bin/env bash
# Deploy the GitLab Operator and apply an Orbit CR.
# Runs after setup.sh (namespaces exist, infra is up).
# Uses the operator image built by build-operator-image.sh.
#
# Everything except the Orbit CR comes from the operator repo's own
# deploy chart (deploy/chart/). No hand-written RBAC or Deployment.
#
# Temporary: goes away when the Orbit CRD ships in the published operator.

set -eo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

NS="e2e-${E2E_SHA}-gkg"
OPERATOR_REPO="${E2E_OPERATOR_REPO:-https://gitlab.com/gitlab-org/cloud-native/gitlab-operator.git}"
OPERATOR_BRANCH="${E2E_OPERATOR_BRANCH:-michaelusa/spike-orbit-crd}"

# --- 1. Clone operator repo (deploy chart + CRDs) ---
log "Cloning operator deploy chart"
OPERATOR_DIR=$(mktemp -d)
trap 'rm -rf "$OPERATOR_DIR"' EXIT
git clone --depth 1 --branch "${OPERATOR_BRANCH}" --filter=blob:none --sparse \
  "${OPERATOR_REPO}" "${OPERATOR_DIR}" 2>/dev/null
(cd "${OPERATOR_DIR}" && git sparse-checkout set deploy/chart 2>/dev/null)

# --- 2. Deploy operator via its own Helm chart ---
log "Rendering and applying operator manifests"
(cd "${OPERATOR_DIR}/deploy/chart" && helm dependency build 2>/dev/null)

# helm template renders the full deployment (RBAC, ServiceAccount, Deployment,
# webhook, cert-manager resources). kubectl apply installs everything including
# the CRDs from the chart's crds/ directory.
helm template gitlab-operator "${OPERATOR_DIR}/deploy/chart" \
  --namespace "$NS" \
  --include-crds \
  --set cert-manager.install=false \
  --set manager.image.repository="${E2E_OPERATOR_IMAGE}" \
  --set manager.image.tag="${E2E_OPERATOR_TAG}" \
  --set manager.resources.requests.cpu=50m \
  --set manager.resources.requests.memory=128Mi \
  --set manager.resources.limits.cpu=250m \
  --set manager.resources.limits.memory=256Mi \
  | $KC apply -n "$NS" -f -

# Wait for CRDs to be established.
for crd in orbits.apps.gitlab.com gitlabs.apps.gitlab.com; do
  $KC wait --for=condition=Established crd/"$crd" --timeout=60s 2>/dev/null || true
done

$KC rollout status deploy/gitlab-controller-manager -n "$NS" --timeout=120s

rm -rf "$OPERATOR_DIR"
trap - EXIT

# --- 3. Orbit CR (the one file a customer writes) ---
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
for i in $(seq 1 30); do
  READY=$($KC get deploy -n "$NS" -l app.kubernetes.io/managed-by=gitlab-operator-orbit \
    -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)
  if [ -n "$READY" ]; then
    log "GKG Deployments: $READY"
    break
  fi
  echo "  ($i/30)"
  sleep 10
done
if [ -z "$READY" ]; then
  log "ERROR: No GKG Deployments after 5 minutes"
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
