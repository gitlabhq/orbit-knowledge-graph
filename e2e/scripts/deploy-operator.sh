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
# Render the operator manifests, then patch for e2e:
# - Disable leader election (single replica, no lease RBAC)
# - Replace the webhook cert volume with an emptyDir + init container
#   (the deploy chart expects cert-manager to provision webhook-server-cert)
helm template gitlab-operator "${OPERATOR_DIR}/deploy/chart" \
  --namespace "$NS" \
  --include-crds \
  --set cert-manager.install=false \
  --set image.registry="registry.gitlab.com" \
  --set image.repository="gitlab-org/orbit/knowledge-graph" \
  --set image.name="operator-spike" \
  --set image.tag="${E2E_OPERATOR_TAG}" \
  --set resources.requests.cpu=50m \
  --set resources.requests.memory=128Mi \
  --set resources.limits.cpu=250m \
  --set resources.limits.memory=256Mi \
  | sed 's/--enable-leader-election/--enable-leader-election=false/' \
  > /tmp/operator-manifests.yaml

# The deploy chart's RBAC does not cover Orbit resources. Create the
# cluster-admin binding and webhook cert BEFORE applying the manifests
# so the operator pod has permissions from the moment it starts.
$KC create clusterrolebinding gitlab-operator-e2e-admin \
  --clusterrole=cluster-admin \
  --serviceaccount="${NS}:gitlab-manager" 2>/dev/null || true

# The deploy chart mounts webhook-server-cert from a Secret that cert-manager
# creates. We do not run the cert-manager issuer, so create a self-signed cert.
CERT_DIR=$(mktemp -d)
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
  -keyout "${CERT_DIR}/tls.key" -out "${CERT_DIR}/tls.crt" \
  -days 1 -nodes -subj "/CN=webhook" 2>/dev/null
$KC create secret tls webhook-server-cert -n "$NS" \
  --cert="${CERT_DIR}/tls.crt" --key="${CERT_DIR}/tls.key" 2>/dev/null || true
rm -rf "${CERT_DIR}"

# Now apply the operator manifests (RBAC and cert are already in place).
$KC apply -n "$NS" -f /tmp/operator-manifests.yaml
rm -f /tmp/operator-manifests.yaml

# Wait for CRDs to be established.
for crd in orbits.apps.gitlab.com gitlabs.apps.gitlab.com; do
  $KC wait --for=condition=Established crd/"$crd" --timeout=60s 2>/dev/null || true
done

$KC rollout status deploy/gitlab-controller-manager -n "$NS" --timeout=120s

# The rollout completes when the container starts, but the operator needs
# time to register controllers and start the manager (~15-30s).
log "Waiting 30s for operator manager to start..."
sleep 30

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
for i in $(seq 1 60); do
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
