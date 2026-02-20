#!/usr/bin/env bash
# =============================================================================
# setup.sh -- Full E2E environment setup: zero to running GKG stack
#
# This is the single "from scratch" script that:
#
#   Phase 1: Cluster + GitLab
#     1. Install Caproni CLI (build from source)
#     2. Start Colima with caproni profile (k3s, 12GiB, 4 cores)
#     3. Set DOCKER_HOST for colima-caproni
#     4. Pre-pull workhorse image into colima's docker daemon
#     5. Build 3 custom CNG images from the GitLab feature branch
#     6. Deploy Traefik ingress controller
#     7. Deploy GitLab via Helm chart (all custom images, PG logical replication)
#     8. Wait for all pods to be healthy
#
#   Phase 2: Post-deploy
#     9. Bridge PG credentials to default namespace (for Siphon)
#    10. Extract JWT secret from toolbox pod -> e2e/tilt/.secrets
#    11. Run Rails db:migrate
#    12. Enable :knowledge_graph feature flag
#    13. Copy test scripts into toolbox pod
#    14. Run create_test_data.rb to create users, groups, projects, etc.
#
#   Phase 3: GKG stack (optional, use --gkg flag)
#    15. Build GKG server image
#    16. Start Tilt (ClickHouse, NATS, Siphon, GKG indexer + webserver)
#
# Usage:
#   cd ~/Desktop/Code/gkg/e2e/caproni
#   ./setup.sh                  # Phase 1 + 2 only
#   ./setup.sh --gkg            # Phase 1 + 2 + 3 (starts Tilt)
#   ./setup.sh --skip-build     # Skip CNG image build (reuse existing)
#   ./setup.sh --skip-caproni   # Skip caproni install (already installed)
#   ./setup.sh --phase2-only    # Only run post-deploy steps (cluster exists)
#
# Prerequisites:
#   - macOS with Homebrew
#   - Docker CLI installed (via colima)
#   - Go 1.22+ (for building caproni from source)
#   - Rust + cargo (for GKG server, only needed with --gkg)
#   - GitLab source checkout at GITLAB_SRC (default: ~/Desktop/Code/gdk/gitlab)
#     on the feature branch
#   - Caproni CLI source at CAPRONI_SRC (default: ~/Desktop/Code/caproni-cli)
#
# Environment variables:
#   GITLAB_SRC      Path to GitLab Rails source (default: ~/Desktop/Code/gdk/gitlab)
#   CAPRONI_SRC     Path to caproni-cli source (default: ~/Desktop/Code/caproni-cli)
#   NO_CACHE        Set to 1 to force --no-cache on docker builds
#   COLIMA_PROFILE  Colima profile name (default: caproni)
#   COLIMA_MEMORY   Colima VM memory (default: 12)
#   COLIMA_CPUS     Colima VM CPUs (default: 4)
#
# =============================================================================
set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
GKG_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TILT_DIR="${GKG_ROOT}/e2e/tilt"

GITLAB_SRC="${GITLAB_SRC:-$HOME/Desktop/Code/gdk/gitlab}"
CAPRONI_SRC="${CAPRONI_SRC:-$HOME/Desktop/Code/caproni-cli}"

COLIMA_PROFILE="${COLIMA_PROFILE:-caproni}"
COLIMA_MEMORY="${COLIMA_MEMORY:-12}"
COLIMA_CPUS="${COLIMA_CPUS:-4}"

GITLAB_NS="gitlab"
DEFAULT_NS="default"

# CNG image settings
BASE_TAG="v18.8.1"
CNG_REGISTRY="registry.gitlab.com/gitlab-org/build/cng"
LOCAL_PREFIX="gkg-e2e"
LOCAL_TAG="local"
WORKHORSE_IMAGE="${CNG_REGISTRY}/gitlab-workhorse-ee:${BASE_TAG}"

# Parse flags
SKIP_BUILD=false
SKIP_CAPRONI=false
PHASE2_ONLY=false
START_GKG=false
for arg in "$@"; do
  case "$arg" in
    --skip-build)    SKIP_BUILD=true ;;
    --skip-caproni)  SKIP_CAPRONI=true ;;
    --phase2-only)   PHASE2_ONLY=true ;;
    --gkg)           START_GKG=true ;;
    --help|-h)
      head -60 "$0" | grep '^#' | sed 's/^# *//'
      exit 0
      ;;
  esac
done

# Log directory (gitignored)
LOG_DIR="${GKG_ROOT}/.dev"
mkdir -p "${LOG_DIR}"

# ── Helpers ───────────────────────────────────────────────────────────────────

log()  { echo ""; echo "=== $1 ==="; }
step() { echo "--- $1 ---"; }
warn() { echo "WARNING: $1"; }

ensure_docker_host() {
  export DOCKER_HOST="unix://${HOME}/.colima/${COLIMA_PROFILE}/docker.sock"
}

wait_for_pod() {
  local label="$1" ns="$2" timeout="${3:-300s}"
  step "Waiting for pod (${label}) in ${ns} (timeout ${timeout})"
  kubectl wait --for=condition=ready pod -l "${label}" -n "${ns}" --timeout="${timeout}" 2>/dev/null || {
    warn "Pod ${label} not ready after ${timeout}. Continuing..."
  }
}

# ── Validate prerequisites ────────────────────────────────────────────────────

if [ "${PHASE2_ONLY}" = false ]; then
  if [ "${SKIP_BUILD}" = false ] && [ ! -f "${GITLAB_SRC}/Gemfile" ]; then
    echo "ERROR: GitLab source not found at ${GITLAB_SRC}/Gemfile"
    echo "Set GITLAB_SRC to the path of your GitLab Rails checkout."
    exit 1
  fi

  if ! command -v colima &>/dev/null; then
    echo "ERROR: colima not found. Install with: brew install colima"
    exit 1
  fi
fi

echo ""
echo "================================================================"
echo "  GKG E2E Setup"
echo "================================================================"
echo "  GKG root:     ${GKG_ROOT}"
echo "  GitLab src:   ${GITLAB_SRC}"
echo "  Caproni src:  ${CAPRONI_SRC}"
echo "  Colima:       profile=${COLIMA_PROFILE} mem=${COLIMA_MEMORY}GiB cpus=${COLIMA_CPUS}"
echo "  Flags:        skip-build=${SKIP_BUILD} skip-caproni=${SKIP_CAPRONI} phase2-only=${PHASE2_ONLY} gkg=${START_GKG}"
echo "================================================================"

# ==============================================================================
# PHASE 1: Cluster + GitLab
# ==============================================================================

if [ "${PHASE2_ONLY}" = false ]; then

  # ── 1. Install Caproni CLI ──────────────────────────────────────────────────

  if [ "${SKIP_CAPRONI}" = false ]; then
    log "1. Installing Caproni CLI"

    if command -v caproni &>/dev/null; then
      step "Caproni already installed: $(caproni version 2>/dev/null || echo 'unknown')"
    else
      if [ ! -d "${CAPRONI_SRC}" ]; then
        step "Cloning caproni-cli source..."
        git clone https://gitlab.com/gitlab-com/gl-infra/sandbox/caproni-cli.git "${CAPRONI_SRC}"
      fi
      step "Building caproni from source..."
      (cd "${CAPRONI_SRC}" && go build -o caproni . && sudo mv caproni /usr/local/bin/caproni)
      step "Installed: $(caproni version 2>/dev/null || echo 'built')"
    fi
  fi

  # ── 2. Start Colima ────────────────────────────────────────────────────────

  log "2. Starting Colima (profile: ${COLIMA_PROFILE})"

  if colima status --profile "${COLIMA_PROFILE}" &>/dev/null; then
    step "Colima (${COLIMA_PROFILE}) already running"
  else
    step "Starting Colima with k3s, ${COLIMA_MEMORY}GiB RAM, ${COLIMA_CPUS} CPUs..."
    colima start \
      --profile "${COLIMA_PROFILE}" \
      --memory "${COLIMA_MEMORY}" \
      --cpu "${COLIMA_CPUS}" \
      --disk 60 \
      --vm-type vz \
      --kubernetes \
      --kubernetes-version v1.31.5+k3s1 \
      2>&1 | tee "${LOG_DIR}/colima-start.log"
  fi

  ensure_docker_host

  # Verify docker + kubectl work
  docker info >/dev/null 2>&1 || { echo "ERROR: docker not reachable via ${DOCKER_HOST}"; exit 1; }
  kubectl cluster-info >/dev/null 2>&1 || { echo "ERROR: kubectl cannot reach cluster"; exit 1; }
  step "Docker + kubectl connected"

  # ── 3. Pre-pull workhorse image ─────────────────────────────────────────────

  log "3. Pre-pulling workhorse image"

  if docker image inspect "${WORKHORSE_IMAGE}" &>/dev/null; then
    step "Workhorse image already present"
  else
    step "Pulling ${WORKHORSE_IMAGE}..."
    docker pull "${WORKHORSE_IMAGE}" 2>&1 | tail -3
  fi

  # ── 4. Build custom CNG images ─────────────────────────────────────────────

  if [ "${SKIP_BUILD}" = false ]; then
    log "4. Building custom CNG images"
    step "Source: ${GITLAB_SRC}"
    step "Base tag: ${BASE_TAG}"

    # The build-images.sh script handles staging, .dockerignore, etc.
    (cd "${SCRIPT_DIR}" && bash build-images.sh "${GITLAB_SRC}")
  else
    log "4. Skipping CNG image build (--skip-build)"
    step "Using existing images:"
    ensure_docker_host
    docker images "${LOCAL_PREFIX}/*" --format "  {{.Repository}}:{{.Tag}}  ({{.Size}})" 2>/dev/null || true
  fi

  # ── 5. Deploy Traefik ──────────────────────────────────────────────────────

  log "5. Deploying Traefik ingress controller"

  ensure_docker_host

  if helm status traefik -n kube-system &>/dev/null; then
    step "Traefik already deployed"
  else
    helm repo add traefik https://traefik.github.io/charts 2>/dev/null || true
    helm repo update traefik 2>/dev/null || true
    helm install traefik traefik/traefik \
      -n kube-system \
      -f "${SCRIPT_DIR}/traefik-values.yaml" \
      --wait --timeout 5m \
      2>&1 | tail -5
    step "Traefik deployed"
  fi

  # ── 6. Deploy GitLab via Helm ──────────────────────────────────────────────

  log "6. Deploying GitLab via Helm chart"

  ensure_docker_host

  helm repo add gitlab https://charts.gitlab.io 2>/dev/null || true
  helm repo update gitlab 2>/dev/null || true

  if helm status gitlab -n "${GITLAB_NS}" &>/dev/null; then
    step "GitLab already deployed, upgrading..."
    helm upgrade gitlab gitlab/gitlab \
      -n "${GITLAB_NS}" \
      -f "${SCRIPT_DIR}/gitlab-values.yaml" \
      --timeout 15m \
      2>&1 | tail -5
  else
    kubectl create namespace "${GITLAB_NS}" --dry-run=client -o yaml | kubectl apply -f -
    helm install gitlab gitlab/gitlab \
      -n "${GITLAB_NS}" \
      -f "${SCRIPT_DIR}/gitlab-values.yaml" \
      --timeout 15m \
      2>&1 | tail -5
  fi

  # ── 7. Wait for all pods ───────────────────────────────────────────────────

  log "7. Waiting for GitLab pods to be ready"

  ensure_docker_host

  wait_for_pod "app.kubernetes.io/name=postgresql" "${GITLAB_NS}" "600s"
  wait_for_pod "app=webservice"  "${GITLAB_NS}" "600s"
  wait_for_pod "app=sidekiq"     "${GITLAB_NS}" "600s"
  wait_for_pod "app=toolbox"     "${GITLAB_NS}" "300s"
  wait_for_pod "app=gitaly"      "${GITLAB_NS}" "300s"

  step "Pod status:"
  kubectl get pods -n "${GITLAB_NS}" --no-headers 2>/dev/null | while read -r line; do
    echo "  ${line}"
  done

fi  # end PHASE2_ONLY check

# ==============================================================================
# PHASE 2: Post-deploy
# ==============================================================================

log "PHASE 2: Post-deploy setup"

ensure_docker_host

# Get toolbox pod name
TOOLBOX_POD=$(kubectl get pod -n "${GITLAB_NS}" -l app=toolbox \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

if [ -z "${TOOLBOX_POD}" ]; then
  echo "ERROR: No toolbox pod found in ${GITLAB_NS} namespace."
  echo "Is GitLab deployed? Run without --phase2-only to deploy."
  exit 1
fi
step "Toolbox pod: ${TOOLBOX_POD}"

# ── 8. Bridge PG credentials to default namespace ────────────────────────────

log "8. Bridging PostgreSQL credentials"

PG_PASS=$(kubectl get secret -n "${GITLAB_NS}" gitlab-postgresql-password \
  -o jsonpath='{.data.postgresql-password}' | base64 -d)

kubectl create secret generic postgres-credentials \
  -n "${DEFAULT_NS}" \
  --from-literal=password="${PG_PASS}" \
  --dry-run=client -o yaml | kubectl apply -f -

step "postgres-credentials secret created in ${DEFAULT_NS}"

# ── 9. Extract JWT secret ────────────────────────────────────────────────────

log "9. Extracting JWT secret"

JWT_SECRET=$(kubectl exec -n "${GITLAB_NS}" "${TOOLBOX_POD}" -- \
  cat /etc/gitlab/shell/.gitlab_shell_secret 2>/dev/null || echo "")

if [ -z "${JWT_SECRET}" ]; then
  warn "Could not extract .gitlab_shell_secret. You'll need to set it manually."
else
  cat > "${TILT_DIR}/.secrets" <<EOF
# Auto-generated by setup.sh -- $(date -u +%Y-%m-%dT%H:%M:%SZ)
POSTGRES_PASSWORD=${PG_PASS}
CLICKHOUSE_PASSWORD=
GKG_JWT_SECRET=${JWT_SECRET}
EOF
  step "Written to ${TILT_DIR}/.secrets"
fi

# ── 10. Run Rails db:migrate ─────────────────────────────────────────────────

log "10. Running Rails db:migrate"

kubectl exec -n "${GITLAB_NS}" "${TOOLBOX_POD}" -- \
  bash -c 'cd /srv/gitlab && bundle exec rails db:migrate RAILS_ENV=production' \
  2>&1 | tail -5

step "Migrations complete"

# ── 11. Enable feature flag ──────────────────────────────────────────────────

log "11. Enabling :knowledge_graph feature flag"

kubectl exec -n "${GITLAB_NS}" "${TOOLBOX_POD}" -- \
  bash -c 'cd /srv/gitlab && echo "Feature.enable(:knowledge_graph)" | bundle exec rails console -e production' \
  2>&1 | tail -3

step "Feature flag enabled"

# ── 12. Copy test scripts into toolbox pod ────────────────────────────────────

log "12. Copying test scripts to toolbox pod"

kubectl exec -n "${GITLAB_NS}" "${TOOLBOX_POD}" -- mkdir -p /tmp/e2e

for f in "${GKG_ROOT}"/tests/e2e/*.rb; do
  kubectl cp "${f}" "${GITLAB_NS}/${TOOLBOX_POD}:/tmp/e2e/$(basename "${f}")"
  echo "  Copied $(basename "${f}")"
done

step "Test scripts at /tmp/e2e/ in toolbox pod"

# ── 13. Create test data ─────────────────────────────────────────────────────

log "13. Creating test data"

kubectl exec -n "${GITLAB_NS}" "${TOOLBOX_POD}" -- \
  bash -c 'cd /srv/gitlab && bundle exec rails runner /tmp/e2e/create_test_data.rb RAILS_ENV=production' \
  2>&1 | tee "${LOG_DIR}/create-test-data.log"

step "Test data creation complete"

# Check if manifest was written
if kubectl exec -n "${GITLAB_NS}" "${TOOLBOX_POD}" -- test -f /tmp/e2e/manifest.json 2>/dev/null; then
  step "Manifest verified at /tmp/e2e/manifest.json"
  # Copy manifest back to host for reference
  kubectl cp "${GITLAB_NS}/${TOOLBOX_POD}:/tmp/e2e/manifest.json" "${LOG_DIR}/manifest.json" 2>/dev/null || true
  step "Manifest copied to ${LOG_DIR}/manifest.json"
else
  warn "Manifest not found at /tmp/e2e/manifest.json -- create_test_data.rb may have failed"
  warn "Check ${LOG_DIR}/create-test-data.log for errors"
fi

# ==============================================================================
# PHASE 3: GKG stack (optional)
# ==============================================================================

if [ "${START_GKG}" = true ]; then
  log "PHASE 3: Starting GKG stack with Tilt"

  ensure_docker_host

  step "Building GKG server image..."
  # Tilt's custom_build handles this, but we can pre-build to catch errors
  if [ -f "${GKG_ROOT}/scripts/build-dev.sh" ]; then
    (cd "${GKG_ROOT}" && bash scripts/build-dev.sh "gkg-server:dev") 2>&1 | tail -10 || {
      warn "GKG server build had issues, Tilt will retry..."
    }
  fi

  step "Starting Tilt..."
  step "  cd ${GKG_ROOT}"
  step "  GKG_E2E_CAPRONI=1 tilt up --file e2e/tilt/Tiltfile"
  echo ""

  (cd "${GKG_ROOT}" && GKG_E2E_CAPRONI=1 exec tilt up --file e2e/tilt/Tiltfile)
else
  # ==============================================================================
  # Summary
  # ==============================================================================

  echo ""
  echo "================================================================"
  echo "  Setup complete!"
  echo "================================================================"
  echo ""
  echo "  GitLab is running in k8s namespace '${GITLAB_NS}'"
  echo "  Toolbox pod: ${TOOLBOX_POD}"
  echo "  Logs: ${LOG_DIR}/"
  echo ""
  echo "  Next steps:"
  echo ""
  echo "  1. Start GKG stack:"
  echo "     cd ${GKG_ROOT}"
  echo "     GKG_E2E_CAPRONI=1 mise exec -- tilt up --file e2e/tilt/Tiltfile"
  echo ""
  echo "  2. Wait for all Tilt resources to be green, then run tests:"
  echo "     kubectl exec -n ${GITLAB_NS} ${TOOLBOX_POD} -- \\"
  echo "       bash -c 'cd /srv/gitlab && bundle exec rails runner /tmp/e2e/redaction_test.rb RAILS_ENV=production'"
  echo ""
  echo "  3. To re-run create_test_data.rb after fixing bugs:"
  echo "     # Copy updated scripts:"
  echo "     for f in ${GKG_ROOT}/tests/e2e/*.rb; do"
  echo "       kubectl cp \"\$f\" ${GITLAB_NS}/${TOOLBOX_POD}:/tmp/e2e/\$(basename \"\$f\")"
  echo "     done"
  echo "     # Run:"
  echo "     kubectl exec -n ${GITLAB_NS} ${TOOLBOX_POD} -- \\"
  echo "       bash -c 'cd /srv/gitlab && bundle exec rails runner /tmp/e2e/create_test_data.rb RAILS_ENV=production'"
  echo ""
  echo "  PostgreSQL (for Siphon):"
  echo "    Host: gitlab-postgresql.${GITLAB_NS}.svc.cluster.local:5432"
  echo "    Database: gitlabhq_production"
  echo "    User: gitlab"
  echo ""
  echo "  Useful commands:"
  echo "    kubectl get pods -n ${GITLAB_NS}              # Check GitLab pods"
  echo "    kubectl get pods -n default                    # Check GKG pods (after tilt up)"
  echo "    kubectl logs -n ${GITLAB_NS} ${TOOLBOX_POD}   # Toolbox logs"
  echo "    colima status --profile ${COLIMA_PROFILE}      # Colima status"
  echo ""
fi
