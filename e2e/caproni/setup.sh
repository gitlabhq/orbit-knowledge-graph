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
#     9.  Bridge PG credentials to default namespace (for Siphon)
#    10.  Grant REPLICATION privilege to gitlab PG user (for Siphon WAL sender)
#    11.  Extract JWT secret from toolbox pod -> e2e/tilt/.secrets
#    12.  Run Rails db:migrate
#    13.  Enable :knowledge_graph feature flag
#    14.  Copy test scripts into toolbox pod
#    15.  Run create_test_data.rb to create users, groups, projects, etc.
#
#   Phase 3: GKG stack (optional, use --gkg flag)
#    15.  Deploy ClickHouse (standalone, BEFORE Tilt)
#    16.  Run datalake migrations (gitlab:clickhouse:migrate — same as GDK)
#         Creates siphon_* tables + materialized views + dictionaries
#    17.  Apply GKG graph schema (graph.sql — gl_* tables)
#    18.  Drop stale replication slot (if re-running Phase 3)
#    19.  Verify knowledge_graph_enabled_namespaces rows in PG
#    20.  Start Tilt (NATS, siphon, GKG — data flows into pre-existing tables)
#    21.  Wait for siphon data to flow (poll hierarchy tables)
#    22.  Run dispatch-indexing (triggers GKG indexer)
#    23.  OPTIMIZE TABLE FINAL (force ReplacingMergeTree deduplication)
#    24.  Verify graph tables have data
#    25.  Wait for Tilt CI to finish
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
#   - mise (for tilt + zig, only needed with --gkg)
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
# Learnings / gotchas baked into this script:
#
#   CNG images:
#     - Registry path is lowercase: registry.gitlab.com/gitlab-org/build/cng/
#       (NOT CNG-mirror)
#     - GitLab's .dockerignore blocks everything; build-images.sh stages to a
#       temp dir with a permissive .dockerignore
#     - --no-cache required when gem source changes (NO_CACHE=1)
#     - Must copy both gems/ and vendor/gems/; without gems/, get
#       NoMethodError on gitlab-grape-openapi
#     - GDK's config/puma.rb has host paths; Dockerfile.rails preserves CNG's
#
#   GitLab Helm chart:
#     - ClickHouse schema check crashes init containers; need
#       SKIP_CLICKHOUSE_SCHEMA_VERSION_CHECK=YesReally via global.extraEnv
#     - Migration job needs 3Gi memory (default OOM kills)
#     - Workhorse image needs explicit tag + must be pre-pulled (pullPolicy
#       inherits from webservice which is Never)
#
#   Secrets / auth:
#     - JWT secret is at /etc/gitlab/shell/.gitlab_shell_secret (NOT
#       /srv/gitlab/.gitlab_shell_secret)
#     - PG password secret key is "postgresql-password" in
#       "gitlab-postgresql-password" secret
#     - PG superuser password key is "postgresql-postgres-password" in the
#       same secret
#
#   PostgreSQL / Siphon:
#     - PG service name in the gitlab namespace is just "postgresql", so the
#       cross-namespace FQDN is postgresql.gitlab.svc.cluster.local
#       (NOT gitlab-postgresql.gitlab.svc.cluster.local)
#     - The gitlab PG user needs REPLICATION privilege for Siphon's WAL sender.
#       The initdb scripts in gitlab-values.yaml grant this on first PVC
#       creation, but after a colima/k3s restart the PVC persists and initdb
#       does NOT re-run. This script re-grants REPLICATION every time.
#     - Siphon is fully self-bootstrapping: creates publication, reconciles
#       tables, creates replication slot automatically at startup
#
#   ClickHouse:
#     - ClickHouse is deployed BEFORE Tilt (via clickhouse.yaml) so that
#       all migrations run before siphon starts inserting data. This is
#       critical because MVs only fire on NEW inserts — if siphon inserts
#       data before MVs exist, hierarchy tables stay empty.
#     - The default user in ClickHouse 25.1-alpine requires explicit empty
#       password config. clickhouse.yaml sets CLICKHOUSE_PASSWORD="" and
#       CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1.
#     - CLICKHOUSE_PASSWORD in e2e/tilt/.secrets must be empty (not omitted)
#
#   ClickHouse / schema:
#     - gkg-server has NO --mode=migrate. Valid modes: dispatch-indexing,
#       health-check, indexer, webserver. All assume tables already exist.
#     - GKG graph schema (gl_* tables) is applied by setup.sh via
#       clickhouse-client directly in the ClickHouse pod.
#     - Datalake tables (siphon_*) are created by GitLab's ClickHouse
#       migrations: bundle exec rake gitlab:clickhouse:migrate — the same
#       way GDK does it. setup.sh writes a config/click_house.yml into
#       the toolbox pod pointing at the in-cluster ClickHouse, then runs
#       the rake task.
#     - The rake task needs RAILS_ENV=production and the database
#       (gitlab_clickhouse_development) must already exist in ClickHouse.
#     - The MV chain: siphon_namespaces -> namespace_traversal_paths,
#       siphon_projects -> project_namespace_traversal_paths,
#       siphon_merge_requests -> hierarchy_merge_requests,
#       siphon_issues -> hierarchy_work_items. All automatic via MVs.
#
#   knowledge_graph_enabled_namespaces / dispatch-indexing:
#     - The PG table knowledge_graph_enabled_namespaces has no Rails model.
#       It's a raw table with (id, root_namespace_id, created_at, updated_at).
#     - Must be populated with root namespace IDs for the dispatcher to know
#       which namespaces to index. Siphon replicates it to ClickHouse.
#     - dispatch-indexing reads siphon_knowledge_graph_enabled_namespaces in
#       ClickHouse, then publishes NATS messages to trigger the indexer.
#     - Run dispatch-indexing as a k8s Job: same image as indexer
#       (gkg-server:dev), mount gkg-indexer-config configmap, use ENTRYPOINT
#       with args: ["--mode=dispatch-indexing"].
#     - After indexing, run OPTIMIZE TABLE FINAL on all gl_* tables to force
#       ReplacingMergeTree deduplication before running tests.
#
#   Siphon consumer:
#     - The siphon consumer (Go binary) uses clickhouse-go/v2 which speaks
#       the ClickHouse native TCP protocol (port 9000), NOT the HTTP
#       interface (port 8123). The Helm values use nativePort: 9000 for
#       the consumer config.
#     - If ClickHouse connection fails, the consumer exits code 0 with no
#       log output due to a bug: nil pointer panic in conn.Stats() is
#       masked by deferred os.Exit(0). Misleading — looks like "no error"
#       but actually can't connect.
#
#   Tilt:
#     - Tiltfile lives at e2e/tilt/Tiltfile; Tilt sets CWD to that dir.
#       The custom_build for gkg-server must cd to repo root first:
#       'cd ../.. && scripts/build-dev.sh $EXPECTED_REF'
#     - allow_k8s_contexts must include 'colima-caproni' (the colima profile
#       name becomes the k8s context name)
#     - build-dev.sh cross-compiles to aarch64-linux via cargo-zigbuild
#       (requires zig, installed via mise)
#
#   Test data:
#     - Fresh GitLab instance has only root (id=1). All test users, groups,
#       projects must be created by create_test_data.rb.
#     - GitLab service classes return ServiceResponse objects, not models
#       directly (Groups::CreateService, Issues::CreateService, etc.)
#     - MergeRequest uses state_id (1=opened, 2=closed, 3=merged) not a
#       "state" column. merged_at lives in merge_request_metrics.
#     - Non-admin users can't create MRs without project access; use admin
#       as author.
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
  # Workhorse sidecar in webservice pod uses the stock CNG image (not our
  # custom build). The Helm chart inherits pullPolicy from webservice (Never),
  # so the image must be pre-loaded into colima's docker daemon.

  log "3. Pre-pulling workhorse image"

  if docker image inspect "${WORKHORSE_IMAGE}" &>/dev/null; then
    step "Workhorse image already present"
  else
    step "Pulling ${WORKHORSE_IMAGE}..."
    docker pull "${WORKHORSE_IMAGE}" 2>&1 | tail -3
  fi

  # ── 4. Build custom CNG images ─────────────────────────────────────────────
  # Builds 3 images overlaying the feature branch Rails code onto stock CNG
  # base images (v18.8.1):
  #   gkg-e2e/gitlab-webservice-ee:local
  #   gkg-e2e/gitlab-sidekiq-ee:local
  #   gkg-e2e/gitlab-toolbox-ee:local
  #
  # See Dockerfile.rails for the overlay strategy and build-images.sh for
  # the staging directory approach that works around GitLab's .dockerignore.

  if [ "${SKIP_BUILD}" = false ]; then
    log "4. Building custom CNG images"
    step "Source: ${GITLAB_SRC}"
    step "Base tag: ${BASE_TAG}"

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
# Siphon (in default ns) needs the postgres password to connect to the
# GitLab PG (in gitlab ns).

log "8. Bridging PostgreSQL credentials"

PG_PASS=$(kubectl get secret -n "${GITLAB_NS}" gitlab-postgresql-password \
  -o jsonpath='{.data.postgresql-password}' | base64 -d)

kubectl create secret generic postgres-credentials \
  -n "${DEFAULT_NS}" \
  --from-literal=password="${PG_PASS}" \
  --dry-run=client -o yaml | kubectl apply -f -

step "postgres-credentials secret created in ${DEFAULT_NS}"

# ── 9. Grant REPLICATION privilege to gitlab PG user ─────────────────────────
# Siphon's WAL sender needs the gitlab user to have REPLICATION privilege.
# The initdb scripts in gitlab-values.yaml grant this on first PVC creation,
# but after a colima/k3s restart the PVC persists and initdb does NOT re-run.
# We always re-grant here to be idempotent.

log "9. Granting REPLICATION privilege to gitlab PG user"

PG_SUPERPASS=$(kubectl get secret -n "${GITLAB_NS}" gitlab-postgresql-password \
  -o jsonpath='{.data.postgresql-postgres-password}' | base64 -d)

kubectl exec -n "${GITLAB_NS}" postgresql-0 -- \
  bash -c "PGPASSWORD='${PG_SUPERPASS}' psql -U postgres -d gitlabhq_production -c 'ALTER USER gitlab REPLICATION;'" \
  2>&1

step "REPLICATION privilege granted"

# ── 10. Extract JWT secret ───────────────────────────────────────────────────
# The GKG server validates JWTs signed with .gitlab_shell_secret.
# Location: /etc/gitlab/shell/.gitlab_shell_secret (NOT /srv/gitlab/)

log "10. Extracting JWT secret"

JWT_SECRET=$(kubectl exec -n "${GITLAB_NS}" "${TOOLBOX_POD}" -- \
  cat /etc/gitlab/shell/.gitlab_shell_secret 2>/dev/null || echo "")

if [ -z "${JWT_SECRET}" ]; then
  warn "Could not extract .gitlab_shell_secret. You'll need to set it manually."
else
  # Write .secrets file for Tilt
  # CLICKHOUSE_PASSWORD must be empty -- ClickHouse 25.1-alpine is configured
  # with no password (CLICKHOUSE_PASSWORD="" + CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1)
  cat > "${TILT_DIR}/.secrets" <<EOF
# Auto-generated by setup.sh -- $(date -u +%Y-%m-%dT%H:%M:%SZ)
POSTGRES_PASSWORD=${PG_PASS}
CLICKHOUSE_PASSWORD=
GKG_JWT_SECRET=${JWT_SECRET}
EOF
  step "Written to ${TILT_DIR}/.secrets"
fi

# ── 11. Run Rails db:migrate ─────────────────────────────────────────────────

log "11. Running Rails db:migrate"

kubectl exec -n "${GITLAB_NS}" "${TOOLBOX_POD}" -- \
  bash -c 'cd /srv/gitlab && bundle exec rails db:migrate RAILS_ENV=production' \
  2>&1 | tail -5

step "Migrations complete"

# ── 12. Enable feature flag ──────────────────────────────────────────────────

log "12. Enabling :knowledge_graph feature flag"

kubectl exec -n "${GITLAB_NS}" "${TOOLBOX_POD}" -- \
  bash -c 'cd /srv/gitlab && echo "Feature.enable(:knowledge_graph)" | bundle exec rails console -e production' \
  2>&1 | tail -3

step "Feature flag enabled"

# ── 13. Copy test scripts into toolbox pod ────────────────────────────────────
# kubectl cp of a directory creates nested subdirs; copy individual files.

log "13. Copying test scripts to toolbox pod"

kubectl exec -n "${GITLAB_NS}" "${TOOLBOX_POD}" -- mkdir -p /tmp/e2e

for f in "${GKG_ROOT}"/tests/e2e/*.rb; do
  kubectl cp "${f}" "${GITLAB_NS}/${TOOLBOX_POD}:/tmp/e2e/$(basename "${f}")"
  echo "  Copied $(basename "${f}")"
done

step "Test scripts at /tmp/e2e/ in toolbox pod"

# ── 14. Create test data ─────────────────────────────────────────────────────
# Creates users (lois, franklyn, vickey, hanna), group hierarchy, projects,
# MRs, work items, notes, milestones, labels, memberships. Writes JSON
# manifest to /tmp/e2e/manifest.json with all dynamic IDs.

log "14. Creating test data"

kubectl exec -n "${GITLAB_NS}" "${TOOLBOX_POD}" -- \
  bash -c 'cd /srv/gitlab && bundle exec rails runner /tmp/e2e/create_test_data.rb RAILS_ENV=production' \
  2>&1 | tee "${LOG_DIR}/create-test-data.log"

step "Test data creation complete"

# Check if manifest was written
if kubectl exec -n "${GITLAB_NS}" "${TOOLBOX_POD}" -- test -f /tmp/e2e/manifest.json 2>/dev/null; then
  step "Manifest verified at /tmp/e2e/manifest.json"
  kubectl cp "${GITLAB_NS}/${TOOLBOX_POD}:/tmp/e2e/manifest.json" "${LOG_DIR}/manifest.json" 2>/dev/null || true
  step "Manifest copied to ${LOG_DIR}/manifest.json"
else
  warn "Manifest not found at /tmp/e2e/manifest.json -- create_test_data.rb may have failed"
  warn "Check ${LOG_DIR}/create-test-data.log for errors"
fi

# ==============================================================================
# PHASE 3: GKG stack (optional)
# ==============================================================================
#
# The key insight: ClickHouse must be deployed and fully migrated BEFORE siphon
# starts, because materialized views (MVs) only fire on NEW inserts. If siphon
# inserts data before MVs exist, hierarchy tables stay empty and traversal
# paths default to '0/'.
#
# Order of operations:
#   1. Deploy ClickHouse (standalone, not via Tilt)
#   2. Run datalake migrations (creates siphon_* tables, MVs, dictionaries)
#   3. Apply GKG graph schema (creates gl_* tables)
#   4. Insert knowledge_graph_enabled_namespaces rows in PG
#   5. Start Tilt (siphon + GKG — data flows into pre-existing tables)
#   6. Wait for siphon data + indexing
#   7. OPTIMIZE TABLE FINAL for deduplication
# ==============================================================================

if [ "${START_GKG}" = true ]; then
  log "PHASE 3: GKG stack"

  ensure_docker_host

  CH_URL="http://gkg-e2e-clickhouse.${DEFAULT_NS}.svc.cluster.local:8123"
  CH_DB="gitlab_clickhouse_development"
  GRAPH_DB="gkg-development"

  # ── 15. Deploy ClickHouse ──────────────────────────────────────────────────
  # Deployed as a standalone k8s resource BEFORE Tilt, so we can run all
  # migrations before siphon starts inserting data.

  log "15. Deploying ClickHouse"

  kubectl apply -f "${SCRIPT_DIR}/clickhouse.yaml"
  step "ClickHouse manifests applied"

  step "Waiting for ClickHouse pod..."
  kubectl wait --for=condition=ready pod -l app=gkg-e2e-clickhouse \
    -n "${DEFAULT_NS}" --timeout=300s
  step "ClickHouse is ready"

  # ── 16. Run datalake migrations ────────────────────────────────────────────
  # Creates ALL siphon_* tables, materialized views, and dictionaries.
  # Same as GDK: write config/click_house.yml, then rake gitlab:clickhouse:migrate.

  log "16. Running ClickHouse datalake migrations"

  step "Writing config/click_house.yml to toolbox pod..."
  kubectl exec -n "${GITLAB_NS}" "${TOOLBOX_POD}" -- bash -c "cat > /srv/gitlab/config/click_house.yml <<CHEOF
production:
  main:
    database: ${CH_DB}
    url: '${CH_URL}'
    username: default
    password:
CHEOF"

  step "Running gitlab:clickhouse:migrate..."
  kubectl exec -n "${GITLAB_NS}" "${TOOLBOX_POD}" -- \
    bash -c 'cd /srv/gitlab && bundle exec rake gitlab:clickhouse:migrate RAILS_ENV=production' \
    2>&1 | tee "${LOG_DIR}/clickhouse-migrate.log" | tail -10

  step "Datalake migrations complete (tables + MVs + dictionaries)"

  # ── 17. Apply GKG graph schema ─────────────────────────────────────────────
  # Creates gl_* tables (gl_user, gl_project, gl_merge_request, etc.) in the
  # gkg-development database. These are where the indexer writes.

  log "17. Applying GKG graph schema"

  CH_POD=$(kubectl get pod -n "${DEFAULT_NS}" -l app=gkg-e2e-clickhouse \
    -o jsonpath='{.items[0].metadata.name}')

  # Copy graph.sql into the ClickHouse pod and apply it
  kubectl cp "${GKG_ROOT}/fixtures/schema/graph.sql" \
    "${DEFAULT_NS}/${CH_POD}:/tmp/graph.sql"

  kubectl exec -n "${DEFAULT_NS}" "${CH_POD}" -- \
    sh -c "clickhouse-client --user default --database '${GRAPH_DB}' --multiquery < /tmp/graph.sql"

  step "Graph schema applied to ${GRAPH_DB}"

  # ── 18. Drop stale siphon state in PG (if any) ─────────────────────────
  # If Phase 3 is re-run after a tilt-only teardown, the old replication slot
  # and publication persist in PG. The producer would pick up from its last
  # LSN (past all test data) and have nothing to stream. Dropping both forces
  # the producer to recreate the publication (triggering snapshot events for
  # each table) and create a fresh replication slot.

  log "18. Dropping stale siphon state in PG (slot + publication)"

  PG_SUPERPASS=$(kubectl get secret -n "${GITLAB_NS}" gitlab-postgresql-password \
    -o jsonpath='{.data.postgresql-postgres-password}' | base64 -d)

  # Drop replication slot if it exists
  SLOT_EXISTS=$(kubectl exec -n "${GITLAB_NS}" postgresql-0 -- \
    bash -c "PGPASSWORD='${PG_SUPERPASS}' psql -U postgres -d gitlabhq_production -t -c \"SELECT count(*) FROM pg_replication_slots WHERE slot_name='siphon_slot_main_db';\"" 2>/dev/null | tr -d ' ')

  if [ "${SLOT_EXISTS}" = "1" ]; then
    kubectl exec -n "${GITLAB_NS}" postgresql-0 -- \
      bash -c "PGPASSWORD='${PG_SUPERPASS}' psql -U postgres -d gitlabhq_production -c \"SELECT pg_drop_replication_slot('siphon_slot_main_db');\"" 2>&1
    step "Dropped stale replication slot"
  else
    step "No stale replication slot found"
  fi

  # Drop publication (forces producer to re-add all tables, triggering snapshots)
  kubectl exec -n "${GITLAB_NS}" postgresql-0 -- \
    bash -c "PGPASSWORD='${PG_SUPERPASS}' psql -U postgres -d gitlabhq_production -c \"DROP PUBLICATION IF EXISTS siphon_publication_main_db;\"" 2>&1
  step "Dropped publication (will be recreated by siphon producer)"

  # ── 19. Verify knowledge_graph_enabled_namespaces ────────────────────────
  # These rows were inserted by create_test_data.rb (Phase 2, step 5).
  # Siphon will replicate them to siphon_knowledge_graph_enabled_namespaces
  # in ClickHouse, which the dispatcher queries to find namespaces to index.

  log "19. Verifying knowledge_graph_enabled_namespaces in PG"

  kubectl exec -n "${GITLAB_NS}" "${TOOLBOX_POD}" -- \
    bash -c "cd /srv/gitlab && echo 'puts ActiveRecord::Base.connection.select_values(\"SELECT root_namespace_id FROM knowledge_graph_enabled_namespaces ORDER BY root_namespace_id\").inspect' | bundle exec rails runner - RAILS_ENV=production" \
    2>&1

  step "knowledge_graph_enabled_namespaces verified"

  # ── 20. Start Tilt ─────────────────────────────────────────────────────────
  # Now that ClickHouse is fully migrated (datalake + graph), start Tilt which
  # deploys NATS, siphon, and GKG. Siphon will do its initial PG snapshot and
  # data flows into tables that already have MVs -> hierarchy tables populate
  # automatically.

  log "20. Starting Tilt (NATS + siphon + GKG)"

  step "Starting Tilt CI in background..."
  (cd "${GKG_ROOT}" && GKG_E2E_CAPRONI=1 mise exec -- tilt ci --file e2e/tilt/Tiltfile --timeout 20m) \
    > "${LOG_DIR}/tilt-ci.log" 2>&1 &
  TILT_PID=$!
  echo "${TILT_PID}" > "${LOG_DIR}/tilt-ci.pid"
  step "Tilt CI started (PID ${TILT_PID}), log: ${LOG_DIR}/tilt-ci.log"

  # ── 21. Wait for siphon data ───────────────────────────────────────────────
  # Siphon needs time to: connect to PG, create publication/replication slot,
  # snapshot existing data, and stream it through NATS to ClickHouse.
  # We poll hierarchy_merge_requests to know when data has flowed through.

  log "21. Waiting for siphon data to flow"

  step "Polling hierarchy_merge_requests for data (up to 10 min)..."
  SIPHON_TIMEOUT=600
  SIPHON_START=$(date +%s)
  while true; do
    ELAPSED=$(( $(date +%s) - SIPHON_START ))
    if [ "${ELAPSED}" -ge "${SIPHON_TIMEOUT}" ]; then
      warn "Timed out waiting for siphon data after ${SIPHON_TIMEOUT}s"
      break
    fi

    # Check if hierarchy_merge_requests has rows (MVs populated)
    ROW_COUNT=$(kubectl exec -n "${DEFAULT_NS}" "${CH_POD}" -- \
      clickhouse-client --user default --database "${CH_DB}" \
      --query "SELECT count() FROM hierarchy_merge_requests" 2>/dev/null || echo "0")

    if [ "${ROW_COUNT}" -gt 0 ] 2>/dev/null; then
      step "hierarchy_merge_requests has ${ROW_COUNT} rows (siphon data flowing)"
      break
    fi

    sleep 15
    echo "  ... waiting (${ELAPSED}s elapsed, hierarchy_merge_requests: ${ROW_COUNT:-0} rows)"
  done

  # ── 22. Run dispatch-indexing ──────────────────────────────────────────────
  # The dispatch-indexing mode publishes NATS messages to trigger the indexer.
  # It reads siphon_knowledge_graph_enabled_namespaces for namespace list.
  # We run it as a one-shot k8s Job using the same gkg-server image + config.

  log "22. Running dispatch-indexing"

  # Tilt tags images as gkg-server:tilt-<hash>, not gkg-server:dev.
  # The dispatch-indexing Job needs gkg-server:dev, so tag the latest build.
  TILT_TAG=$(docker images gkg-server --format '{{.Tag}}' 2>/dev/null | grep '^tilt-' | head -1)
  if [ -n "${TILT_TAG}" ]; then
    docker tag "gkg-server:${TILT_TAG}" gkg-server:dev 2>/dev/null || true
    step "Tagged gkg-server:${TILT_TAG} as gkg-server:dev"
  fi

  # Delete previous job if it exists (jobs are immutable)
  kubectl delete job gkg-dispatch-indexing -n "${DEFAULT_NS}" --ignore-not-found 2>/dev/null

  kubectl apply -f - <<'DISPATCHEOF'
apiVersion: batch/v1
kind: Job
metadata:
  name: gkg-dispatch-indexing
  namespace: default
spec:
  backoffLimit: 2
  template:
    spec:
      restartPolicy: Never
      enableServiceLinks: false
      containers:
        - name: dispatch
          image: gkg-server:dev
          imagePullPolicy: Never
          args: ["--mode=dispatch-indexing"]
          env:
            - name: RUST_LOG
              value: "info,gkg_server=debug"
            - name: GKG_DATALAKE__PASSWORD
              valueFrom:
                secretKeyRef:
                  name: clickhouse-credentials
                  key: password
                  optional: true
          volumeMounts:
            - name: config
              mountPath: /app/config
              readOnly: true
      volumes:
        - name: config
          configMap:
            name: gkg-indexer-config
DISPATCHEOF

  step "Waiting for dispatch-indexing job to complete..."
  kubectl wait --for=condition=complete job/gkg-dispatch-indexing \
    -n "${DEFAULT_NS}" --timeout=120s 2>/dev/null || {
    warn "dispatch-indexing job did not complete. Checking logs..."
    kubectl logs -n "${DEFAULT_NS}" job/gkg-dispatch-indexing --tail=20 2>/dev/null || true
  }

  step "dispatch-indexing complete"

  # Give the indexer time to process the dispatched messages
  step "Waiting 60s for indexer to process messages..."
  sleep 60

  # ── 23. OPTIMIZE TABLE FINAL ───────────────────────────────────────────────
  # ReplacingMergeTree deduplication is lazy. Force it now so test assertions
  # on exact counts work correctly.

  log "23. Running OPTIMIZE TABLE FINAL on graph tables"

  GL_TABLES="gl_user gl_group gl_project gl_merge_request gl_work_item gl_note gl_milestone gl_label gl_edge"
  for table in ${GL_TABLES}; do
    kubectl exec -n "${DEFAULT_NS}" "${CH_POD}" -- \
      clickhouse-client --user default --database "${GRAPH_DB}" \
      --query "OPTIMIZE TABLE ${table} FINAL" 2>/dev/null || true
  done

  step "OPTIMIZE TABLE FINAL complete"

  # ── 24. Verify graph tables have data ──────────────────────────────────────

  log "24. Verifying graph tables"

  step "Row counts in ${GRAPH_DB}:"
  for table in ${GL_TABLES}; do
    COUNT=$(kubectl exec -n "${DEFAULT_NS}" "${CH_POD}" -- \
      clickhouse-client --user default --database "${GRAPH_DB}" \
      --query "SELECT count() FROM ${table} FINAL" 2>/dev/null || echo "?")
    echo "  ${table}: ${COUNT}"
  done

  # ── 25. Wait for Tilt CI to finish ─────────────────────────────────────────

  log "25. Waiting for Tilt CI to finish"

  if wait "${TILT_PID}" 2>/dev/null; then
    step "Tilt CI completed successfully"
  else
    warn "Tilt CI exited with non-zero status. Check ${LOG_DIR}/tilt-ci.log"
  fi

  echo ""
  echo "================================================================"
  echo "  Phase 3 complete! GKG stack is running."
  echo "================================================================"
  echo ""
  echo "  Run the E2E tests:"
  echo "    kubectl exec -n ${GITLAB_NS} ${TOOLBOX_POD} -- \\"
  echo "      bash -c 'cd /srv/gitlab && KNOWLEDGE_GRAPH_GRPC_ENDPOINT=gkg-webserver.default.svc.cluster.local:50051 bundle exec rails runner /tmp/e2e/redaction_test.rb RAILS_ENV=production'"
  echo ""

else
  # ==============================================================================
  # Summary (Phase 1+2 only)
  # ==============================================================================

  echo ""
  echo "================================================================"
  echo "  Phase 1+2 complete! GitLab is running."
  echo "================================================================"
  echo ""
  echo "  GitLab namespace: ${GITLAB_NS}"
  echo "  Toolbox pod: ${TOOLBOX_POD}"
  echo "  Logs: ${LOG_DIR}/"
  echo ""
  echo "  To start the GKG stack (Phase 3), re-run:"
  echo "    ./setup.sh --phase2-only --gkg"
  echo ""
  echo "  Or manually:"
  echo "    1. Deploy ClickHouse:   kubectl apply -f ${SCRIPT_DIR}/clickhouse.yaml"
  echo "    2. Run migrations:      (see setup.sh --gkg for automated steps)"
  echo "    3. Start Tilt:          GKG_E2E_CAPRONI=1 mise exec -- tilt up --file e2e/tilt/Tiltfile"
  echo ""
  echo "  PostgreSQL (for Siphon):"
  echo "    Host: postgresql.${GITLAB_NS}.svc.cluster.local:5432"
  echo "    Database: gitlabhq_production"
  echo "    User: gitlab"
  echo ""
  echo "  Useful commands:"
  echo "    kubectl get pods -n ${GITLAB_NS}              # Check GitLab pods"
  echo "    kubectl get pods -n default                    # Check GKG pods"
  echo "    kubectl logs -n ${GITLAB_NS} ${TOOLBOX_POD}   # Toolbox logs"
  echo "    colima status --profile ${COLIMA_PROFILE}      # Colima status"
  echo ""
fi
