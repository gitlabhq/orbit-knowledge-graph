#!/usr/bin/env bash
# =============================================================================
# setup.sh -- Full E2E environment setup: zero to running GKG stack
#
# This is the single "from scratch" script that:
#
#   Phase 1: Cluster + GitLab
#     1. Install Caproni CLI (build from source)
#     2. Start Colima with caproni profile (k3s, 12GiB, 4 cores)
#     3. Pre-pull workhorse image into colima's docker daemon
#     4. Build 3 custom CNG images from the GitLab feature branch
#     5. Deploy Traefik ingress controller
#     6. Deploy GitLab via Helm chart (all custom images, PG logical replication)
#     7. Wait for all pods to be healthy
#
#   Phase 2: Post-deploy
#     8.  Bridge PG credentials to default namespace (for Siphon)
#     9.  Grant REPLICATION privilege to gitlab PG user (for Siphon WAL sender)
#    10.  Extract JWT secret from toolbox pod -> e2e/tilt/.secrets
#    11.  Run Rails db:migrate
#    12.  Enable :knowledge_graph feature flag
#    13.  Copy test scripts into toolbox pod
#    14.  Run create_test_data.rb to create users, groups, projects, etc.
#
#   Phase 3: GKG stack (use --gkg flag)
#    15.  Deploy ClickHouse (standalone, BEFORE Tilt)
#    16.  Run datalake migrations (gitlab:clickhouse:migrate — same as GDK)
#         Creates siphon_* tables + materialized views + dictionaries
#    17.  Apply GKG graph schema (graph.sql — gl_* tables)
#    18.  Drop stale siphon replication state (slot + publication)
#    19.  Verify knowledge_graph_enabled_namespaces rows in PG
#    20.  Start Tilt (NATS, siphon, GKG — data flows into pre-existing tables)
#    21.  Wait for siphon data to flow (poll hierarchy tables)
#    22.  Run dispatch-indexing (triggers GKG indexer)
#    23.  OPTIMIZE TABLE FINAL (force ReplacingMergeTree deduplication)
#    24.  Verify graph tables have data
#    25.  Run E2E tests
#
# Usage:
#   cd ~/Desktop/Code/gkg/e2e/caproni
#   ./setup.sh                  # Phase 1 + 2 (default)
#   ./setup.sh --gkg            # Phase 1 + 2 + 3 (full stack + E2E tests)
#   ./setup.sh --phase1         # Phase 1 only (cluster + GitLab)
#   ./setup.sh --phase2         # Phase 2 only (post-deploy + test data)
#   ./setup.sh --phase3         # Phase 3 only (GKG stack + E2E tests)
#   ./setup.sh --skip-build     # Skip CNG image build (reuse existing)
#   ./setup.sh --skip-caproni   # Skip caproni install (already installed)
#
# All configuration lives in config.sh. Override any value via env var:
#   GITLAB_SRC=/my/path COLIMA_MEMORY=16 ./setup.sh --gkg
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
#   Siphon snapshot triggering:
#     - Snapshots are triggered when tables are ADDED to the PG publication.
#       If the publication already exists with all tables (from a previous
#       run), no tables get "added" and NO snapshots fire.
#     - Must drop BOTH the publication AND the replication slot to force a
#       fresh start. Step 18 handles this.
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
#     - After indexing, run OPTIMIZE TABLE FINAL on all gl_* tables to force
#       ReplacingMergeTree deduplication before running tests.
#
#   Siphon consumer:
#     - The siphon consumer (Go binary) uses clickhouse-go/v2 which speaks
#       the ClickHouse native TCP protocol (port 9000), NOT the HTTP
#       interface (port 8123). The Helm values use nativePort: 9000 for
#       the consumer config.
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
#     - Organizations::OrganizationUser records are required for group
#       membership. GitLab seeds create this for root but NOT for
#       programmatically-created users. Without it, group.add_member fails
#       with "already belongs to another organization". create_test_data.rb
#       handles this in find_or_create_user.
#     - Non-admin users can't create MRs without project access; use admin
#       as author.
#
#   gRPC connectivity:
#     - The redaction_test.rb runs in the toolbox pod (gitlab namespace).
#       The GKG webserver is in the default namespace, so the gRPC endpoint
#       must be set to gkg-webserver.default.svc.cluster.local:50051.
#     - Set via env var: KNOWLEDGE_GRAPH_GRPC_ENDPOINT. Default in Rails
#       (localhost:50051) does NOT work cross-namespace.
#
# =============================================================================
set -euo pipefail

# ── Load shared config ────────────────────────────────────────────────────────
# shellcheck source=config.sh
source "$(cd "$(dirname "$0")" && pwd)/config.sh"

# ── Parse flags ───────────────────────────────────────────────────────────────
#
# Phase selection:
#   (no flags)       Phase 1 + 2
#   --gkg            Phase 1 + 2 + 3
#   --phase1         Phase 1 only
#   --phase2         Phase 2 only
#   --phase3         Phase 3 only
#
# Modifiers:
#   --skip-build     Skip CNG image build in Phase 1 (reuse existing images)
#   --skip-caproni   Skip caproni CLI install in Phase 1

SKIP_BUILD=false
SKIP_CAPRONI=false
RUN_PHASE1=false
RUN_PHASE2=false
RUN_PHASE3=false
EXPLICIT_PHASE=false

for arg in "$@"; do
  case "$arg" in
    --skip-build)    SKIP_BUILD=true ;;
    --skip-caproni)  SKIP_CAPRONI=true ;;
    --phase1)        RUN_PHASE1=true; EXPLICIT_PHASE=true ;;
    --phase2)        RUN_PHASE2=true; EXPLICIT_PHASE=true ;;
    --phase3)        RUN_PHASE3=true; EXPLICIT_PHASE=true ;;
    --gkg)           RUN_PHASE1=true; RUN_PHASE2=true; RUN_PHASE3=true; EXPLICIT_PHASE=true ;;
    # Legacy aliases
    --phase2-only)   RUN_PHASE2=true; EXPLICIT_PHASE=true ;;
    --phase3-only)   RUN_PHASE3=true; EXPLICIT_PHASE=true ;;
    --help|-h)
      head -45 "$0" | grep '^#' | sed 's/^# *//'
      exit 0
      ;;
  esac
done

# Default: Phase 1 + 2 if no explicit phase flags
if [ "${EXPLICIT_PHASE}" = false ]; then
  RUN_PHASE1=true
  RUN_PHASE2=true
fi

mkdir -p "${LOG_DIR}"

# ── Validate prerequisites ────────────────────────────────────────────────────

if [ "${RUN_PHASE1}" = true ]; then
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
echo "  Phases:       1=${RUN_PHASE1} 2=${RUN_PHASE2} 3=${RUN_PHASE3}"
echo "  Modifiers:    skip-build=${SKIP_BUILD} skip-caproni=${SKIP_CAPRONI}"
echo "================================================================"

# ==============================================================================
# PHASE 1: Cluster + GitLab
# ==============================================================================

if [ "${RUN_PHASE1}" = true ]; then

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
      mkdir -p "${HOME}/.local/bin"
      (cd "${CAPRONI_SRC}" && go build -o "${HOME}/.local/bin/caproni" .)
      export PATH="${HOME}/.local/bin:${PATH}"
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
      --disk "${COLIMA_DISK}" \
      --vm-type vz \
      --kubernetes \
      --kubernetes-version "${COLIMA_K8S_VERSION}" \
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

fi  # end RUN_PHASE1

# ==============================================================================
# PHASE 2: Post-deploy
# ==============================================================================

if [ "${RUN_PHASE2}" = true ]; then

log "PHASE 2: Post-deploy setup"

ensure_docker_host

TOOLBOX_POD=$(get_toolbox_pod)

if [ -z "${TOOLBOX_POD}" ]; then
  echo "ERROR: No toolbox pod found in ${GITLAB_NS} namespace."
  echo "Is GitLab deployed? Run without --phase2-only to deploy."
  exit 1
fi
step "Toolbox pod: ${TOOLBOX_POD}"

# ── 8. Bridge PG credentials to default namespace ────────────────────────────

log "8. Bridging PostgreSQL credentials"

PG_PASS=$(kubectl get secret -n "${GITLAB_NS}" "${PG_SECRET_NAME}" \
  -o jsonpath="{.data.${PG_PASSWORD_KEY}}" | base64 -d)

kubectl create secret generic postgres-credentials \
  -n "${DEFAULT_NS}" \
  --from-literal=password="${PG_PASS}" \
  --dry-run=client -o yaml | kubectl apply -f -

step "postgres-credentials secret created in ${DEFAULT_NS}"

# ── 9. Grant REPLICATION privilege to gitlab PG user ─────────────────────────

log "9. Granting REPLICATION privilege to ${PG_USER} PG user"

PG_SUPERPASS=$(kubectl get secret -n "${GITLAB_NS}" "${PG_SECRET_NAME}" \
  -o jsonpath="{.data.${PG_SUPERPASS_KEY}}" | base64 -d)

pg_superuser_exec "${PG_SUPERPASS}" "ALTER USER ${PG_USER} REPLICATION;" 2>&1

step "REPLICATION privilege granted"

# ── 10. Extract JWT secret ───────────────────────────────────────────────────

log "10. Extracting JWT secret"

JWT_SECRET=$(toolbox_exec "${TOOLBOX_POD}" cat "${JWT_SECRET_PATH}" 2>/dev/null || echo "")

if [ -z "${JWT_SECRET}" ]; then
  warn "Could not extract JWT secret. You'll need to set it manually."
else
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

toolbox_exec "${TOOLBOX_POD}" \
  bash -c "cd ${RAILS_ROOT} && bundle exec rails db:migrate RAILS_ENV=production" \
  2>&1 | tail -5

step "Migrations complete"

# ── 12. Enable feature flag ──────────────────────────────────────────────────

log "12. Enabling :knowledge_graph feature flag"

toolbox_rails_eval "${TOOLBOX_POD}" "Feature.enable(:knowledge_graph)" 2>&1 | tail -3

step "Feature flag enabled"

# ── 13. Copy test scripts into toolbox pod ────────────────────────────────────

log "13. Copying test scripts to toolbox pod"

toolbox_exec "${TOOLBOX_POD}" mkdir -p "${E2E_POD_DIR}"

for f in "${GKG_ROOT}"/e2e/tests/*.rb; do
  kubectl cp "${f}" "${GITLAB_NS}/${TOOLBOX_POD}:${E2E_POD_DIR}/$(basename "${f}")"
  echo "  Copied $(basename "${f}")"
done

step "Test scripts at ${E2E_POD_DIR}/ in toolbox pod"

# ── 14. Create test data ─────────────────────────────────────────────────────

log "14. Creating test data"

toolbox_rails_runner "${TOOLBOX_POD}" "${E2E_POD_DIR}/create_test_data.rb" \
  2>&1 | tee "${LOG_DIR}/create-test-data.log"

step "Test data creation complete"

# Check if manifest was written
if toolbox_exec "${TOOLBOX_POD}" test -f "${MANIFEST_POD_PATH}" 2>/dev/null; then
  step "Manifest verified at ${MANIFEST_POD_PATH}"
  kubectl cp "${GITLAB_NS}/${TOOLBOX_POD}:${MANIFEST_POD_PATH}" "${LOG_DIR}/manifest.json" 2>/dev/null || true
  step "Manifest copied to ${LOG_DIR}/manifest.json"
else
  warn "Manifest not found at ${MANIFEST_POD_PATH} -- create_test_data.rb may have failed"
  warn "Check ${LOG_DIR}/create-test-data.log for errors"
fi

fi  # end RUN_PHASE2

# ==============================================================================
# PHASE 3: GKG stack
# ==============================================================================

if [ "${RUN_PHASE3}" = true ]; then

  # Resolve toolbox pod (needed if Phase 2 didn't run in this invocation)
  ensure_docker_host
  TOOLBOX_POD=$(get_toolbox_pod)
  if [ -z "${TOOLBOX_POD}" ]; then
    echo "ERROR: No toolbox pod found in ${GITLAB_NS} namespace."
    exit 1
  fi

  log "PHASE 3: GKG stack"

  ensure_docker_host

  # Re-bridge PG credentials if missing (tilt-only teardown removes them)
  if ! kubectl get secret postgres-credentials -n "${DEFAULT_NS}" &>/dev/null; then
    step "Re-bridging PostgreSQL credentials to ${DEFAULT_NS}..."
    PG_PASS=$(kubectl get secret -n "${GITLAB_NS}" "${PG_SECRET_NAME}" \
      -o jsonpath="{.data.${PG_PASSWORD_KEY}}" | base64 -d)
    kubectl create secret generic postgres-credentials \
      -n "${DEFAULT_NS}" \
      --from-literal=password="${PG_PASS}" \
      --dry-run=client -o yaml | kubectl apply -f -
  fi

  # Re-write .secrets if missing (tilt-only teardown removes it)
  if [ ! -f "${TILT_DIR}/.secrets" ]; then
    step "Re-generating ${TILT_DIR}/.secrets..."
    PG_PASS="${PG_PASS:-$(kubectl get secret -n "${GITLAB_NS}" "${PG_SECRET_NAME}" \
      -o jsonpath="{.data.${PG_PASSWORD_KEY}}" | base64 -d)}"
    JWT_SECRET=$(toolbox_exec "${TOOLBOX_POD}" cat "${JWT_SECRET_PATH}" 2>/dev/null || echo "")
    cat > "${TILT_DIR}/.secrets" <<EOF
# Auto-generated by setup.sh -- $(date -u +%Y-%m-%dT%H:%M:%SZ)
POSTGRES_PASSWORD=${PG_PASS}
CLICKHOUSE_PASSWORD=
GKG_JWT_SECRET=${JWT_SECRET}
EOF
  fi

  # ── 15. Deploy ClickHouse ──────────────────────────────────────────────────

  log "15. Deploying ClickHouse"

  kubectl apply -f "${SCRIPT_DIR}/clickhouse.yaml"
  step "ClickHouse manifests applied"

  step "Waiting for ClickHouse pod..."
  kubectl wait --for=condition=ready pod -l "app=${CH_SERVICE_NAME}" \
    -n "${DEFAULT_NS}" --timeout=300s
  step "ClickHouse is ready"

  # ── 16. Run datalake migrations ────────────────────────────────────────────

  log "16. Running ClickHouse datalake migrations"

  step "Writing config/click_house.yml to toolbox pod..."
  toolbox_exec "${TOOLBOX_POD}" bash -c "cat > ${RAILS_ROOT}/config/click_house.yml <<CHEOF
production:
  main:
    database: ${CH_DATALAKE_DB}
    url: '${CH_URL}'
    username: default
    password:
CHEOF"

  step "Running gitlab:clickhouse:migrate..."
  toolbox_exec "${TOOLBOX_POD}" \
    bash -c "cd ${RAILS_ROOT} && bundle exec rake gitlab:clickhouse:migrate RAILS_ENV=production" \
    2>&1 | tee "${LOG_DIR}/clickhouse-migrate.log" | tail -10

  step "Datalake migrations complete (tables + MVs + dictionaries)"

  # ── 17. Apply GKG graph schema ─────────────────────────────────────────────

  log "17. Applying GKG graph schema"

  CH_POD=$(get_ch_pod)

  kubectl cp "${GKG_ROOT}/fixtures/schema/graph.sql" \
    "${DEFAULT_NS}/${CH_POD}:/tmp/graph.sql"

  kubectl exec -n "${DEFAULT_NS}" "${CH_POD}" -- \
    sh -c "clickhouse-client --user default --database '${CH_GRAPH_DB}' --multiquery < /tmp/graph.sql"

  step "Graph schema applied to ${CH_GRAPH_DB}"

  # ── 18. Drop stale siphon state in PG ──────────────────────────────────────

  log "18. Dropping stale siphon state in PG (slot + publication)"

  PG_SUPERPASS=$(kubectl get secret -n "${GITLAB_NS}" "${PG_SECRET_NAME}" \
    -o jsonpath="{.data.${PG_SUPERPASS_KEY}}" | base64 -d)

  SLOT_EXISTS=$(pg_superuser_query "${PG_SUPERPASS}" \
    "SELECT count(*) FROM pg_replication_slots WHERE slot_name='${SIPHON_SLOT}';")

  if [ "${SLOT_EXISTS}" = "1" ]; then
    pg_superuser_exec "${PG_SUPERPASS}" "SELECT pg_drop_replication_slot('${SIPHON_SLOT}');" 2>&1
    step "Dropped stale replication slot"
  else
    step "No stale replication slot found"
  fi

  pg_superuser_exec "${PG_SUPERPASS}" "DROP PUBLICATION IF EXISTS ${SIPHON_PUBLICATION};" 2>&1
  step "Dropped publication (will be recreated by siphon producer)"

  # ── 19. Verify knowledge_graph_enabled_namespaces ──────────────────────────

  log "19. Verifying knowledge_graph_enabled_namespaces in PG"

  toolbox_rails_eval "${TOOLBOX_POD}" \
    'puts ActiveRecord::Base.connection.select_values("SELECT root_namespace_id FROM knowledge_graph_enabled_namespaces ORDER BY root_namespace_id").inspect' \
    2>&1

  step "knowledge_graph_enabled_namespaces verified"

  # ── 20. Start Tilt ─────────────────────────────────────────────────────────

  log "20. Starting Tilt (NATS + siphon + GKG)"

  step "Starting Tilt CI in background..."
  (cd "${GKG_ROOT}" && GKG_E2E_CAPRONI=1 mise exec -- tilt ci --file e2e/tilt/Tiltfile --timeout 20m) \
    > "${LOG_DIR}/tilt-ci.log" 2>&1 &
  TILT_PID=$!
  echo "${TILT_PID}" > "${LOG_DIR}/tilt-ci.pid"
  step "Tilt CI started (PID ${TILT_PID}), log: ${LOG_DIR}/tilt-ci.log"

  # ── 21. Wait for siphon data ───────────────────────────────────────────────

  log "21. Waiting for siphon data to flow"

  step "Polling hierarchy_merge_requests for data (up to ${SIPHON_POLL_TIMEOUT}s)..."
  SIPHON_START=$(date +%s)
  while true; do
    ELAPSED=$(( $(date +%s) - SIPHON_START ))
    if [ "${ELAPSED}" -ge "${SIPHON_POLL_TIMEOUT}" ]; then
      warn "Timed out waiting for siphon data after ${SIPHON_POLL_TIMEOUT}s"
      break
    fi

    ROW_COUNT=$(ch_query "${CH_DATALAKE_DB}" "SELECT count() FROM hierarchy_merge_requests" || echo "0")

    if [ "${ROW_COUNT}" -gt 0 ] 2>/dev/null; then
      step "hierarchy_merge_requests has ${ROW_COUNT} rows (siphon data flowing)"
      break
    fi

    sleep 15
    echo "  ... waiting (${ELAPSED}s elapsed, hierarchy_merge_requests: ${ROW_COUNT:-0} rows)"
  done

  # Also wait for siphon_knowledge_graph_enabled_namespaces — the dispatcher
  # reads this table to know which namespaces to index. If we dispatch before
  # it's populated, we get 0 namespace indexing requests.
  step "Waiting for siphon_knowledge_graph_enabled_namespaces..."
  KG_START=$(date +%s)
  while true; do
    KG_ELAPSED=$(( $(date +%s) - KG_START ))
    if [ "${KG_ELAPSED}" -ge 300 ]; then
      warn "Timed out waiting for siphon_knowledge_graph_enabled_namespaces after 300s"
      break
    fi

    KG_COUNT=$(ch_query "${CH_DATALAKE_DB}" "SELECT count() FROM siphon_knowledge_graph_enabled_namespaces" || echo "0")

    if [ "${KG_COUNT}" -gt 0 ] 2>/dev/null; then
      step "siphon_knowledge_graph_enabled_namespaces has ${KG_COUNT} rows"
      break
    fi

    sleep 10
    echo "  ... waiting (${KG_ELAPSED}s elapsed, siphon_knowledge_graph_enabled_namespaces: ${KG_COUNT:-0} rows)"
  done

  # ── 22. Run dispatch-indexing ──────────────────────────────────────────────

  log "22. Running dispatch-indexing"

  # Tilt tags images as gkg-server:tilt-<hash>, not gkg-server:dev.
  TILT_TAG=$(docker images "${GKG_SERVER_IMAGE}" --format '{{.Tag}}' 2>/dev/null | grep '^tilt-' | head -1)
  if [ -n "${TILT_TAG}" ]; then
    docker tag "${GKG_SERVER_IMAGE}:${TILT_TAG}" "${GKG_SERVER_IMAGE}:dev" 2>/dev/null || true
    step "Tagged ${GKG_SERVER_IMAGE}:${TILT_TAG} as ${GKG_SERVER_IMAGE}:dev"
  fi

  # Delete previous job if it exists (jobs are immutable)
  kubectl delete job "${GKG_DISPATCH_JOB}" -n "${DEFAULT_NS}" --ignore-not-found 2>/dev/null

  kubectl apply -f - <<DISPATCHEOF
apiVersion: batch/v1
kind: Job
metadata:
  name: ${GKG_DISPATCH_JOB}
  namespace: ${DEFAULT_NS}
spec:
  backoffLimit: 2
  template:
    spec:
      restartPolicy: Never
      enableServiceLinks: false
      containers:
        - name: dispatch
          image: ${GKG_SERVER_IMAGE}:dev
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
            name: ${GKG_INDEXER_CONFIGMAP}
DISPATCHEOF

  step "Waiting for dispatch-indexing job to complete..."
  kubectl wait --for=condition=complete "job/${GKG_DISPATCH_JOB}" \
    -n "${DEFAULT_NS}" --timeout=120s 2>/dev/null || {
    warn "dispatch-indexing job did not complete. Checking logs..."
    kubectl logs -n "${DEFAULT_NS}" "job/${GKG_DISPATCH_JOB}" --tail=20 2>/dev/null || true
  }

  step "dispatch-indexing complete"

  # Wait for the indexer to process dispatched messages by polling gl_project.
  # gl_project is populated by namespace indexing, so once it has rows, the
  # indexer has processed at least some namespace requests.
  step "Waiting for indexer to populate graph tables (polling gl_project)..."
  IDX_START=$(date +%s)
  while true; do
    IDX_ELAPSED=$(( $(date +%s) - IDX_START ))
    if [ "${IDX_ELAPSED}" -ge 300 ]; then
      warn "Timed out waiting for indexer after 300s"
      break
    fi

    PROJECT_COUNT=$(ch_query "${CH_GRAPH_DB}" "SELECT count() FROM gl_project" || echo "0")

    if [ "${PROJECT_COUNT}" -gt 0 ] 2>/dev/null; then
      step "gl_project has ${PROJECT_COUNT} rows — indexer is working"
      # Give it a bit more time to finish all pipelines
      sleep 30
      break
    fi

    sleep 10
    echo "  ... waiting (${IDX_ELAPSED}s elapsed, gl_project: ${PROJECT_COUNT:-0} rows)"
  done

  # ── 23. OPTIMIZE TABLE FINAL ───────────────────────────────────────────────

  log "23. Running OPTIMIZE TABLE FINAL on graph tables"

  for table in ${GL_TABLES}; do
    ch_query "${CH_GRAPH_DB}" "OPTIMIZE TABLE ${table} FINAL" || true
  done

  step "OPTIMIZE TABLE FINAL complete"

  # ── 24. Verify graph tables have data ──────────────────────────────────────

  log "24. Verifying graph tables"

  step "Row counts in ${CH_GRAPH_DB}:"
  for table in ${GL_TABLES}; do
    COUNT=$(ch_query "${CH_GRAPH_DB}" "SELECT count() FROM ${table} FINAL" || echo "?")
    echo "  ${table}: ${COUNT}"
  done

  # ── 25. Run E2E tests ──────────────────────────────────────────────────────

  log "25. Running E2E redaction tests"

  # Re-copy test scripts in case they changed during iteration
  for f in "${GKG_ROOT}"/e2e/tests/*.rb; do
    kubectl cp "${f}" "${GITLAB_NS}/${TOOLBOX_POD}:${E2E_POD_DIR}/$(basename "${f}")"
  done

  step "Running redaction_test.rb..."
  toolbox_exec "${TOOLBOX_POD}" \
    bash -c "cd ${RAILS_ROOT} && KNOWLEDGE_GRAPH_GRPC_ENDPOINT=${GKG_GRPC_ENDPOINT} bundle exec rails runner ${E2E_POD_DIR}/redaction_test.rb RAILS_ENV=production" \
    2>&1 | tee "${LOG_DIR}/redaction-test.log"

  TEST_EXIT=$?

  # ── 26. Wait for Tilt CI to finish ─────────────────────────────────────────

  log "26. Waiting for Tilt CI to finish"

  if wait "${TILT_PID}" 2>/dev/null; then
    step "Tilt CI completed successfully"
  else
    warn "Tilt CI exited with non-zero status. Check ${LOG_DIR}/tilt-ci.log"
  fi

  echo ""
  echo "================================================================"
  if [ "${TEST_EXIT}" -eq 0 ]; then
    echo "  Phase 3 complete! All E2E tests passed."
  else
    echo "  Phase 3 complete. Some tests failed."
    echo "  Check: ${LOG_DIR}/redaction-test.log"
  fi
  echo "================================================================"
  echo ""
  echo "  Re-run tests manually:"
  echo "    kubectl exec -n ${GITLAB_NS} ${TOOLBOX_POD} -- \\"
  echo "      bash -c 'cd ${RAILS_ROOT} && KNOWLEDGE_GRAPH_GRPC_ENDPOINT=${GKG_GRPC_ENDPOINT} bundle exec rails runner ${E2E_POD_DIR}/redaction_test.rb RAILS_ENV=production'"
  echo ""

fi  # end RUN_PHASE3

# ==============================================================================
# Summary
# ==============================================================================

echo ""
echo "================================================================"
echo "  Setup complete!"
PHASES_RAN=""
[ "${RUN_PHASE1}" = true ] && PHASES_RAN="${PHASES_RAN}1 "
[ "${RUN_PHASE2}" = true ] && PHASES_RAN="${PHASES_RAN}2 "
[ "${RUN_PHASE3}" = true ] && PHASES_RAN="${PHASES_RAN}3 "
echo "  Phases run: ${PHASES_RAN:-none}"
echo "================================================================"
echo ""
if [ "${RUN_PHASE3}" = false ]; then
  echo "  Next steps:"
  if [ "${RUN_PHASE2}" = false ] && [ "${RUN_PHASE1}" = true ]; then
    echo "    ./setup.sh --phase2        # Run post-deploy setup"
  fi
  if [ "${RUN_PHASE3}" = false ]; then
    echo "    ./setup.sh --phase3        # Run GKG stack + E2E tests"
    echo "    ./setup.sh --gkg           # Run all phases"
  fi
  echo ""
fi
echo "  Useful commands:"
echo "    kubectl get pods -n ${GITLAB_NS}              # Check GitLab pods"
echo "    kubectl get pods -n ${DEFAULT_NS}              # Check GKG pods"
echo "    colima status --profile ${COLIMA_PROFILE}      # Colima status"
echo ""
