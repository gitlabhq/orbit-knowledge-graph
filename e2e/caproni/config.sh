#!/usr/bin/env bash
# =============================================================================
# config.sh -- Shared configuration for all E2E caproni scripts
#
# Sourced by: setup.sh, teardown.sh, build-images.sh
#
# All configurable values live here. Override any of them via environment
# variables before running the scripts.
# =============================================================================

# ── Paths ─────────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[1]:-${BASH_SOURCE[0]}}")" && pwd)"
GKG_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TILT_DIR="${GKG_ROOT}/e2e/tilt"
LOG_DIR="${GKG_ROOT}/.dev"

GITLAB_SRC="${GITLAB_SRC:-$HOME/Desktop/Code/gdk/gitlab}"
CAPRONI_SRC="${CAPRONI_SRC:-$HOME/Desktop/Code/caproni-cli}"

# ── Colima / k8s ──────────────────────────────────────────────────────────────

COLIMA_PROFILE="${COLIMA_PROFILE:-caproni}"
COLIMA_MEMORY="${COLIMA_MEMORY:-12}"
COLIMA_CPUS="${COLIMA_CPUS:-4}"
COLIMA_DISK="${COLIMA_DISK:-60}"
COLIMA_K8S_VERSION="${COLIMA_K8S_VERSION:-v1.31.5+k3s1}"

# ── Kubernetes namespaces ─────────────────────────────────────────────────────

GITLAB_NS="${GITLAB_NS:-gitlab}"
DEFAULT_NS="${DEFAULT_NS:-default}"

# ── CNG image settings ───────────────────────────────────────────────────────
# BASE_TAG: closest stable CNG release to the feature branch (18.9.0-pre).
# Update this if the feature branch is rebased onto a newer release.

BASE_TAG="${BASE_TAG:-v18.8.1}"
CNG_REGISTRY="${CNG_REGISTRY:-registry.gitlab.com/gitlab-org/build/cng}"
LOCAL_PREFIX="${LOCAL_PREFIX:-gkg-e2e}"
LOCAL_TAG="${LOCAL_TAG:-local}"
WORKHORSE_IMAGE="${CNG_REGISTRY}/gitlab-workhorse-ee:${BASE_TAG}"

CNG_COMPONENTS=(
  "gitlab-webservice-ee"
  "gitlab-sidekiq-ee"
  "gitlab-toolbox-ee"
)

# ── ClickHouse ────────────────────────────────────────────────────────────────

CH_SERVICE_NAME="${CH_SERVICE_NAME:-gkg-e2e-clickhouse}"
CH_URL="http://${CH_SERVICE_NAME}.${DEFAULT_NS}.svc.cluster.local:8123"
CH_DATALAKE_DB="${CH_DATALAKE_DB:-gitlab_clickhouse_development}"
CH_GRAPH_DB="${CH_GRAPH_DB:-gkg-development}"

# ── PostgreSQL ────────────────────────────────────────────────────────────────

PG_SECRET_NAME="${PG_SECRET_NAME:-gitlab-postgresql-password}"
PG_PASSWORD_KEY="${PG_PASSWORD_KEY:-postgresql-password}"
PG_SUPERPASS_KEY="${PG_SUPERPASS_KEY:-postgresql-postgres-password}"
PG_POD="${PG_POD:-postgresql-0}"
PG_DATABASE="${PG_DATABASE:-gitlabhq_production}"
PG_USER="${PG_USER:-gitlab}"

# ── Siphon ────────────────────────────────────────────────────────────────────

SIPHON_PUBLICATION="${SIPHON_PUBLICATION:-siphon_publication_main_db}"
SIPHON_SLOT="${SIPHON_SLOT:-siphon_slot_main_db}"
SIPHON_POLL_TIMEOUT="${SIPHON_POLL_TIMEOUT:-600}"

# ── GKG ───────────────────────────────────────────────────────────────────────

GKG_GRPC_ENDPOINT="${GKG_GRPC_ENDPOINT:-gkg-webserver.${DEFAULT_NS}.svc.cluster.local:50051}"
GKG_SERVER_IMAGE="${GKG_SERVER_IMAGE:-gkg-server}"
GKG_DISPATCH_JOB="${GKG_DISPATCH_JOB:-gkg-dispatch-indexing}"
GKG_INDEXER_CONFIGMAP="${GKG_INDEXER_CONFIGMAP:-gkg-indexer-config}"
GL_TABLES="gl_user gl_group gl_project gl_merge_request gl_work_item gl_note gl_milestone gl_label gl_edge"

# ── Paths inside pods ────────────────────────────────────────────────────────

RAILS_ROOT="/srv/gitlab"
JWT_SECRET_PATH="/etc/gitlab/shell/.gitlab_shell_secret"
E2E_POD_DIR="/tmp/e2e"
MANIFEST_POD_PATH="${E2E_POD_DIR}/manifest.json"

# ── Shared helpers ────────────────────────────────────────────────────────────

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

# Get the toolbox pod name (call after GitLab is deployed)
get_toolbox_pod() {
  kubectl get pod -n "${GITLAB_NS}" -l app=toolbox \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null
}

# Get the ClickHouse pod name
get_ch_pod() {
  kubectl get pod -n "${DEFAULT_NS}" -l app="${CH_SERVICE_NAME}" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null
}

# Run a command in the toolbox pod
toolbox_exec() {
  local pod="${1}"; shift
  kubectl exec -n "${GITLAB_NS}" "${pod}" -- "$@"
}

# Run a rails runner script in the toolbox pod
toolbox_rails_runner() {
  local pod="$1" script="$2"
  kubectl exec -n "${GITLAB_NS}" "${pod}" -- \
    bash -c "cd ${RAILS_ROOT} && bundle exec rails runner ${script} RAILS_ENV=production"
}

# Run a one-liner Ruby command via rails runner in the toolbox pod.
# NOTE: Do NOT use `rails console` with piped input — it hangs in k8s pods
# because it waits for an interactive TTY even after processing stdin.
toolbox_rails_eval() {
  local pod="$1" cmd="$2"
  kubectl exec -n "${GITLAB_NS}" "${pod}" -- \
    bash -c "cd ${RAILS_ROOT} && bundle exec rails runner '${cmd}' RAILS_ENV=production"
}

# Run a psql command as superuser
pg_superuser_exec() {
  local pg_superpass="$1" sql="$2"
  kubectl exec -n "${GITLAB_NS}" "${PG_POD}" -- \
    bash -c "PGPASSWORD='${pg_superpass}' psql -U postgres -d ${PG_DATABASE} -c \"${sql}\""
}

# Run a psql command as superuser (return value only, no headers)
pg_superuser_query() {
  local pg_superpass="$1" sql="$2"
  kubectl exec -n "${GITLAB_NS}" "${PG_POD}" -- \
    bash -c "PGPASSWORD='${pg_superpass}' psql -U postgres -d ${PG_DATABASE} -t -c \"${sql}\"" 2>/dev/null | tr -d ' '
}

# Run a clickhouse-client query
ch_query() {
  local database="$1" query="$2"
  local pod
  pod=$(get_ch_pod)
  kubectl exec -n "${DEFAULT_NS}" "${pod}" -- \
    clickhouse-client --user default --database "${database}" --query "${query}" 2>/dev/null
}
