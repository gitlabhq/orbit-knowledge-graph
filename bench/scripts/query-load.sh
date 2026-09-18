#!/usr/bin/env bash
# Drive query load against the bench GKG webserver, then leave latency in the
# ClickHouse query_log for slos.sh to score. Reads the JWT signing key and TLS
# CA from the deployed secrets, port-forwards the webserver, and runs the
# concurrent gRPC driver at the tier's worker concurrency.
#
# Usage: KCTX=... RUN_ID=bench7 TIER=small bash bench/scripts/query-load.sh
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${BENCH_DIR}/scripts/lib.sh"

GKG_NS="e2e-${RUN_ID}-gkg"
LOCAL_PORT="${QUERY_LOAD_PORT:-50054}"
CONCURRENCY="${QUERY_LOAD_CONCURRENCY:-$(tier '.gkg.concurrency.max_concurrent_workers')}"
ROUNDS="${QUERY_LOAD_ROUNDS:-5}"
RESULTS_DIR="${BENCH_DIR}/results/${RUN_ID}"
mkdir -p "${RESULTS_DIR}"

WORK_DIR=$(mktemp -d)
PF_PID=""
cleanup() {
  [[ -n "${PF_PID}" ]] && kill "${PF_PID}" 2>/dev/null || true
  rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

log "Reading JWT signing key and TLS CA from ${GKG_NS}"
GKG_JWT_SECRET=$($KC get secret gkg-secrets -n "${GKG_NS}" \
  -o jsonpath='{.data.gitlab-jwt-signing-key}' | base64 -d)
if [[ -z "${GKG_JWT_SECRET}" ]]; then
  echo "ERROR: could not read gitlab-jwt-signing-key from secret gkg-secrets in ${GKG_NS}" >&2
  exit 1
fi
export GKG_JWT_SECRET

CA_FILE="${WORK_DIR}/ca.crt"
$KC get secret gkg-grpc-tls -n "${GKG_NS}" \
  -o jsonpath='{.data.ca\.crt}' | base64 -d > "${CA_FILE}"
SERVER_NAME="gkg-webserver.${GKG_NS}.svc.cluster.local"

log "Port-forwarding svc/gkg-webserver ${LOCAL_PORT}:50054"
$KC port-forward -n "${GKG_NS}" svc/gkg-webserver "${LOCAL_PORT}:50054" >/dev/null 2>&1 &
PF_PID=$!
for _ in $(seq 1 30); do
  if bash -c "exec 3<>/dev/tcp/127.0.0.1/${LOCAL_PORT}" 2>/dev/null; then
    exec 3>&- 3<&- 2>/dev/null || true
    break
  fi
  sleep 1
done

OUT_FILE="${RESULTS_DIR}/query-load-${TIER}.txt"
log "Running query load: tier=${TIER} concurrency=${CONCURRENCY} rounds=${ROUNDS}"
python3 "${BENCH_DIR}/scripts/grpc_load_driver.py" \
  --endpoint "127.0.0.1:${LOCAL_PORT}" \
  --tls --tls-ca "${CA_FILE}" --tls-server-name "${SERVER_NAME}" \
  --concurrency "${CONCURRENCY}" --rounds "${ROUNDS}" \
  | tee "${OUT_FILE}"

log "Query load complete. Results: ${OUT_FILE}"
