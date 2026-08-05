#!/usr/bin/env bash
# Submit an in-cluster Job that imports the datalake dump from GCS into
# ClickHouse. Data never leaves GCP.
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${BENCH_DIR}/scripts/lib.sh"

: "${DUMP_PREFIX:=core-2026-06-25-1115}"
CH_NAMESPACE="e2e-${RUN_ID}-clickhouse"
JOB_NS="${CH_NAMESPACE}"

log "Submitting import job (dump=${DUMP_PREFIX}, ch_ns=${CH_NAMESPACE})"

export CH_NAMESPACE DUMP_PREFIX
envsubst '${CH_NAMESPACE} ${DUMP_PREFIX}' \
  < "${BENCH_DIR}/manifests/import-job.yaml" \
  | $KC apply -n "${JOB_NS}" -f -

log "Waiting for import job to complete (this takes 15-30 min)..."

# Stream logs while the job runs.
$KC wait -n "${JOB_NS}" job/ra-import-dump \
  --for=condition=ready --timeout=30s 2>/dev/null || true

$KC logs -n "${JOB_NS}" job/ra-import-dump -f --tail=1000 2>/dev/null &
LOG_PID=$!

# Wait for completion (timeout 45 min).
if $KC wait -n "${JOB_NS}" job/ra-import-dump \
  --for=condition=complete --timeout=2700s 2>/dev/null; then
  log "Import job completed successfully"
else
  log "Import job failed or timed out"
  $KC describe job/ra-import-dump -n "${JOB_NS}" 2>/dev/null || true
  kill "${LOG_PID}" 2>/dev/null || true
  exit 1
fi

kill "${LOG_PID}" 2>/dev/null || true
log "Import done"
