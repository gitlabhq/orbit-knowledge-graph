#!/usr/bin/env bash
# Submit an in-cluster Job that imports the datalake dump from GCS into
# the Siphon-created ClickHouse tables. No DDL: tables already exist
# from setup.sh. Data never leaves GCP.
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${BENCH_DIR}/scripts/lib.sh"

: "${DUMP_PREFIX:=core-2026-06-25-1115}"
CH_NAMESPACE="e2e-${RUN_ID}-clickhouse"
# Run the Job in the CH namespace so it has DNS access to the CH service.
JOB_NS="${CH_NAMESPACE}"

# Read the CH default password and store it in a Secret.
CH_PASSWORD=$($KC get statefulset clickhouse -n "${CH_NAMESPACE}" \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="CLICKHOUSE_PASSWORD")].value}')
if [[ -z "${CH_PASSWORD}" ]]; then
  log "ERROR: could not read CLICKHOUSE_PASSWORD from StatefulSet"
  exit 1
fi

$KC create secret generic ra-import-ch-auth \
  -n "${JOB_NS}" \
  --from-literal=password="${CH_PASSWORD}" \
  --dry-run=client -o yaml | $KC apply -f -

# Ensure the GCS-access KSA exists in this namespace.
$KC create sa ra-import-sa -n "${JOB_NS}" 2>/dev/null || true
$KC annotate sa ra-import-sa -n "${JOB_NS}" \
  "iam.gke.io/gcp-service-account=1079327125344-compute@developer.gserviceaccount.com" \
  --overwrite 2>/dev/null

# Delete any previous Job.
$KC delete job ra-import-dump -n "${JOB_NS}" --ignore-not-found=true 2>/dev/null

log "Submitting import job (dump=${DUMP_PREFIX}, ch_ns=${CH_NAMESPACE})"

sed -e "s/\${CH_NAMESPACE}/${CH_NAMESPACE}/g" \
    -e "s/\${DUMP_PREFIX}/${DUMP_PREFIX}/g" \
  < "${BENCH_DIR}/manifests/import-job.yaml" \
  | $KC apply -n "${JOB_NS}" -f -

log "Waiting for import job to start..."
$KC wait -n "${JOB_NS}" job/ra-import-dump \
  --for=condition=ready --timeout=120s 2>/dev/null || true

log "Streaming logs (15-30 min expected)..."
$KC logs -n "${JOB_NS}" job/ra-import-dump -f --tail=1000 2>/dev/null &
LOG_PID=$!

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

# Clean up secrets.
$KC delete secret ra-import-ch-auth -n "${JOB_NS}" --ignore-not-found=true

log "Import done"
