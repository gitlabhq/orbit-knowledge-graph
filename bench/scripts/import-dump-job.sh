#!/usr/bin/env bash
# Submit an in-cluster Job that imports the datalake dump from GCS into
# the standalone ClickHouse. Tables already exist (created by Siphon
# one-shot in provision.sh). Data never leaves GCP.
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${BENCH_DIR}/scripts/lib.sh"

: "${DUMP_PREFIX:=core-2026-06-25-1115}"
: "${CH_NS:=ra-ch-${RUN_ID}}"

# Read credentials from the secret created by provision.sh.
CH_PASSWORD=$($KC get secret ra-ch-credentials -n "${CH_NS}" \
  -o jsonpath='{.data.default-password}' 2>/dev/null | base64 -d)
if [[ -z "${CH_PASSWORD}" ]]; then
  log "ERROR: ra-ch-credentials secret not found in ${CH_NS}"
  exit 1
fi

$KC create secret generic ra-import-ch-auth \
  -n "${CH_NS}" \
  --from-literal=password="${CH_PASSWORD}" \
  --dry-run=client -o yaml | $KC apply -f -

# Ensure the GCS-access KSA exists.
$KC create sa ra-import-sa -n "${CH_NS}" 2>/dev/null || true
$KC annotate sa ra-import-sa -n "${CH_NS}" \
  "iam.gke.io/gcp-service-account=1079327125344-compute@developer.gserviceaccount.com" \
  --overwrite 2>/dev/null

# Delete any previous Job.
$KC delete job ra-import-dump -n "${CH_NS}" --ignore-not-found=true 2>/dev/null

log "Submitting import job (dump=${DUMP_PREFIX}, ch=${CH_NS})"

CH_NAMESPACE="${CH_NS}" DUMP_PREFIX="${DUMP_PREFIX}" \
  envsubst < "${BENCH_DIR}/manifests/import-datalake-job.yaml" | $KC apply -n "${CH_NS}" -f -

log "Waiting for import job to start..."
$KC wait -n "${CH_NS}" job/ra-import-dump \
  --for=condition=ready --timeout=120s 2>/dev/null || true

log "Streaming logs (15-30 min expected)..."
$KC logs -n "${CH_NS}" job/ra-import-dump -f --tail=1000 2>/dev/null &
LOG_PID=$!

if $KC wait -n "${CH_NS}" job/ra-import-dump \
  --for=condition=complete --timeout=2700s 2>/dev/null; then
  log "Import job completed successfully"
else
  log "Import job failed or timed out"
  $KC describe job/ra-import-dump -n "${CH_NS}" 2>/dev/null || true
  kill "${LOG_PID}" 2>/dev/null || true
  exit 1
fi

kill "${LOG_PID}" 2>/dev/null || true

# Clean up auth secret.
$KC delete secret ra-import-ch-auth -n "${CH_NS}" --ignore-not-found=true

log "Import done"
