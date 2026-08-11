#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

log "Teardown for run=${RUN_ID}"

for suffix in gkg nats clickhouse gitlab siphon; do
  $KC delete ns "e2e-${RUN_ID}-${suffix}" --wait=false 2>/dev/null || true
done
$KC delete ns "ra-ch-${RUN_ID}" --wait=false 2>/dev/null || true

# Delete dedicated node pool if it exists.
if [[ "${RA_DEDICATED_POOL:-}" == "1" ]]; then
  IFS=_ read -r _ GKE_PROJECT GKE_ZONE GKE_CLUSTER <<< "${KCTX}"
  gcloud container node-pools delete "ra-${RUN_ID}" \
    --cluster "${GKE_CLUSTER}" --project "${GKE_PROJECT}" --zone "${GKE_ZONE}" \
    --quiet 2>/dev/null || true
fi

log "Teardown complete"
