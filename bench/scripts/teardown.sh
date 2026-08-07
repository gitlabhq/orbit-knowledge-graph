#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

log "Teardown for run=${RUN_ID}"

# Read KCTX from the run dir if an ephemeral cluster was used.
if [[ -f "${BENCH_DIR}/results/${RUN_ID}/kctx" ]]; then
  KCTX=$(cat "${BENCH_DIR}/results/${RUN_ID}/kctx")
  export KCTX
  KC="kubectl --context=${KCTX}"
  export KC
fi

# Delete e2e namespaces.
for suffix in gkg nats clickhouse gitlab siphon; do
  $KC delete ns "e2e-${RUN_ID}-${suffix}" --wait=false 2>/dev/null || true
done

# Delete namespaces by label (never by grep).
NS_LIST=$($KC get ns -l "ra-run=${RUN_ID}" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)
for ns in ${NS_LIST}; do
  log "  Deleting namespace ${ns}"
  $KC delete ns "${ns}" --wait=false 2>/dev/null || true
done

if [[ "${RA_EPHEMERAL_CLUSTER:-}" == "1" ]]; then
  log "Deleting ephemeral cluster"
  "${BENCH_DIR}/scripts/delete-cluster.sh"
else
  POOL_NAME="ra-${RUN_ID}-${TIER}"
  log "  Deleting node pool ${POOL_NAME}"
  gcloud container node-pools delete "${POOL_NAME}" \
    --cluster e2e-harness --zone us-central1-a \
    --project gl-knowledgegraph-prj-f2eec59d \
    --quiet 2>/dev/null || log "  Pool ${POOL_NAME} not found or already deleted"
fi

log "Teardown complete"
