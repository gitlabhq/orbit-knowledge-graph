#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

log "Provisioning tier=${TIER} run=${RUN_ID}"

mkdir -p "${BENCH_DIR}/results/${RUN_ID}"

if [[ "${RA_EPHEMERAL_CLUSTER:-}" == "1" ]]; then
  log "Creating ephemeral cluster"
  "${BENCH_DIR}/scripts/create-cluster.sh"
  KCTX=$(cat "${BENCH_DIR}/results/${RUN_ID}/kctx")
  export KCTX
  KC="kubectl --context=${KCTX}"
  export KC
else
  MACHINE=$(tier ".nodes.machine")
  COUNT=$(tier ".nodes.count")
  POOL_NAME="ra-${RUN_ID}-${TIER}"

  log "Creating tainted node pool ${POOL_NAME} (${MACHINE} x${COUNT})"
  gcloud container node-pools create "${POOL_NAME}" \
    --cluster e2e-harness --zone us-central1-a \
    --machine-type "${MACHINE}" --num-nodes "${COUNT}" \
    --node-taints "ra-run=${RUN_ID}:NoSchedule" \
    --node-labels "ra-run=${RUN_ID}" \
    --labels "ttl-hours=12,run-id=${RUN_ID}" \
    --project gl-knowledgegraph-prj-f2eec59d \
    --quiet
fi

log "Rendering tier overlay"
"${BENCH_DIR}/scripts/render-tier.sh" > "/tmp/ra-${RUN_ID}-tier-values.yaml"

log "Deploying stack via e2e setup"
export E2E_SHA="${RUN_ID}"
export E2E_EXTRA_VALUES="/tmp/ra-${RUN_ID}-tier-values.yaml"
"${E2E_DIR}/scripts/setup.sh"

log "Running validation gate"
"${BENCH_DIR}/scripts/validate.sh"

log "Provision complete"
