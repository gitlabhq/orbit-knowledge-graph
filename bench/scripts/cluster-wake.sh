#!/usr/bin/env bash
# Resize the bench cluster back to N nodes (default 3).
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${BENCH_DIR}/scripts/lib.sh"

NODES="${1:-3}"

IFS=_ read -r _ GKE_PROJECT GKE_ZONE GKE_CLUSTER <<< "${KCTX}"

log "Resizing ${GKE_CLUSTER} to ${NODES} nodes"
gcloud container clusters resize "${GKE_CLUSTER}" \
  --project "${GKE_PROJECT}" \
  --zone "${GKE_ZONE}" \
  --num-nodes "${NODES}" \
  --quiet

log "Waiting for nodes to be ready..."
$KC wait --for=condition=Ready nodes --all --timeout=300s

log "Cluster awake (${NODES} nodes)."
