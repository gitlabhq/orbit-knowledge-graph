#!/usr/bin/env bash
# Resize the bench cluster to 0 nodes. PVCs, secrets, and config survive.
# Cost asleep: ~$2.40/day (GKE control-plane) vs ~$19/day running.
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${BENCH_DIR}/scripts/lib.sh"

# Parse project/zone/cluster from the KCTX (gke_PROJECT_ZONE_CLUSTER).
IFS=_ read -r _ GKE_PROJECT GKE_ZONE GKE_CLUSTER <<< "${KCTX}"

ACTIVE=$($KC get ns -o name 2>/dev/null \
  | grep -E "e2e-|ra-ch-" \
  | sed 's|namespace/||' || true)

if [[ -n "${ACTIVE}" ]]; then
  log "WARNING: active bench namespaces found:"
  echo "${ACTIVE}" | sed 's/^/  /'
  log "Sleeping with active runs means pods will be evicted."
  log "Run teardown.sh first, or press Enter to continue."
  read -r
fi

log "Resizing ${GKE_CLUSTER} to 0 nodes"
gcloud container clusters resize "${GKE_CLUSTER}" \
  --project "${GKE_PROJECT}" \
  --zone "${GKE_ZONE}" \
  --num-nodes 0 \
  --quiet

log "Cluster asleep. Run cluster-wake.sh to resume."
