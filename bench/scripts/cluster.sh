#!/usr/bin/env bash
# Usage: cluster.sh sleep | cluster.sh wake [N]
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${BENCH_DIR}/scripts/lib.sh"

IFS=_ read -r _ GKE_PROJECT GKE_ZONE GKE_CLUSTER <<< "${KCTX}"

case "${1:-}" in
  sleep)
    ACTIVE=$($KC get ns -o name 2>/dev/null | grep -E "e2e-|ra-ch-" | sed 's|namespace/||' || true)
    if [[ -n "${ACTIVE}" ]]; then
      log "WARNING: active namespaces will be evicted:"
      echo "${ACTIVE}" | sed 's/^/  /'
      read -rp "Press Enter to continue or Ctrl-C to abort. "
    fi
    log "Resizing ${GKE_CLUSTER} to 0 nodes"
    gcloud container clusters resize "${GKE_CLUSTER}" \
      --project "${GKE_PROJECT}" --zone "${GKE_ZONE}" --num-nodes 0 --quiet
    log "Cluster asleep (~\$2.40/day)."
    ;;
  wake)
    NODES="${2:-3}"
    log "Resizing ${GKE_CLUSTER} to ${NODES} nodes"
    gcloud container clusters resize "${GKE_CLUSTER}" \
      --project "${GKE_PROJECT}" --zone "${GKE_ZONE}" --num-nodes "${NODES}" --quiet
    $KC wait --for=condition=Ready nodes --all --timeout=300s
    log "Cluster awake (${NODES} nodes)."
    ;;
  *)
    echo "Usage: $0 sleep | wake [N]" >&2
    exit 1
    ;;
esac
