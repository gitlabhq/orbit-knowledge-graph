#!/usr/bin/env bash
# Delete the ephemeral RA bench cluster.
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${BENCH_DIR}/scripts/lib.sh"

CLUSTER_NAME="ra-bench-${RUN_ID}"
: "${PROJECT:=gl-knowledgegraph-prj-f2eec59d}"
: "${ZONE:=us-central1-a}"

log "Deleting cluster ${CLUSTER_NAME}"

gcloud container clusters delete "${CLUSTER_NAME}" \
  --project "${PROJECT}" \
  --zone "${ZONE}" \
  --quiet 2>/dev/null || log "Cluster ${CLUSTER_NAME} not found or already deleted"

log "Cluster deleted"
