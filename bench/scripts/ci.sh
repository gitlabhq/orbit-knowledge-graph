#!/usr/bin/env bash
# End-to-end bench run for CI (non-interactive). Chains the existing bench
# scripts: optionally create the cluster, provision from a snapshot, generate
# query load, then score SLOs. Optionally tear the cluster down afterwards.
#
# GCP auth (gcloud) must already be done by the caller. This script only
# orchestrates; it does not authenticate.
#
# Required:
#   RUN_ID                 unique run identifier
#   RA_DATALAKE_SNAPSHOT   golden snapshot to provision from
# Optional (defaults shown):
#   TIER=small             overrides bench.yaml
#   CREATE_CLUSTER=false   run infra.sh apply before provisioning
#   DEPLOY_MOCK=false      deploy the mock git server for code indexing
#   INDEX_WAIT_SECS=1800   wait after provision before scoring
#   TEARDOWN=false         run infra.sh destroy at the end
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

: "${RUN_ID:?RUN_ID is required}"
: "${RA_DATALAKE_SNAPSHOT:?RA_DATALAKE_SNAPSHOT is required (golden snapshot name)}"
: "${TIER:=small}"
: "${CREATE_CLUSTER:=false}"
: "${DEPLOY_MOCK:=false}"
: "${INDEX_WAIT_SECS:=1800}"
: "${TEARDOWN:=false}"
export RUN_ID TIER RA_DATALAKE_SNAPSHOT

log() { echo "[ci] $(date -u +%H:%M:%S) $*"; }

teardown() {
  if [[ "${TEARDOWN}" == "true" ]]; then
    log "Tearing down cluster"
    bash "${BENCH_DIR}/scripts/infra.sh" destroy -auto-approve || log "WARN: destroy failed"
  else
    log "TEARDOWN=false; leaving cluster running"
  fi
}
trap teardown EXIT

if [[ "${CREATE_CLUSTER}" == "true" ]]; then
  log "Creating cluster (tier=${TIER})"
  bash "${BENCH_DIR}/scripts/infra.sh" init
  bash "${BENCH_DIR}/scripts/infra.sh" apply -auto-approve
fi

log "Provisioning from snapshot ${RA_DATALAKE_SNAPSHOT}"
bash "${BENCH_DIR}/scripts/provision.sh"

if [[ "${DEPLOY_MOCK}" == "true" ]]; then
  log "Deploying mock git server for code indexing"
  bash "${BENCH_DIR}/scripts/deploy-mock-git-server.sh"
fi

log "Waiting ${INDEX_WAIT_SECS}s for indexing to reach steady state"
sleep "${INDEX_WAIT_SECS}"

log "Generating query load"
bash "${BENCH_DIR}/scripts/query-load.sh"

log "Scoring SLOs"
bash "${BENCH_DIR}/scripts/slos.sh" | tee "${BENCH_DIR}/results/${RUN_ID}/slos-${TIER}.txt"
