#!/usr/bin/env bash
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "${BENCH_DIR}/.." && pwd)"
E2E_DIR="${REPO_ROOT}/e2e"
TF_DIR="${BENCH_DIR}/infra"

: "${RUN_ID:=ra-$(date +%s)}"
: "${TIER:=small}"
: "${KCTX:=$(cd "${TF_DIR}" && terraform output -raw kctx 2>/dev/null || echo "")}"
if [[ -z "${KCTX}" ]]; then
  echo "ERROR: KCTX not set and terraform output unavailable. Run 'terraform apply' in bench/infra/ first." >&2
  exit 1
fi

export RUN_ID TIER KCTX BENCH_DIR REPO_ROOT E2E_DIR TF_DIR

KC="kubectl --context=${KCTX}"
export KC

log() { echo "[bench] $(date -u +%H:%M:%S) $*"; }

tier() {
  yq eval ".tiers.${TIER}${1}" "${BENCH_DIR}/config/tiers.yaml"
}

bench() {
  yq eval "${1}" "${BENCH_DIR}/config/bench.yaml"
}

heartbeat_start() {
  ( while true; do sleep 30; echo "."; done ) &
  HEARTBEAT_PID=$!
  export HEARTBEAT_PID
}

heartbeat_stop() {
  if [[ -n "${HEARTBEAT_PID:-}" ]]; then
    kill "${HEARTBEAT_PID}" 2>/dev/null || true
    wait "${HEARTBEAT_PID}" 2>/dev/null || true
    unset HEARTBEAT_PID
  fi
}
