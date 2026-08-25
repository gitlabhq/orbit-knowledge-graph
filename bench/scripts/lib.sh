#!/usr/bin/env bash
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "${BENCH_DIR}/.." && pwd)"
E2E_DIR="${REPO_ROOT}/e2e"
TF_DIR="${BENCH_DIR}/infra"
TF=$(mise which terraform 2>/dev/null || command -v terraform || true)

: "${RUN_ID:=ra-$(date +%s)}"
: "${TIER:=$(yq eval '.tier' "${BENCH_DIR}/config/bench.yaml")}"
: "${KCTX:=$(cd "${TF_DIR}" && "$TF" output -raw kctx 2>/dev/null || echo "")}"
if [[ -z "${KCTX}" ]]; then
  echo "ERROR: KCTX not set and terraform output unavailable. Run 'terraform apply' in bench/infra/ first." >&2
  exit 1
fi

export RUN_ID TIER KCTX BENCH_DIR REPO_ROOT E2E_DIR TF_DIR TF

KC="kubectl --context=${KCTX}"
export KC

log() { echo "[bench] $(date -u +%H:%M:%S) $*"; }

tier() {
  yq eval ".tiers.${TIER}${1}" "${BENCH_DIR}/config/tiers.yaml"
}

bench() {
  yq eval "${1}" "${BENCH_DIR}/config/bench.yaml"
}


