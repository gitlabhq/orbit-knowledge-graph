#!/usr/bin/env bash
# Lifecycle wrapper for bench Terraform. Reads all config from bench.yaml.
#
# Reads tier, state bucket, and all config from bench.yaml.
# Override tier with TIER env var or -var tier=... passthrough.
#
# Usage:
#   bench/scripts/infra.sh init
#   bench/scripts/infra.sh apply                          # tier from bench.yaml
#   bench/scripts/infra.sh apply -var cluster_name=custom
#   bench/scripts/infra.sh destroy
#   bench/scripts/infra.sh output cluster_name
#   bench/scripts/infra.sh plan
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "${BENCH_DIR}/.." && pwd)"
TF_DIR="${BENCH_DIR}/infra"

bench() { yq eval "${1}" "${BENCH_DIR}/config/bench.yaml"; }

TF=$(mise which terraform 2>/dev/null || command -v terraform)
if [[ -z "${TF}" ]]; then
  echo "ERROR: terraform not found. Install it or run 'mise install terraform'." >&2
  exit 1
fi

STATE_BUCKET=$(bench '.buckets.tf_state')
: "${TIER:=$(bench '.tier')}"

case "${1:-}" in
  init)
    shift
    "$TF" -chdir="${TF_DIR}" init \
      -backend-config="bucket=${STATE_BUCKET}" \
      -backend-config="prefix=bench" \
      "$@"
    ;;
  apply)
    shift
    "$TF" -chdir="${TF_DIR}" apply -var "tier=${TIER}" "$@"
    ;;
  plan)
    shift
    "$TF" -chdir="${TF_DIR}" plan -var "tier=${TIER}" "$@"
    ;;
  destroy)
    shift
    "$TF" -chdir="${TF_DIR}" destroy "$@"
    ;;
  output)
    shift
    "$TF" -chdir="${TF_DIR}" output "$@"
    ;;
  *)
    echo "Usage: $0 init | apply | plan | destroy | output [args...]" >&2
    exit 1
    ;;
esac
