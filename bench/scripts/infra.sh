#!/usr/bin/env bash
# Lifecycle wrapper for bench Terraform. Reads all config from bench.yaml.
#
# Usage:
#   bench/scripts/infra.sh init
#   bench/scripts/infra.sh apply                          # uses TIER (default: small)
#   bench/scripts/infra.sh apply -var tier=large
#   bench/scripts/infra.sh apply -var cluster_name=custom
#   bench/scripts/infra.sh destroy
#   bench/scripts/infra.sh output cluster_name
#   bench/scripts/infra.sh plan
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF_DIR="${BENCH_DIR}/infra"

bench() { yq eval "${1}" "${BENCH_DIR}/config/bench.yaml"; }

STATE_BUCKET=$(bench '.buckets.tf_state')
: "${TIER:=small}"

case "${1:-}" in
  init)
    shift
    terraform -chdir="${TF_DIR}" init \
      -backend-config="bucket=${STATE_BUCKET}" \
      -backend-config="prefix=bench" \
      "$@"
    ;;
  apply)
    shift
    terraform -chdir="${TF_DIR}" apply -var "tier=${TIER}" "$@"
    ;;
  plan)
    shift
    terraform -chdir="${TF_DIR}" plan -var "tier=${TIER}" "$@"
    ;;
  destroy)
    shift
    terraform -chdir="${TF_DIR}" destroy "$@"
    ;;
  output)
    shift
    terraform -chdir="${TF_DIR}" output "$@"
    ;;
  *)
    echo "Usage: $0 init | apply | plan | destroy | output [args...]" >&2
    exit 1
    ;;
esac
