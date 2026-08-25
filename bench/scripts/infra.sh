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

TF=$(mise which terraform 2>/dev/null \
  || command -v terraform 2>/dev/null \
  || echo "$(mise where terraform 2>/dev/null)/terraform")
if [[ ! -x "${TF}" ]]; then
  echo "ERROR: terraform not found. Install it or run 'mise install terraform'." >&2
  exit 1
fi

STATE_BUCKET=$(bench '.buckets.tf_state')
: "${TIER:=$(bench '.tier')}"

fetch_credentials() {
  echo "[infra] Fetching kubectl credentials"
  gcloud container clusters get-credentials \
    "$("$TF" -chdir="${TF_DIR}" output -raw cluster_name)" \
    --zone "$("$TF" -chdir="${TF_DIR}" output -raw cluster_location)" \
    --project "$("$TF" -chdir="${TF_DIR}" output -raw project)" \
    2>/dev/null
}

case "${1:-}" in
  init)
    shift
    if ! gcloud storage buckets describe "gs://${STATE_BUCKET}" --project="$(bench '.project')" >/dev/null 2>&1; then
      echo "ERROR: State bucket gs://${STATE_BUCKET} does not exist." >&2
      echo "       Run 'terraform apply' in bench/infra/bootstrap/ first." >&2
      exit 1
    fi
    "$TF" -chdir="${TF_DIR}" init \
      -backend-config="bucket=${STATE_BUCKET}" \
      -backend-config="prefix=bench" \
      "$@"
    ;;
  apply)
    shift
    "$TF" -chdir="${TF_DIR}" apply -var "tier=${TIER}" "$@"
    fetch_credentials
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
