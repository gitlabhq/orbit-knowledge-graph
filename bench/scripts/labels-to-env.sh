#!/usr/bin/env bash
# Parse CI_MERGE_REQUEST_LABELS into bench env vars.
set -euo pipefail

LABELS="${CI_MERGE_REQUEST_LABELS:-}"

extract_label() {
  local prefix="$1"
  echo "${LABELS}" | tr ',' '\n' | sed -n "s/^${prefix}:://p" | head -1
}

TIER=$(extract_label "ra_tier")
RATE=$(extract_label "ra_rate")
QPS=$(extract_label "ra_qps")

[[ -n "${TIER}" ]] && export TIER
[[ -n "${RATE}" ]] && export RATE="${RATE}"
[[ -n "${QPS}" ]]  && export QPS_LADDER="${QPS}"

if echo "${LABELS}" | tr ',' '\n' | grep -qx "ra_skip_import"; then
  export RA_SKIP_IMPORT=1
fi
