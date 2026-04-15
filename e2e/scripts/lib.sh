#!/usr/bin/env bash
# Shared variables and functions for e2e scripts. Source, don't execute.
set -euo pipefail

_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export E2E_DIR="$(cd "$_LIB_DIR/.." && pwd)"
export GKG_ROOT="$(cd "$E2E_DIR/.." && pwd)"

export KCTX="${KCTX:-gke_gl-knowledgegraph-prj-f2eec59d_us-central1-a_e2e-harness}"
KC="kubectl --context $KCTX"

export E2E_SHA="${E2E_SHA:-$(git -C "$GKG_ROOT" rev-parse --short=7 HEAD 2>/dev/null || true)}"
if [[ -z "$E2E_SHA" ]]; then
  echo "E2E_SHA is required (set it or run from a git repo)"
  exit 1
fi

export NS_SECRETS="e2e-${E2E_SHA}-secrets"
export NS_NATS="e2e-${E2E_SHA}-nats"
export NS_CH="e2e-${E2E_SHA}-clickhouse"
export NS_GITLAB="e2e-${E2E_SHA}-gitlab"
export NS_SIPHON="e2e-${E2E_SHA}-siphon"
export NS_GKG="e2e-${E2E_SHA}-gkg"

log() { echo "==> $*"; }

cdc_table_names() {
  python3 -c "
import re, sys
text = open(sys.argv[1]).read()
print('\n'.join(re.findall(r'- name: (\S+)', text)))
" "$E2E_DIR/config/cdc-tables.yaml"
}

wait_for_pods() {
  local ns=$1 timeout=${2:-300}
  log "Waiting for pods in $ns (timeout: ${timeout}s)"
  $KC wait --for=condition=Ready pods \
    --field-selector=status.phase!=Succeeded,status.phase!=Failed \
    --all -n "$ns" --timeout="${timeout}s" 2>/dev/null || {
    log "Warning: not all pods in $ns became ready within ${timeout}s"
    $KC get pods -n "$ns" --no-headers 2>/dev/null
  }
  $KC get pods -n "$ns" --no-headers 2>/dev/null
}
