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

export NS_NATS="e2e-${E2E_SHA}-nats"
export NS_CH="e2e-${E2E_SHA}-clickhouse"
export NS_GITLAB="e2e-${E2E_SHA}-gitlab"
export NS_SIPHON="e2e-${E2E_SHA}-siphon"
export NS_GKG="e2e-${E2E_SHA}-gkg"

log() { echo "==> $*"; }
