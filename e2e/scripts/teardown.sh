#!/usr/bin/env bash
set -euo pipefail

KCTX="${KCTX:-gke_gl-knowledgegraph-prj-f2eec59d_us-central1-a_e2e-harness}"
KC="kubectl --context $KCTX"
_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
E2E_DIR="$(cd "$_SCRIPT_DIR/.." && pwd)"

CONFIRM=true
SHA=""

while [[ $# -gt 0 ]]; do
  case $1 in
    -y) CONFIRM=false; shift ;;
    --sha=*) SHA="${1#*=}"; shift ;;
    --sha) SHA="$2"; shift 2 ;;
    *) echo "Usage: $0 [-y] [--sha=<commit-sha>]"; exit 1 ;;
  esac
done

if [ -n "$SHA" ]; then
  PATTERN="^e2e-${SHA}-"
else
  PATTERN="^e2e-"
fi

E2E_NAMESPACES=($($KC get ns -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | grep "$PATTERN")) || true

if [ ${#E2E_NAMESPACES[@]} -eq 0 ]; then
  echo "No matching namespaces found (pattern: $PATTERN)."
  exit 0
fi

echo "Will tear down: ${E2E_NAMESPACES[*]}"

if $CONFIRM; then
  read -p "Confirm? [y/N] " -n 1 -r
  echo
  [[ $REPLY =~ ^[Yy]$ ]] || { echo "Cancelled."; exit 0; }
fi

if [ -n "$SHA" ]; then
  echo "==> Removing helm releases for SHA $SHA..."
  cd "$E2E_DIR"

  # Uninstall robot-runner if present
  helm uninstall e2e-robot-runner -n "e2e-${SHA}-gkg" --kube-context "$KCTX" 2>/dev/null || true

  # helmfile needs all env vars to parse .gotmpl; values don't matter for destroy
  E2E_SHA="$SHA" \
  E2E_JWT_KEY=x E2E_CH_DEFAULT_PASS=x E2E_CH_SIPHON_PASS=x \
  E2E_CH_DATALAKE_PASS=x E2E_CH_GRAPH_PASS=x E2E_CH_GRAPH_READ_PASS=x \
  E2E_PG_SIPHON_PASS=x E2E_CH_GITLAB_PASS=x E2E_ROOT_CA_B64=x \
  E2E_GITLAB_ROOT_PASS=x \
  E2E_PG_GITLAB_PASS=x E2E_PG_POSTGRES_PASS=x E2E_PG_REPLICATION_PASS=x \
  E2E_REDIS_PASS=x \
  helmfile --file helmfile.yaml.gotmpl destroy 2>/dev/null || true
else
  echo "==> No SHA specified, deleting namespaces directly (skipping helmfile destroy)"
fi

for ns in "${E2E_NAMESPACES[@]}"; do
  echo "==> Deleting namespace: $ns"
  $KC delete namespace "$ns" --wait=false 2>/dev/null || true
done

echo "==> Cleaning orphaned PVs..."
ORPHANED_PVS=$($KC get pv -o json 2>/dev/null | \
  python3 -c "
import json,sys
pvs = json.load(sys.stdin)['items']
for pv in pvs:
  ns = pv.get('spec',{}).get('claimRef',{}).get('namespace','')
  if ns.startswith('e2e-') and pv['status']['phase'] in ('Released','Failed'):
    print(pv['metadata']['name'])
" 2>/dev/null) || true

if [ -n "$ORPHANED_PVS" ]; then
  echo "$ORPHANED_PVS" | xargs $KC delete pv 2>/dev/null || true
fi

echo "Teardown complete."
