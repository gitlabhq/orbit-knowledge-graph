#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

log "E2E Setup (SHA: $E2E_SHA)"

if [[ -z "$E2E_EXTERNAL_GITLAB_NS" ]]; then
"$E2E_DIR/scripts/cleanup-stale-namespaces.sh"

log "Cleaning orphaned e2e cluster-scoped resources"
EXISTING_E2E_NS=$($KC get ns -o jsonpath='{.items[*].metadata.name}' 2>/dev/null \
  | tr ' ' '\n' | grep '^e2e-' || true)
for kind in gatewayclass clusterrole clusterrolebinding; do
  ORPHANS=$(EXISTING="$EXISTING_E2E_NS" $KC get "$kind" -o json 2>/dev/null \
    | python3 -c "
import json, os, sys
existing = set(os.environ.get('EXISTING','').split())
for r in json.load(sys.stdin).get('items', []):
    ns = r.get('metadata', {}).get('annotations', {}).get('meta.helm.sh/release-namespace', '')
    if ns.startswith('e2e-') and ns not in existing:
        print(r['metadata']['name'])
" 2>/dev/null) || true
  for orphan in $ORPHANS; do
    $KC delete "$kind" "$orphan" \
      --ignore-not-found=true --wait=false --timeout=30s 2>/dev/null || true
    $KC patch "$kind" "$orphan" --type=merge \
      -p '{"metadata":{"finalizers":[]}}' 2>/dev/null || true
    for _ in $(seq 1 30); do
      $KC get "$kind" "$orphan" >/dev/null 2>&1 || break
      sleep 1
    done
  done
done

fi

log "Generating secrets"
: "${E2E_JWT_KEY:=$(openssl rand -base64 32)}"
: "${E2E_CH_DEFAULT_PASS:=$(openssl rand -base64 24)}"
: "${E2E_CH_SIPHON_PASS:=$(openssl rand -base64 24)}"
: "${E2E_CH_DATALAKE_PASS:=$(openssl rand -base64 24)}"
: "${E2E_CH_GRAPH_PASS:=$(openssl rand -base64 24)}"
: "${E2E_CH_GRAPH_READ_PASS:=$(openssl rand -base64 24)}"
: "${E2E_PG_SIPHON_PASS:=$(openssl rand -base64 24)}"
: "${E2E_CH_GITLAB_PASS:=$(openssl rand -base64 24)}"
: "${E2E_GITLAB_ROOT_PASS:=$(openssl rand -base64 24)}"
: "${E2E_PG_GITLAB_PASS:=$(openssl rand -base64 24)}"
: "${E2E_PG_POSTGRES_PASS:=$(openssl rand -base64 24)}"
: "${E2E_PG_REPLICATION_PASS:=$(openssl rand -base64 24)}"
: "${E2E_REDIS_PASS:=$(openssl rand -base64 24)}"
export E2E_JWT_KEY E2E_CH_DEFAULT_PASS E2E_CH_SIPHON_PASS E2E_CH_DATALAKE_PASS
export E2E_CH_GRAPH_PASS E2E_CH_GRAPH_READ_PASS E2E_PG_SIPHON_PASS E2E_CH_GITLAB_PASS
export E2E_GITLAB_ROOT_PASS
export E2E_PG_GITLAB_PASS E2E_PG_POSTGRES_PASS E2E_PG_REPLICATION_PASS E2E_REDIS_PASS

log "Extracting root CA from cert-manager"
: "${E2E_ROOT_CA_B64:=$($KC get secret root-ca-secret -n cert-manager \
  -o jsonpath='{.data.ca\.crt}')}"
export E2E_ROOT_CA_B64

log "Syncing siphon CDC tables from SSOT"
"$E2E_DIR/scripts/sync-cdc-tables.sh"

if [[ -n "$E2E_EXTERNAL_GITLAB_NS" ]]; then
  true &
else
  "$E2E_DIR/scripts/bootstrap-instance.sh" &
fi
BOOTSTRAP_PID=$!
"$E2E_DIR/scripts/patch-ch-dicts.sh" &
DICTS_PID=$!
trap 'kill "$BOOTSTRAP_PID" "$DICTS_PID" 2>/dev/null || true' EXIT

log "Deploying via helmfile"
cd "$E2E_DIR"
helmfile --file helmfile.yaml.gotmpl sync

"$E2E_DIR/scripts/patch-ch-siphon-watermark.sh"

POST_SYNC_FAILED=0
wait "$BOOTSTRAP_PID" || { log "bootstrap-instance.sh failed"; POST_SYNC_FAILED=1; }
wait "$DICTS_PID" || { log "patch-ch-dicts.sh failed"; POST_SYNC_FAILED=1; }
trap - EXIT
[ "$POST_SYNC_FAILED" -eq 0 ]

"$E2E_DIR/scripts/ch-chaos.sh" backfill

log "Setup complete (SHA: $E2E_SHA)"
log "Run: E2E_SHA=$E2E_SHA scripts/test.sh"
