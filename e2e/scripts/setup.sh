#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

log "E2E Setup (SHA: $E2E_SHA)"

# Clean cluster-scoped resources orphaned by previous e2e runs whose owning
# namespace has been torn down. The GitLab chart 9.11.x installs cluster-scoped
# resources (e.g. GatewayClass "gitlab-gw") that survive `kubectl delete ns`
# and that helm validates by `meta.helm.sh/release-namespace` annotation;
# a stale entry from a prior e2e-<oldsha>-gitlab release blocks subsequent
# installs in any new e2e-<sha>-gitlab namespace with:
#   "GatewayClass <name> exists and cannot be imported into the current release"
log "Cleaning orphaned e2e cluster-scoped resources"
EXISTING_E2E_NS=$($KC get ns -o jsonpath='{.items[*].metadata.name}' 2>/dev/null \
  | tr ' ' '\n' | grep '^e2e-' || true)
for kind in gatewayclass; do
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
    # 1. Mark for deletion (--wait=false avoids hanging on finalizers).
    $KC delete "$kind" "$orphan" \
      --ignore-not-found=true --wait=false --timeout=30s 2>/dev/null || true
    # 2. Strip finalizers so the API server actually removes the object;
    #    gateway-api/envoy-gateway controllers from a torn-down namespace
    #    are gone, so their finalizers will never reconcile on their own.
    $KC patch "$kind" "$orphan" --type=merge \
      -p '{"metadata":{"finalizers":[]}}' 2>/dev/null || true
    # 3. Poll up to 30s for actual removal — helm install fails if the
    #    object is still present (even with deletionTimestamp set).
    for _ in $(seq 1 30); do
      $KC get "$kind" "$orphan" >/dev/null 2>&1 || break
      sleep 1
    done
  done
done

# Generate random credentials
log "Generating secrets"
E2E_JWT_KEY=$(openssl rand -base64 32)
E2E_CH_DEFAULT_PASS=$(openssl rand -base64 24)
E2E_CH_SIPHON_PASS=$(openssl rand -base64 24)
E2E_CH_DATALAKE_PASS=$(openssl rand -base64 24)
E2E_CH_GRAPH_PASS=$(openssl rand -base64 24)
E2E_CH_GRAPH_READ_PASS=$(openssl rand -base64 24)
E2E_PG_SIPHON_PASS=$(openssl rand -base64 24)
E2E_CH_GITLAB_PASS=$(openssl rand -base64 24)
E2E_GITLAB_ROOT_PASS=$(openssl rand -base64 24)
E2E_PG_GITLAB_PASS=$(openssl rand -base64 24)
E2E_PG_POSTGRES_PASS=$(openssl rand -base64 24)
E2E_PG_REPLICATION_PASS=$(openssl rand -base64 24)
E2E_REDIS_PASS=$(openssl rand -base64 24)
export E2E_JWT_KEY E2E_CH_DEFAULT_PASS E2E_CH_SIPHON_PASS E2E_CH_DATALAKE_PASS
export E2E_CH_GRAPH_PASS E2E_CH_GRAPH_READ_PASS E2E_PG_SIPHON_PASS E2E_CH_GITLAB_PASS
export E2E_GITLAB_ROOT_PASS
export E2E_PG_GITLAB_PASS E2E_PG_POSTGRES_PASS E2E_PG_REPLICATION_PASS E2E_REDIS_PASS

# Root CA for gRPC TLS (pre-existing cluster resource from cert-manager)
log "Extracting root CA from cert-manager"
export E2E_ROOT_CA_B64=$($KC get secret root-ca-secret -n cert-manager \
  -o jsonpath='{.data.ca\.crt}')

# Regenerate siphon CDC config from gitlab-org/gitlab SSOT at the pinned ref.
# Output (cdc-producer.yaml, cdc-consumer.yaml) is consumed by values/siphon.yaml.gotmpl.
log "Syncing siphon CDC tables from SSOT"
"$E2E_DIR/scripts/sync-cdc-tables.sh"

# Deploy all components via helmfile (bootstrap → infra → pipeline)
log "Deploying via helmfile"
cd "$E2E_DIR"
helmfile --file helmfile.yaml.gotmpl sync

# Activate GitLab with the cloud license from the staging customer portal and
# create a root PAT in a single rails-runner call (each call cold-boots Rails,
# so combining halves the boot cost). The license unlocks EE-gated features
# (epics, work item hierarchies); the PAT replaces the ROPC flow that GitLab
# 19.0 removed and is read by robot runner as GITLAB_ROOT_PAT.
"$E2E_DIR/scripts/bootstrap-instance.sh"

# Shrink CACHE-layout LIFETIME on traversal-path dictionaries so the routes-
# vs-namespaces race window for new namespaces is sub-second instead of the
# upstream 60-300s. Must run after GitLab CH migrations created the dicts.
"$E2E_DIR/scripts/patch-ch-dicts.sh"

log "Setup complete (SHA: $E2E_SHA)"
log "Run: E2E_SHA=$E2E_SHA scripts/test.sh"
