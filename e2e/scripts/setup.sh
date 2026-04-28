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
  if [ -n "$ORPHANS" ]; then
    # --wait=false + --timeout=30s: gateway-api controllers can hold finalizers
    # for minutes while cleaning attached routes; without these, kubectl delete
    # blocks until the resource is gone and exhausted the 1h job budget once.
    echo "$ORPHANS" | xargs -I{} $KC delete "$kind" "{}" \
      --ignore-not-found=true --wait=false --timeout=30s 2>/dev/null || true
  fi
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

# Activate GitLab with the cloud license from the staging customer portal so
# EE-gated features (epics, work item hierarchies, etc.) are available in the
# test suite. Runs after helmfile sync because it requires a Ready toolbox pod.
"$E2E_DIR/scripts/activate-license.sh"

# Create a root PAT via rails-runner. Replaces the OAuth password grant (ROPC)
# flow that GitLab 19.0 removed. Robot runner reads the PAT from the
# `gitlab-root-pat` secret as the GITLAB_ROOT_PAT env var.
"$E2E_DIR/scripts/create-root-pat.sh"

# Shrink CACHE-layout LIFETIME on traversal-path dictionaries so the routes-
# vs-namespaces race window for new namespaces is sub-second instead of the
# upstream 60-300s. Must run after GitLab CH migrations created the dicts.
"$E2E_DIR/scripts/patch-ch-dicts.sh"

log "Setup complete (SHA: $E2E_SHA)"
log "Run: E2E_SHA=$E2E_SHA scripts/test.sh"
