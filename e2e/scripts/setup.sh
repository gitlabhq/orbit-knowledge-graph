#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

log "E2E Setup (SHA: $E2E_SHA)"

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

# Shrink CACHE-layout LIFETIME on traversal-path dictionaries so the routes-
# vs-namespaces race window for new namespaces is sub-second instead of the
# upstream 60-300s. Must run after GitLab CH migrations created the dicts.
"$E2E_DIR/scripts/patch-ch-dicts.sh"

log "Setup complete (SHA: $E2E_SHA)"
log "Run: E2E_SHA=$E2E_SHA scripts/test.sh"
