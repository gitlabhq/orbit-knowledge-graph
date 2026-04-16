#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

log "E2E Setup (SHA: $E2E_SHA)"

# Generate random credentials
log "Generating secrets"
export E2E_JWT_KEY=$(openssl rand -base64 32)
export E2E_CH_DEFAULT_PASS=$(openssl rand -base64 24)
export E2E_CH_SIPHON_PASS=$(openssl rand -base64 24)
export E2E_CH_DATALAKE_PASS=$(openssl rand -base64 24)
export E2E_CH_GRAPH_PASS=$(openssl rand -base64 24)
export E2E_CH_GRAPH_READ_PASS=$(openssl rand -base64 24)
export E2E_PG_SIPHON_PASS=$(openssl rand -base64 24)
export E2E_CH_GITLAB_PASS=$(openssl rand -base64 24)

# Root CA for gRPC TLS (pre-existing cluster resource from cert-manager)
log "Extracting root CA from cert-manager"
export E2E_ROOT_CA_B64=$($KC get secret root-ca-secret -n cert-manager \
  -o jsonpath='{.data.ca\.crt}')

# Deploy all components via helmfile (bootstrap → infra → pipeline)
log "Deploying via helmfile"
cd "$E2E_DIR"
helmfile --file helmfile.yaml.gotmpl sync

# Seed GitLab with test data (requires running GitLab instance)
source "$E2E_DIR/scripts/phases/07-seed-toolbox.sh"

log "Setup complete (SHA: $E2E_SHA)"
log "Run: E2E_SHA=$E2E_SHA scripts/test.sh"
