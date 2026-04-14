#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

log "Phase 1: Generating secrets"

export E2E_JWT_KEY=$(openssl rand -base64 32)
export E2E_CH_DEFAULT_PASS=$(openssl rand -base64 24)
export E2E_CH_SIPHON_PASS=$(openssl rand -base64 24)
export E2E_CH_DATALAKE_PASS=$(openssl rand -base64 24)
export E2E_CH_GRAPH_PASS=$(openssl rand -base64 24)
export E2E_CH_GRAPH_READ_PASS=$(openssl rand -base64 24)
export E2E_PG_SIPHON_PASS=$(openssl rand -base64 24)
export E2E_CH_GITLAB_PASS=$(openssl rand -base64 24)

$KC create secret generic e2e-master-secrets -n "$NS_SECRETS" \
  --from-literal=jwt-key="$E2E_JWT_KEY" \
  --from-literal=ch-default-pass="$E2E_CH_DEFAULT_PASS" \
  --from-literal=ch-siphon-pass="$E2E_CH_SIPHON_PASS" \
  --from-literal=ch-datalake-pass="$E2E_CH_DATALAKE_PASS" \
  --from-literal=ch-graph-pass="$E2E_CH_GRAPH_PASS" \
  --from-literal=ch-graph-read-pass="$E2E_CH_GRAPH_READ_PASS" \
  --from-literal=pg-siphon-pass="$E2E_PG_SIPHON_PASS" \
  --from-literal=ch-gitlab-pass="$E2E_CH_GITLAB_PASS" \
  --dry-run=client -o yaml | $KC apply -f -

log "Copying root CA to GitLab namespace"
ROOT_CA=$($KC get secret root-ca-secret -n cert-manager -o jsonpath='{.data.ca\.crt}' | base64 -d)
$KC create secret generic gkg-grpc-ca -n "$NS_GITLAB" \
  --from-literal=gkg-grpc-ca.crt="$ROOT_CA" \
  --dry-run=client -o yaml | $KC apply -f -

$KC create secret generic gitlab-knowledge-graph-jwt -n "$NS_GITLAB" \
  --from-literal=knowledge_graph_jwt_shared_key="$E2E_JWT_KEY" \
  --dry-run=client -o yaml | $KC apply -f -

$KC create secret generic gitlab-clickhouse-password -n "$NS_GITLAB" \
  --from-literal=main_password="$E2E_CH_GITLAB_PASS" \
  --dry-run=client -o yaml | $KC apply -f -

$KC create secret generic gkg-secrets -n "$NS_GKG" \
  --from-literal=gitlab-jwt-verifying-key="$E2E_JWT_KEY" \
  --from-literal=gitlab-jwt-signing-key="$E2E_JWT_KEY" \
  --from-literal=datalake-password="$E2E_CH_DATALAKE_PASS" \
  --from-literal=graph-password="$E2E_CH_GRAPH_PASS" \
  --from-literal=graph-read-password="$E2E_CH_GRAPH_READ_PASS" \
  --dry-run=client -o yaml | $KC apply -f -
