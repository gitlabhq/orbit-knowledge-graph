#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

log "Phase 7: Seeding GitLab (test user, PAT, feature flags)"

TOOLBOX_POD=$($KC get pods -n "$NS_GITLAB" -l app=toolbox \
  -o jsonpath='{.items[0].metadata.name}')

if [[ -z "$TOOLBOX_POD" ]]; then
  log "ERROR: toolbox pod not found"
  exit 1
fi

log "Copying toolbox scripts to pod"
$KC cp "$E2E_DIR/toolbox/" "$NS_GITLAB/$TOOLBOX_POD:/tmp/e2e-toolbox"

# 1. Create test user and PAT
log "Running create_user_and_pat.rb"
E2E_BOT_PASS=$(openssl rand -base64 24)
PAT=$($KC exec -n "$NS_GITLAB" "$TOOLBOX_POD" -- \
  env E2E_BOT_PASS="$E2E_BOT_PASS" \
  gitlab-rails runner /tmp/e2e-toolbox/create_user_and_pat.rb 2>/dev/null)

if [[ -z "$PAT" ]]; then
  log "ERROR: PAT creation failed"
  exit 1
fi

log "PAT created, storing in secret"
$KC create secret generic e2e-test-credentials -n "$NS_GKG" \
  --from-literal=gitlab-pat="$PAT" \
  --from-literal=gitlab-url="http://gitlab-webservice-default.${NS_GITLAB}.svc.cluster.local:8181" \
  --from-literal=gkg-url="http://gkg-webserver.${NS_GKG}.svc.cluster.local:8080" \
  --dry-run=client -o yaml | $KC apply -f -

# 2. Enable feature flags
log "Running enable_feature_flags.rb"
$KC exec -n "$NS_GITLAB" "$TOOLBOX_POD" -- \
  gitlab-rails runner /tmp/e2e-toolbox/enable_feature_flags.rb 2>/dev/null
