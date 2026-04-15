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

# Verify scripts landed (kubectl cp over GitLab Agent can silently fail)
$KC exec -n "$NS_GITLAB" "$TOOLBOX_POD" -- ls /tmp/e2e-toolbox/ >/dev/null

# 1. Create test user and PAT
# 2>/dev/null suppresses Rails boot noise that would pollute stdout capture
log "Running create_user_and_pat.rb"
E2E_BOT_PASS=$(openssl rand -base64 24)
PAT=$($KC exec -n "$NS_GITLAB" "$TOOLBOX_POD" -- \
  env E2E_BOT_PASS="$E2E_BOT_PASS" \
  gitlab-rails runner /tmp/e2e-toolbox/create_user_and_pat.rb 2>/dev/null)

if [[ -z "$PAT" ]]; then
  log "ERROR: PAT creation failed, retrying with visible stderr..."
  PAT=$($KC exec -n "$NS_GITLAB" "$TOOLBOX_POD" -- \
    env E2E_BOT_PASS="$E2E_BOT_PASS" \
    gitlab-rails runner /tmp/e2e-toolbox/create_user_and_pat.rb)
  if [[ -z "$PAT" ]]; then
    log "ERROR: PAT creation failed on retry"
    exit 1
  fi
fi

log "PAT created, storing in secret"
$KC create secret generic e2e-test-credentials -n "$NS_GKG" \
  --from-literal=gitlab-pat="$PAT" \
  --from-literal=gitlab-url="http://gitlab-webservice-default.${NS_GITLAB}.svc.cluster.local:8181" \
  --from-literal=gkg-url="http://gkg-webserver.${NS_GKG}.svc.cluster.local:8080" \
  --dry-run=client -o yaml | $KC apply -f -

# 2. Enable feature flags (no stdout capture, let stderr through for debugging)
log "Running enable_feature_flags.rb"
$KC exec -n "$NS_GITLAB" "$TOOLBOX_POD" -- \
  gitlab-rails runner /tmp/e2e-toolbox/enable_feature_flags.rb
