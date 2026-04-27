#!/usr/bin/env bash
# Create a PAT for the root user via rails-runner inside toolbox and store it
# as a Secret. Replaces the OAuth password grant (ROPC) flow that was removed
# in GitLab 19.0 (https://about.gitlab.com/blog/a-guide-to-the-breaking-changes-in-gitlab-19-0/).
#
# The robot-runner job picks up this Secret as GITLAB_ROOT_PAT.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

SECRET_NAME="${SECRET_NAME:-gitlab-root-pat}"

log "Creating root PAT for SHA $E2E_SHA"

TOOLBOX=$($KC get pod -n "$NS_GITLAB" -l app=toolbox \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [[ -z "$TOOLBOX" ]]; then
  echo "No toolbox pod found in $NS_GITLAB"
  exit 1
fi

# admin_mode scope is required so the PAT can perform admin actions
# (creating users, issuing PATs for other users) even when admin mode is
# enforced. Output is the raw token on stdout, nothing else.
PAT=$($KC exec -i -n "$NS_GITLAB" "$TOOLBOX" -c toolbox -- \
  gitlab-rails runner -e production - <<'RUBY' | tail -1
user = User.find_by(username: 'root') or abort 'no root user'
token = user.personal_access_tokens.find_or_create_by!(name: 'e2e-bootstrap') do |t|
  t.scopes = %w[api read_api admin_mode]
  t.expires_at = 30.days.from_now
end
# Existing token's plaintext is unrecoverable; rotate if we got an old one.
if token.token.nil?
  token.destroy!
  token = user.personal_access_tokens.create!(
    name: 'e2e-bootstrap',
    scopes: %w[api read_api admin_mode],
    expires_at: 30.days.from_now,
  )
end
puts token.token
RUBY
)

if [[ -z "$PAT" || ${#PAT} -lt 20 ]]; then
  echo "Failed to create root PAT (got: '$PAT')"
  exit 1
fi

# Robot runner runs in NS_GKG; secrets are namespace-scoped so the PAT lives
# there directly.
$KC -n "$NS_GKG" create secret generic "$SECRET_NAME" \
  --from-literal=pat="$PAT" \
  --dry-run=client -o yaml | $KC apply -f -

log "Root PAT stored in $NS_GKG/$SECRET_NAME"
