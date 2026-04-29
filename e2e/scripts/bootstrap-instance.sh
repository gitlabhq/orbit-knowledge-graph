#!/usr/bin/env bash
# Bootstrap the e2e GitLab instance: activate the license and mint a root PAT
# in a single gitlab-rails runner invocation.
#
# Each `gitlab-rails runner` call cold-boots the entire Rails app (~60s on the
# toolbox image). Doing both operations in one invocation saves one full boot
# on every e2e setup.
#
# Prereqs (in place by the time helmfile sync finishes):
#   - global.extraEnv on the gitlab chart sets GITLAB_LICENSE_MODE=test and
#     CUSTOMER_PORTAL_URL=https://customers.staging.gitlab.com on every Rails
#     component, so the staging-signed license decrypts and the activation
#     client talks to staging.
#   - Activation code lives in the `gitlab-license-code` secret in the
#     `gitlab-agent-e2e-harness` namespace (synced from GCP Secret Manager via
#     ExternalSecret), key `code`.
#
# Idempotent: re-activation is skipped when License.current is a non-expired
# cloud license; PAT creation uses find_or_create_by and rotates when the
# plaintext token is unrecoverable.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

LICENSE_NS="${LICENSE_NS:-gitlab-agent-e2e-harness}"
LICENSE_SECRET="${LICENSE_SECRET:-gitlab-license-code}"
LICENSE_KEY="${LICENSE_KEY:-code}"
SECRET_NAME="${SECRET_NAME:-gitlab-root-pat}"

log "Bootstrapping GitLab instance for SHA $E2E_SHA"

ACTIVATION_CODE=$($KC get secret "$LICENSE_SECRET" -n "$LICENSE_NS" \
  -o jsonpath="{.data.${LICENSE_KEY}}" 2>/dev/null | base64 -d)
if [[ -z "$ACTIVATION_CODE" ]]; then
  echo "Missing activation code at ${LICENSE_NS}/${LICENSE_SECRET}.${LICENSE_KEY}"
  exit 1
fi

TOOLBOX=$($KC get pod -n "$NS_GITLAB" -l app=toolbox \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [[ -z "$TOOLBOX" ]]; then
  echo "No toolbox pod found in $NS_GITLAB"
  exit 1
fi

# Status messages go to stderr; only the PAT lands on stdout so the bash
# capture below picks it up cleanly. Activation code is passed via ARGV so it
# never appears in the pod's process list.
PAT=$($KC exec -i -n "$NS_GITLAB" "$TOOLBOX" -c toolbox -- \
  gitlab-rails runner -e production - "$ACTIVATION_CODE" <<'RUBY' | tail -1
code = ARGV.first.to_s.strip
abort 'missing activation code' if code.empty?

current = ::License.current
if current && current.cloud_license? && !current.expired?
  warn "license already active: plan=#{current.plan} expires=#{current.expires_at}"
else
  result = ::GitlabSubscriptions::ActivateService.new.execute(code)
  abort "activation failed: #{result[:errors].inspect}" unless result[:success]
  l = ::License.current
  warn "license activated: plan=#{l.plan} expires=#{l.expires_at} cloud=#{l.cloud_license?}"
end

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

log "License activated and root PAT stored in $NS_GKG/$SECRET_NAME"
