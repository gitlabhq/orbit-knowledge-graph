#!/usr/bin/env bash
# Activate the e2e GitLab instance with a cloud license from the staging
# customer portal.
#
# Prereqs (already in place by the time helmfile sync finishes):
#   - global.extraEnv on the gitlab chart sets GITLAB_LICENSE_MODE=test and
#     CUSTOMER_PORTAL_URL=https://customers.staging.gitlab.com on every rails
#     component, so the staging-signed license decrypts and the activation
#     client talks to staging.
#   - The activation code lives in the `gitlab-license-code` secret in the
#     `gitlab-agent-e2e-harness` namespace (synced from GCP Secret Manager via
#     ExternalSecret), key `code`.
#
# Idempotent: skips activation if License.current is already a non-expired
# cloud license.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

LICENSE_NS="${LICENSE_NS:-gitlab-agent-e2e-harness}"
LICENSE_SECRET="${LICENSE_SECRET:-gitlab-license-code}"
LICENSE_KEY="${LICENSE_KEY:-code}"

log "Activating GitLab license for SHA $E2E_SHA"

ACTIVATION_CODE=$($KC get secret "$LICENSE_SECRET" -n "$LICENSE_NS" \
  -o jsonpath="{.data.${LICENSE_KEY}}" 2>/dev/null | base64 -d)
if [[ -z "$ACTIVATION_CODE" ]]; then
  echo "Missing activation code at ${LICENSE_NS}/${LICENSE_SECRET}.${LICENSE_KEY}"
  exit 1
fi

# Toolbox is the only rails-capable pod that survives a full sync; webservice
# and sidekiq also work but toolbox has no ingress traffic to interfere with.
TOOLBOX=$($KC get pod -n "$NS_GITLAB" -l app=toolbox \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [[ -z "$TOOLBOX" ]]; then
  echo "No toolbox pod found in $NS_GITLAB"
  exit 1
fi

# Pass the activation code as the script's sole argument so it never appears
# in the pod's process list (ARGV is read inside the runner, not logged by kubectl).
$KC exec -i -n "$NS_GITLAB" "$TOOLBOX" -c toolbox -- \
  gitlab-rails runner - "$ACTIVATION_CODE" <<'RUBY'
code = ARGV.first.to_s.strip
abort "missing activation code" if code.empty?

current = ::License.current
if current && current.cloud_license? && !current.expired?
  puts "already activated: plan=#{current.plan} expires=#{current.expires_at}"
  exit 0
end

result = ::GitlabSubscriptions::ActivateService.new.execute(code)
unless result[:success]
  abort "activation failed: #{result[:errors].inspect}"
end

l = ::License.current
puts "activated: plan=#{l.plan} expires=#{l.expires_at} cloud=#{l.cloud_license?}"
RUBY

log "License activated"
