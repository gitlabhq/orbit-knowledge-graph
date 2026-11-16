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

# Gate on the migrations Job, not the toolbox pod: toolbox goes Running
# before migrations finish and rails-runner crashes on an unmigrated DB.
MIGRATIONS=""
for _ in $(seq 1 360); do
  MIGRATIONS=$($KC get jobs -n "$NS_GITLAB" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null \
    | tr ' ' '\n' | grep -m1 'migrations' || true)
  [[ -n "$MIGRATIONS" ]] && break
  sleep 5
done
if [[ -z "$MIGRATIONS" ]]; then
  echo "GitLab migrations job not created in $NS_GITLAB after 1800s"
  exit 1
fi
$KC wait -n "$NS_GITLAB" "job/$MIGRATIONS" --for=condition=complete --timeout=600s || {
  echo "GitLab migrations job $MIGRATIONS not complete in $NS_GITLAB after 600s"
  exit 1
}

TOOLBOX=""
for _ in $(seq 1 60); do
  TOOLBOX=$($KC get pod -n "$NS_GITLAB" -l app=toolbox \
    --field-selector=status.phase=Running \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  [[ -n "$TOOLBOX" ]] && break
  sleep 5
done
if [[ -z "$TOOLBOX" ]]; then
  echo "No running toolbox pod found in $NS_GITLAB after 300s"
  exit 1
fi

# Status messages go to stderr; only the PAT lands on stdout so the bash
# capture below picks it up cleanly. Activation code is passed via ARGV so it
# never appears in the pod's process list.
# Retry up to 3 times — rails-runner can fail on transient connectivity issues
# when the webservice is still warming up.
PAT=""
for attempt in 1 2 3; do
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

# A seat link sync re-provisions add-ons from the real license, so provision after it finishes.
require 'sidekiq/api'
seat_link_pending = lambda do
  queued = Sidekiq::Queue.all.flat_map(&:to_a) + Sidekiq::ScheduledSet.new.to_a + Sidekiq::RetrySet.new.to_a
  queued.any? { |job| job.klass == 'SyncSeatLinkRequestWorker' } ||
    Sidekiq::Workers.new.any? { |_process, _thread, work| work.payload['class'] == 'SyncSeatLinkRequestWorker' }
end
seat_link_deadline = Time.current + 180
sleep 2 while seat_link_pending.call && Time.current < seat_link_deadline
abort 'SyncSeatLinkRequestWorker did not finish in 180s' if seat_link_pending.call

# The staging subscription has no Orbit add-on, so provision the one its license would carry.
orbit_promo = ::GitlabSubscriptions::AddOnPurchases::SelfManaged::ProvisionServices::OrbitPromo.new
orbit_promo.define_singleton_method(:license_restrictions) do
  { add_on_products: { orbit_promo: [{ quantity: 1, started_on: Date.current.to_s,
    expires_on: 1.year.from_now.to_date.to_s, purchase_xid: 'e2e', trial: false }] } }
end
provisioned = orbit_promo.execute
abort "orbit_promo provision failed: #{provisioned.message}" unless provisioned.success?
account = ::Analytics::Orbit::Billing::InstanceAccount.new
rows = ::GitlabSubscriptions::AddOnPurchase.for_self_managed.map do |purchase|
  purchase.attributes.slice('subscription_add_on_id', 'subscription_add_on_uid', 'started_at', 'expires_on', 'quantity')
end
warn "orbit entitlement: gate=#{account.send(:promo_gate?)} add_on=#{account.send(:promo_add_on?)} " \
  "entitled=#{account.entitled?} payload=#{provisioned.payload.inspect} purchases=#{rows.inspect}"
abort 'Orbit is not entitled after provisioning orbit_promo' unless account.entitled?

user = User.find_by(username: 'root') or abort 'no root user'
token = user.personal_access_tokens.find_or_create_by!(name: 'e2e-bootstrap') do |t|
  t.scopes = %w[api read_api admin_mode]
  t.expires_at = 30.days.from_now
end
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
  ) || true
  [[ -n "$PAT" && ${#PAT} -ge 20 ]] && break
  if [[ "$attempt" -lt 3 ]]; then
    log "  bootstrap attempt ${attempt} failed, retrying in 30s..."
    sleep 30
  fi
done

if [[ -z "$PAT" || ${#PAT} -lt 20 ]]; then
  echo "Failed to create root PAT after 3 attempts (got: '$PAT')"
  exit 1
fi

# Robot runner runs in NS_GKG; secrets are namespace-scoped so the PAT lives
# there directly.
$KC -n "$NS_GKG" create secret generic "$SECRET_NAME" \
  --from-literal=pat="$PAT" \
  --dry-run=client -o yaml | $KC apply -f -

log "License activated and root PAT stored in $NS_GKG/$SECRET_NAME"
