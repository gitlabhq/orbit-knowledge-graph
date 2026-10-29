#!/bin/sh
set -eu

image=${1:?Usage: check-billing-enforcement.sh image@sha256:digest}
if ! printf '%s' "$image" | grep -Eq '@sha256:[0-9a-f]{64}$'; then
  echo "Use an immutable image digest" >&2
  exit 1
fi

workdir=$(mktemp -d)
container=
cleanup() {
  if [ -n "$container" ]; then docker rm -f "$container" >/dev/null 2>&1 || true; fi
  rm -rf "$workdir"
}
trap cleanup EXIT
# Exit on a signal so the EXIT trap cleans up once; otherwise the shell resumes polling.
trap 'exit 1' HUP INT TERM

start() {
  billing_enabled=$1
  quota_enabled=$2
  collector=$3
  billing_auth=$4
  quota_auth=$5
  cat >"$workdir/config.yaml" <<EOF
billing:
  enabled: $billing_enabled
  collector_url: "$collector"
  auth_mode: $billing_auth
  quota:
    enabled: $quota_enabled
    customers_dot_url: https://customers.gitlab.com
    auth_mode: $quota_auth
EOF
  chmod 644 "$workdir/config.yaml"
  container=$(docker create --network none \
    "$image" --mode=webserver --config=/app/enforcement.yaml)
  docker cp "$workdir/config.yaml" "$container:/app/enforcement.yaml"
  docker start "$container" >/dev/null
}

# An enforced build logs this only after it accepts the billing configuration.
accepted() {
  docker logs "$container" 2>&1 | grep -F 'billing enforcement' | grep -Fq '"enabled":true'
}

wait_for() {
  attempts=0
  while [ "$(docker inspect --format '{{.State.Running}}' "$container")" = true ]; do
    if [ "$1" = accept ] && accepted; then return 0; fi
    attempts=$((attempts + 1))
    if [ "$attempts" -ge 30 ]; then
      echo "$name: image did not $1 configuration within 30 seconds" >&2
      exit 1
    fi
    sleep 1
  done
}

finish() {
  docker rm -f "$container" >/dev/null
  container=
  echo "$name: $1 as expected"
}

reject() {
  name=$1
  expected=$2
  shift 2
  start "$@"
  wait_for reject
  status=$(docker inspect --format '{{.State.ExitCode}}' "$container")
  output=$(docker logs "$container" 2>&1)
  if [ "$status" = 0 ] || ! printf '%s\n' "$output" | grep -F "$expected" >/dev/null; then
    printf '%s: expected rejection containing %s; exit=%s\n%s\n' "$name" "$expected" "$status" "$output" >&2
    exit 1
  fi
  finish rejected
}

accept() {
  name=$1
  shift
  start "$@"
  wait_for accept
  if ! accepted; then
    printf '%s: expected the enforced build to accept the configuration\n%s\n' "$name" "$(docker logs "$container" 2>&1)" >&2
    exit 1
  fi
  finish accepted
}

reject billing-disabled 'this build requires billing.enabled=true' false false https://billing.prdsub.gitlab.net cloud_connector license_checksum
reject quota-disabled 'this build requires billing.quota.enabled=true' true false https://billing.prdsub.gitlab.net cloud_connector license_checksum
reject unknown-endpoint 'must both belong to one environment' true true https://invalid.example.com cloud_connector license_checksum
reject mixed-auth 'cannot be combined with' true true https://billing.prdsub.gitlab.net cloud_connector admin_token
accept self-managed true true https://billing.prdsub.gitlab.net cloud_connector license_checksum
accept gitlab-com true true https://billing.prdsub.gitlab.net oidc admin_token
