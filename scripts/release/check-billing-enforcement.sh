#!/bin/sh
set -eu

image=${1:?Usage: check-billing-enforcement.sh image@sha256:digest}
case "$image" in
  *@sha256:*) ;;
  *) echo "Use an immutable image digest" >&2; exit 1 ;;
esac

workdir=$(mktemp -d)
container=
cleanup() {
  if [ -n "$container" ]; then docker rm -f "$container" >/dev/null 2>&1 || true; fi
  rm -f "$workdir/config.yaml"
  rmdir "$workdir"
}
trap cleanup EXIT HUP INT TERM

check() {
  name=$1
  expected=$2
  billing_enabled=$3
  quota_enabled=$4
  collector=$5
  quota_auth=$6
  cat >"$workdir/config.yaml" <<EOF
billing:
  enabled: $billing_enabled
  collector_url: "$collector"
  auth_mode: cloud_connector
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
  attempts=0
  while [ "$(docker inspect --format '{{.State.Running}}' "$container")" = true ]; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge 30 ]; then
      echo "$name: image did not reject configuration within 30 seconds" >&2
      exit 1
    fi
    sleep 1
  done
  status=$(docker inspect --format '{{.State.ExitCode}}' "$container")
  output=$(docker logs "$container" 2>&1)
  if [ "$status" = 0 ] || ! printf '%s\n' "$output" | grep -F "$expected" >/dev/null; then
    printf '%s: expected rejection containing %s; exit=%s\n%s\n' "$name" "$expected" "$status" "$output" >&2
    exit 1
  fi
  docker rm "$container" >/dev/null
  container=
  echo "$name: rejected as expected"
}

check billing-disabled 'this build requires billing.enabled=true' false false https://billing.prdsub.gitlab.net license_checksum
check quota-disabled 'this build requires billing.quota.enabled=true' true false https://billing.prdsub.gitlab.net license_checksum
check unknown-endpoint 'must both belong to one environment' true true https://invalid.example.com license_checksum
check mixed-auth 'cannot be combined with' true true https://billing.prdsub.gitlab.net admin_token
