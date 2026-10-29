#!/bin/sh
set -eu

root=$(CDPATH='' cd -- "$(dirname "$0")" && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
trap 'exit 1' HUP INT TERM
cat >"$scratch/docker" <<'EOF'
#!/bin/sh
set -eu
echo "$*" >>"$MOCK_DIR/calls"
case "$1" in
  create) echo fake-container ;;
  cp) cp "$2" "$MOCK_DIR/config.yaml" ;;
  start) : ;;
  inspect)
    case "$3" in
      *Running*) echo "${MOCK_RUNNING:-false}" ;;
      *ExitCode*) echo "${MOCK_EXIT:-1}" ;;
    esac ;;
  logs)
    if [ "${MOCK_WRONG_ERROR:-0}" = 1 ]; then echo 'unrelated startup error'; exit 0; fi
    file="$MOCK_DIR/config.yaml"
    if grep -q '^  enabled: false' "$file"; then echo 'this build requires billing.enabled=true'
    elif grep -q '^    enabled: false' "$file"; then echo 'this build requires billing.quota.enabled=true'
    elif grep -q invalid.example "$file"; then echo 'must both belong to one environment'
    elif grep -q 'auth_mode: cloud_connector' "$file" && grep -q 'auth_mode: admin_token' "$file"; then
      echo 'cannot be combined with'
    else
      echo "{\"message\":\"billing enforcement\",\"enabled\":${MOCK_ENFORCED:-true}}"
      echo 'jwt.verifying_key is required'
    fi ;;
  rm) : ;;
  *) exit 1 ;;
esac
EOF
# Instant by default; MOCK_SLEEP=term signals the gate the way a cancelled job does.
cat >"$scratch/sleep" <<'EOF'
#!/bin/sh
if [ "${MOCK_SLEEP:-}" = term ]; then kill -TERM "$PPID"; fi
EOF
chmod +x "$scratch/docker" "$scratch/sleep"
export MOCK_DIR="$scratch"
export PATH="$scratch:$PATH"
image=example.com/orbit@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa

expect_failure() {
  message=$1
  target=$2
  shift 2
  : >"$scratch/calls"
  if env "$@" sh "$root/check-billing-enforcement.sh" "$target" >"$scratch/output" 2>&1; then
    echo "$message" >&2
    exit 1
  fi
}

sh "$root/check-billing-enforcement.sh" "$image"
expect_failure 'Accepted a successful process exit' "$image" MOCK_EXIT=0
expect_failure 'Accepted an unrelated startup failure' "$image" MOCK_WRONG_ERROR=1
expect_failure 'Accepted an unenforced build' "$image" MOCK_ENFORCED=false
expect_failure 'Accepted a container that never exited' "$image" MOCK_RUNNING=true
if ! grep -q '^rm -f fake-container' "$scratch/calls"; then
  echo 'Left the container behind after a timeout' >&2; exit 1
fi
expect_failure 'Passed after SIGTERM' "$image" MOCK_RUNNING=true MOCK_SLEEP=term
if [ "$(grep -c '^inspect' "$scratch/calls")" -gt 1 ]; then
  echo 'Kept polling after SIGTERM' >&2; exit 1
fi
for target in example.com/orbit:latest example.com/orbit@sha256: example.com/orbit@sha256:abc; do
  expect_failure "Accepted image reference ${target}" "$target"
done
echo 'Billing image gate contract tests passed'
