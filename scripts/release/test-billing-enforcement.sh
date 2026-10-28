#!/bin/sh
set -eu

root=$(CDPATH='' cd -- "$(dirname "$0")" && pwd)
scratch=$(mktemp -d)
trap 'rm -f "$scratch/docker" "$scratch/config.yaml" "$scratch/output"; rmdir "$scratch"' EXIT HUP INT TERM
cat >"$scratch/docker" <<'EOF'
#!/bin/sh
set -eu
case "$1" in
  create) echo fake-container ;;
  cp) cp "$2" "$MOCK_DIR/config.yaml" ;;
  start) : ;;
  inspect)
    case "$3" in
      *Running*) echo false ;;
      *ExitCode*) echo "${MOCK_EXIT:-1}" ;;
    esac ;;
  logs)
    if [ "${MOCK_WRONG_ERROR:-0}" = 1 ]; then echo 'unrelated startup error'; exit 0; fi
    file="$MOCK_DIR/config.yaml"
    if grep -q '^  enabled: false' "$file"; then echo 'this build requires billing.enabled=true'
    elif grep -q '^    enabled: false' "$file"; then echo 'this build requires billing.quota.enabled=true'
    elif grep -q invalid.example "$file"; then echo 'must both belong to one environment'
    else echo 'cannot be combined with'; fi ;;
  rm) : ;;
  *) exit 1 ;;
esac
EOF
chmod +x "$scratch/docker"
export MOCK_DIR="$scratch"
export PATH="$scratch:$PATH"
image=example.com/orbit@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
sh "$root/check-billing-enforcement.sh" "$image"
if MOCK_EXIT=0 sh "$root/check-billing-enforcement.sh" "$image" >"$scratch/output" 2>&1; then
  echo 'Accepted a successful process exit' >&2; exit 1
fi
if MOCK_WRONG_ERROR=1 sh "$root/check-billing-enforcement.sh" "$image" >"$scratch/output" 2>&1; then
  echo 'Accepted an unrelated startup failure' >&2; exit 1
fi
if sh "$root/check-billing-enforcement.sh" example.com/orbit:latest >"$scratch/output" 2>&1; then
  echo 'Accepted a mutable tag' >&2; exit 1
fi
echo 'Billing image gate contract tests passed'
