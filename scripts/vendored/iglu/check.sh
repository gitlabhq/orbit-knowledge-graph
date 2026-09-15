#!/usr/bin/env bash
# Verify pinned Iglu schema versions exist locally and match upstream.
#
# Called by `mise check:vendored -- iglu` which sets:
#   VENDOR_VERSIONS_FILE  — absolute path to config/versions.yaml
#   VENDOR_DIR            — absolute path to config/schemas/iglu
#   VENDOR_NAME           — "iglu"
#
# Can also be called directly; falls back to repo-relative paths.
#
# Without flags: full check (committed file exists + matches upstream).
# With --remote-only: only verifies upstream has the pinned version.

set -euo pipefail

VERSIONS_FILE="${VENDOR_VERSIONS_FILE:?Set VENDOR_VERSIONS_FILE or call via scripts/vendored/run.sh}"
VENDOR_DIR="${VENDOR_DIR:?Set VENDOR_DIR or call via scripts/vendored/run.sh}"
IGLU_BASE="https://gitlab-org.gitlab.io/iglu/schemas/com.gitlab"

check_local=true
if [ "${1:-}" = "--remote-only" ]; then
  check_local=false
fi

failed=0

for name in $(yq '.vendored.iglu.pins | keys | .[]' "$VERSIONS_FILE"); do
  version=$(yq ".vendored.iglu.pins.$name" "$VERSIONS_FILE")
  local_file="$VENDOR_DIR/$name/$version.json"

  if [ "$check_local" = true ] && [ ! -f "$local_file" ]; then
    echo "ERROR: $local_file missing (pinned: $version). Run: mise vendor -- iglu"
    failed=1
    continue
  fi

  remote=$(curl -sf "$IGLU_BASE/$name/jsonschema/$version") || {
    echo "ERROR: $name/$version not found on live Iglu"
    failed=1
    continue
  }

  if [ "$check_local" = true ] && [ -f "$local_file" ]; then
    local_norm=$(python3 -c "import json,sys; json.dump(json.load(sys.stdin), sys.stdout, sort_keys=True)" < "$local_file")
    remote_norm=$(printf '%s' "$remote" | python3 -c "import json,sys; json.dump(json.load(sys.stdin), sys.stdout, sort_keys=True)")

    if [ "$local_norm" != "$remote_norm" ]; then
      echo "DRIFT: $local_file differs from upstream Iglu. Run: mise vendor -- iglu"
      failed=1
      continue
    fi
  fi

  echo "OK: $name/$version"
done

if [ "$failed" -ne 0 ]; then
  echo ""
  echo "Iglu schema check failed."
  exit 1
fi

echo "All pinned Iglu schemas verified."
