#!/usr/bin/env bash
# Verify pinned Iglu schema versions.
#
# Without flags: checks both local vendored files and live Iglu.
# With --remote-only: skips local file checks (for CI jobs without vendir).
#
# Exits non-zero if any check fails.

set -euo pipefail

IGLU_BASE="https://gitlab-org.gitlab.io/iglu/schemas/com.gitlab"
VENDOR_DIR="vendor/iglu/public/schemas/com.gitlab"
VERSION_DIR="config/schemas/iglu"

check_local=true
if [ "${1:-}" = "--remote-only" ]; then
  check_local=false
fi

failed=0

for version_file in "$VERSION_DIR"/*.version; do
  [ -f "$version_file" ] || continue
  name=$(basename "$version_file" .version)
  version=$(cat "$version_file" | tr -d '[:space:]')
  local_file="$VENDOR_DIR/$name/jsonschema/$version"

  # 1. Check the vendored file exists locally (skip with --remote-only).
  if [ "$check_local" = true ] && [ ! -f "$local_file" ]; then
    echo "ERROR: $local_file missing (pinned: $version). Run: vendir sync"
    failed=1
    continue
  fi

  # 2. Check live Iglu has this version.
  remote=$(curl -sf "$IGLU_BASE/$name/jsonschema/$version") || {
    echo "ERROR: $name/$version not found on live Iglu"
    failed=1
    continue
  }

  # 3. Verify content matches (only when local file exists).
  if [ "$check_local" = true ] && [ -f "$local_file" ]; then
    local_norm=$(python3 -c "import json,sys; json.dump(json.load(sys.stdin), sys.stdout, sort_keys=True)" < "$local_file")
    remote_norm=$(echo "$remote" | python3 -c "import json,sys; json.dump(json.load(sys.stdin), sys.stdout, sort_keys=True)")

    if [ "$local_norm" != "$remote_norm" ]; then
      echo "DRIFT: $local_file differs from live Iglu. Run: vendir sync"
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
