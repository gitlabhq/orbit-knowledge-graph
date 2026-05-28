#!/usr/bin/env bash
# Verify pinned Iglu schema versions exist locally and match upstream.
#
# Without flags: full check (committed file exists + matches upstream).
# With --remote-only: only verifies upstream has the pinned version.
#
# Upstream is the anonymous public Pages CDN at
# https://gitlab-org.gitlab.io/iglu/...
#
# Exits non-zero if any pinned version is missing locally, missing
# upstream, or has drifted in content.

set -euo pipefail

IGLU_BASE="https://gitlab-org.gitlab.io/iglu/schemas/com.gitlab"
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
  local_file="${VERSION_DIR}/${name}/${version}.json"

  if [ "$check_local" = true ] && [ ! -f "$local_file" ]; then
    echo "ERROR: $local_file missing (pinned: $version). Run: mise iglu:bump -- $name $version"
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
      echo "DRIFT: $local_file differs from upstream Iglu. Run: mise iglu:bump -- $name $version"
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
