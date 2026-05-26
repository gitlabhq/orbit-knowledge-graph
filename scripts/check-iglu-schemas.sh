#!/usr/bin/env bash
# Verify pinned Iglu schema versions exist on the live Iglu server.
#
# For each *.version file in config/schemas/:
#   1. Read the pinned version
#   2. Fetch from live Iglu and verify it returns valid JSON
#
# Exits non-zero if any pinned version is missing from live Iglu.

set -euo pipefail

IGLU_BASE="https://gitlab-org.gitlab.io/iglu/schemas/com.gitlab"
VERSION_DIR="config/schemas/iglu"

failed=0

for version_file in "$VERSION_DIR"/*.version; do
  [ -f "$version_file" ] || continue
  name=$(basename "$version_file" .version)
  version=$(cat "$version_file" | tr -d '[:space:]')

  remote=$(curl -sf "$IGLU_BASE/$name/jsonschema/$version") || {
    echo "ERROR: $name/$version not found on live Iglu ($IGLU_BASE/$name/jsonschema/$version)"
    failed=1
    continue
  }

  # Verify it's valid JSON with the expected self.version.
  embedded=$(echo "$remote" | python3 -c "import json,sys; print(json.load(sys.stdin).get('self',{}).get('version',''))")
  if [ "$embedded" != "$version" ]; then
    echo "ERROR: $name live schema has self.version='$embedded', expected '$version'"
    failed=1
    continue
  fi

  echo "OK: $name/$version"
done

if [ "$failed" -ne 0 ]; then
  echo ""
  echo "One or more pinned Iglu schema versions failed validation."
  exit 1
fi

echo "All pinned Iglu schema versions verified against live Iglu."
