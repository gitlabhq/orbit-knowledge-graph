#!/usr/bin/env bash
# Verify vendored Iglu schemas match live Iglu for pinned versions.
#
# For each *.iglu-version file in config/schemas/:
#   1. Read the pinned version
#   2. Check the corresponding file exists in the vendored directory
#   3. Diff against the live Iglu endpoint
#
# Exits non-zero if any schema is missing or has drifted.

set -euo pipefail

IGLU_BASE="https://gitlab-org.gitlab.io/iglu/schemas/com.gitlab"
VENDOR_DIR="vendor/iglu/public/schemas/com.gitlab"
VERSION_DIR="config/schemas"

failed=0

for version_file in "$VERSION_DIR"/*.iglu-version; do
  [ -f "$version_file" ] || continue
  name=$(basename "$version_file" .iglu-version)
  version=$(cat "$version_file" | tr -d '[:space:]')
  local_file="$VENDOR_DIR/$name/jsonschema/$version"

  # 1. Check the vendored directory has this version.
  if [ ! -f "$local_file" ]; then
    echo "MISSING: $local_file (pinned version: $version)"
    echo "  Run: mise iglu:bump -- $name $version"
    failed=1
    continue
  fi

  # 2. Diff against live Iglu.
  remote=$(curl -sf "$IGLU_BASE/$name/jsonschema/$version") || {
    echo "ERROR: could not fetch $IGLU_BASE/$name/jsonschema/$version"
    failed=1
    continue
  }

  local_norm=$(python3 -c "import json,sys; json.dump(json.load(sys.stdin), sys.stdout, sort_keys=True)" < "$local_file")
  remote_norm=$(echo "$remote" | python3 -c "import json,sys; json.dump(json.load(sys.stdin), sys.stdout, sort_keys=True)")

  if [ "$local_norm" != "$remote_norm" ]; then
    echo "DRIFT: $local_file differs from live Iglu"
    echo "  Run: mise iglu:bump -- $name $version"
    failed=1
  fi
done

if [ "$failed" -ne 0 ]; then
  echo ""
  echo "Vendored Iglu schemas are out of date or pinned version is missing."
  exit 1
fi

echo "All pinned orbit Iglu schemas are up to date."
