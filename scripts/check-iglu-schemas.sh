#!/usr/bin/env bash
# Verify vendored Iglu schemas match the versions declared in .version
# files and are up to date with the live Iglu server.
#
# For each *.version file in config/schemas/iglu/:
#   1. Read the pinned version (e.g. "2-0-1")
#   2. Check the corresponding .json file exists and its embedded
#      self.version matches the pinned version
#   3. Fetch the live schema from Iglu and diff against the vendored copy
#
# Exits non-zero if any schema is missing, mismatched, or drifted.

set -euo pipefail

IGLU_BASE="https://gitlab-org.gitlab.io/iglu/schemas/com.gitlab"
VENDOR_DIR="config/schemas/iglu"

failed=0

for version_file in "$VENDOR_DIR"/*.version; do
  name=$(basename "$version_file" .version)  # e.g. "orbit_query"
  version=$(cat "$version_file" | tr -d '[:space:]')
  json_file="$VENDOR_DIR/${name}.json"

  # 1. Check the JSON file exists.
  if [ ! -f "$json_file" ]; then
    echo "MISSING: $json_file (declared version: $version)"
    failed=1
    continue
  fi

  # 2. Check the embedded self.version matches the pinned version.
  embedded=$(python3 -c "
import json, sys
schema = json.load(open('$json_file'))
print(schema.get('self', {}).get('version', ''))
")
  if [ "$embedded" != "$version" ]; then
    echo "MISMATCH: $json_file has self.version='$embedded' but $version_file says '$version'"
    failed=1
    continue
  fi

  # 3. Fetch from Iglu and compare.
  iglu_path="${name}/jsonschema/${version}"
  remote=$(curl -sf "$IGLU_BASE/$iglu_path") || {
    echo "WARN: could not fetch $IGLU_BASE/$iglu_path (skipping freshness check)"
    continue
  }

  local_norm=$(python3 -c "import json,sys; json.dump(json.load(sys.stdin), sys.stdout, sort_keys=True)" < "$json_file")
  remote_norm=$(echo "$remote" | python3 -c "import json,sys; json.dump(json.load(sys.stdin), sys.stdout, sort_keys=True)")

  if [ "$local_norm" != "$remote_norm" ]; then
    echo "DRIFT: $json_file differs from $IGLU_BASE/$iglu_path"
    echo "  Run: curl -s '$IGLU_BASE/$iglu_path' > '$json_file'"
    failed=1
  fi
done

if [ "$failed" -ne 0 ]; then
  echo ""
  echo "Vendored Iglu schemas are out of date or misconfigured."
  echo "To bump a schema version:"
  echo "  1. Update the version in config/schemas/iglu/<name>.version"
  echo "  2. Rename the .json file to match: <name>.<new-version>.json"
  echo "  3. Re-vendor: curl -s '\$IGLU_BASE/<name>/jsonschema/<new-version>' > config/schemas/iglu/<name>.<new-version>.json"
  exit 1
fi

echo "All vendored Iglu schemas are up to date."
