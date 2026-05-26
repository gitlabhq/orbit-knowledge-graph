#!/usr/bin/env bash
# Verify vendored Iglu schemas match the live versions served by GitLab Pages.
# Exits non-zero if any schema has drifted.

set -euo pipefail

IGLU_BASE="https://gitlab-org.gitlab.io/iglu/schemas/com.gitlab"
VENDOR_DIR="config/schemas/iglu"

schemas=(
  "orbit_common/jsonschema/1-0-0:orbit_common.1-0-0.json"
  "orbit_query/jsonschema/2-0-1:orbit_query.2-0-1.json"
)

failed=0
for entry in "${schemas[@]}"; do
  path="${entry%%:*}"
  file="${entry##*:}"
  local_file="$VENDOR_DIR/$file"

  if [ ! -f "$local_file" ]; then
    echo "MISSING: $local_file"
    failed=1
    continue
  fi

  remote=$(curl -sf "$IGLU_BASE/$path") || {
    echo "WARN: could not fetch $IGLU_BASE/$path (skipping)"
    continue
  }

  # Normalize JSON for comparison (sort keys, consistent whitespace).
  local_norm=$(python3 -c "import json,sys; json.dump(json.load(sys.stdin), sys.stdout, sort_keys=True)" < "$local_file")
  remote_norm=$(echo "$remote" | python3 -c "import json,sys; json.dump(json.load(sys.stdin), sys.stdout, sort_keys=True)")

  if [ "$local_norm" != "$remote_norm" ]; then
    echo "DRIFT: $local_file differs from $IGLU_BASE/$path"
    echo "  Run: curl -s '$IGLU_BASE/$path' > '$local_file'"
    failed=1
  fi
done

if [ "$failed" -ne 0 ]; then
  echo "Vendored Iglu schemas are out of date. Re-vendor and commit."
  exit 1
fi

echo "All vendored Iglu schemas are up to date."
