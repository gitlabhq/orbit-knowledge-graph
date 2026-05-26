#!/usr/bin/env bash
# Verify the vendored iglu subtree matches the live Iglu server for orbit schemas.
#
# For each orbit_* schema directory in the subtree, finds the latest version
# file, compares it against the live Iglu endpoint.
#
# Exits non-zero if any schema has drifted (subtree needs `git subtree pull`).

set -euo pipefail

IGLU_BASE="https://gitlab-org.gitlab.io/iglu/schemas/com.gitlab"
SUBTREE_DIR="vendor/iglu/public/schemas/com.gitlab"

failed=0

for schema_dir in "$SUBTREE_DIR"/orbit_*/; do
  name=$(basename "$schema_dir")
  # Find the latest version file (highest semver directory name).
  latest=$(ls -1 "$schema_dir/jsonschema/" 2>/dev/null | sort -t- -k1,1n -k2,2n -k3,3n | tail -1)

  if [ -z "$latest" ]; then
    echo "WARN: no version found in $schema_dir/jsonschema/"
    continue
  fi

  local_file="$schema_dir/jsonschema/$latest"

  remote=$(curl -sf "$IGLU_BASE/$name/jsonschema/$latest") || {
    echo "WARN: could not fetch $IGLU_BASE/$name/jsonschema/$latest (skipping)"
    continue
  }

  local_norm=$(python3 -c "import json,sys; json.dump(json.load(sys.stdin), sys.stdout, sort_keys=True)" < "$local_file")
  remote_norm=$(echo "$remote" | python3 -c "import json,sys; json.dump(json.load(sys.stdin), sys.stdout, sort_keys=True)")

  if [ "$local_norm" != "$remote_norm" ]; then
    echo "DRIFT: $local_file differs from live Iglu"
    echo "  Run: git subtree pull --prefix=vendor/iglu https://gitlab.com/gitlab-org/iglu.git master --squash"
    failed=1
  fi
done

if [ "$failed" -ne 0 ]; then
  echo ""
  echo "Vendored iglu subtree is out of date."
  exit 1
fi

echo "All vendored orbit Iglu schemas are up to date."
