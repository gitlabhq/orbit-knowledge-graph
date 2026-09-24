#!/usr/bin/env bash
# Compare recorded public schema output for HEAD with the target branch.
# Only use [skip graph-schema-api-check] for intentionally unversioned output
# changes after confirming that clients do not cache the changed representation.
set -euo pipefail

BASE_REF="${1:-origin/main}"
source "$(dirname "$0")/ci-skip-utils.sh"
if ci_skip_requested "graph-schema-api-check"; then
    echo "✅ [skip graph-schema-api-check] requested — checking freshness and new element versions only."
    cargo xtask schema-public-output --check --base "$BASE_REF" --skip-pin-check
else
    cargo xtask schema-public-output --check --base "$BASE_REF"
fi
