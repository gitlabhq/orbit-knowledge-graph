#!/usr/bin/env bash
# Verify that every pin in config/versions.yaml whose covered files changed
# was bumped. Checks all pins and reports every failure before exiting, so a
# single run surfaces everything that needs bumping.
#
# Usage: check-pinned-version.sh [base-ref]
#
# Skip with [skip pinned-version-check] in the MR description, or
# SKIP_PINNED_VERSION_CHECK=1 locally.
set -euo pipefail

BASE_REF="${1:-origin/main}"

source "$(dirname "$0")/ci-skip-utils.sh"
if ci_skip_requested "pinned-version-check"; then
    echo "✅ [skip pinned-version-check] requested — skipping."
    exit 0
fi

# pin, regex of files that require a bump when changed
COVERS='
query_dsl          ^(config/schemas/graph_query\.schema\.json|crates/query-engine/compiler/src/(input\.rs|passes/validate\.rs))$
opencypher_dialect ^crates/query-engine/opencypher/src/.*$
raw_output_format  ^(crates/query-engine/formatters/src/(graph|lib)\.rs|config/schemas/query_response\.json)$
goon_output_format ^(crates/query-engine/formatters/src/goon/[^/]+\.rs|crates/query-engine/formatters/src/(graph|lib)\.rs)$
'

changed_files=$(git diff --name-only "$BASE_REF"...HEAD)
versions_diff=$(git diff "$BASE_REF"...HEAD -- config/versions.yaml)
failed=()

while read -r pin pattern; do
    [ -n "$pin" ] || continue
    if ! grep -qE "$pattern" <<<"$changed_files"; then
        echo "✅ $pin: no covered files changed."
    elif grep -qE "^\+${pin}:" <<<"$versions_diff"; then
        echo "✅ $pin: covered files changed and the pin was bumped."
    else
        echo "❌ $pin: covered files changed but the pin was not bumped."
        failed+=("$pin")
    fi
done <<<"$COVERS"

if [ "${#failed[@]}" -eq 0 ]; then
    exit 0
fi

echo ""
echo "Bump in config/versions.yaml: ${failed[*]}"
echo "If the change does not affect the shape (comments, refactoring, tests),"
echo "add [skip pinned-version-check] to the MR description or set"
echo "SKIP_PINNED_VERSION_CHECK=1 locally."
exit 1
