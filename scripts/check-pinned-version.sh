#!/usr/bin/env bash
# Verify that a pin in config/versions.yaml is bumped when the files it
# covers change. Used in CI on merge requests and as a lefthook hook.
#
# Usage: check-pinned-version.sh <pin> [base-ref]
#
# Pins and the file patterns that require a bump:
#   query_dsl          config/schemas/graph_query.schema.json, compiler input/validate passes
#   raw_output_format  formatters graph.rs/lib.rs, config/schemas/query_response.json
#   goon_output_format formatters goon/*.rs, graph.rs, lib.rs
#
# Skip with [skip <pin>-version-check] in the MR description, or
# SKIP_<PIN>_VERSION_CHECK=1 locally.
set -euo pipefail

PIN="$1"
BASE_REF="${2:-origin/main}"

case "$PIN" in
    query_dsl)
        pattern='^(config/schemas/graph_query\.schema\.json|crates/query-engine/compiler/src/(input\.rs|passes/validate\.rs))$' ;;
    raw_output_format)
        pattern='^(crates/query-engine/formatters/src/(graph|lib)\.rs|config/schemas/query_response\.json)$' ;;
    goon_output_format)
        pattern='^(crates/query-engine/formatters/src/goon/[^/]+\.rs|crates/query-engine/formatters/src/(graph|lib)\.rs)$' ;;
    *)
        echo "unknown pin: $PIN" >&2; exit 2 ;;
esac

check_name="${PIN//_/-}-version-check"
source "$(dirname "$0")/ci-skip-utils.sh"

if ci_skip_requested "$check_name"; then
    echo "✅ [skip $check_name] requested — skipping."
    exit 0
fi

if ! git diff --name-only "$BASE_REF"...HEAD | grep -qE "$pattern"; then
    echo "✅ No files covered by \`$PIN\` changed — no bump required."
    exit 0
fi

if git diff "$BASE_REF"...HEAD -- config/versions.yaml | grep -qE "^\+${PIN}:"; then
    echo "✅ Files covered by \`$PIN\` changed and the pin was bumped."
    exit 0
fi

echo "❌ Files covered by \`$PIN\` changed but \`$PIN\` in config/versions.yaml was not bumped."
echo ""
echo "If this change does not affect the shape (comments, refactoring, tests),"
echo "add [skip $check_name] to the MR description or set"
echo "SKIP_$(echo "$check_name" | tr '[:lower:]-' '[:upper:]_')=1 locally."
exit 1
