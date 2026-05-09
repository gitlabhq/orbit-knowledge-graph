#!/usr/bin/env bash
# Verify that config/GOON_OUTPUT_FORMAT_VERSION is bumped when GOON-format-
# affecting files change. Mirrors check-response-schema-version.sh for
# RAW. Used in CI on merge requests and as a lefthook hook.
set -euo pipefail

BASE_REF="${1:-origin/main}"

goon_files_changed() {
    git diff --name-only "$BASE_REF"...HEAD \
        | grep -qE '^(crates/query-engine/formatters/src/goon/[^/]+\.rs|crates/query-engine/formatters/src/(graph|lib)\.rs)$'
}

skip_requested() {
    [[ "${SKIP_GOON_FORMAT_VERSION_CHECK:-}" == "1" ]] && return 0
    local mr_desc
    mr_desc="${CI_MERGE_REQUEST_DESCRIPTION:-}"
    [[ "$mr_desc" == *"[skip goon-format-version-check]"* ]]
}

version_bumped() {
    git diff "$BASE_REF"...HEAD -- config/GOON_OUTPUT_FORMAT_VERSION | grep -q '^+[0-9]'
}

if skip_requested; then
    echo "✅ [skip goon-format-version-check] found in MR description — skipping."
    exit 0
fi

if goon_files_changed; then
    if version_bumped; then
        echo "✅ GOON format files changed and config/GOON_OUTPUT_FORMAT_VERSION was bumped."
    else
        echo "❌ GOON format files changed but config/GOON_OUTPUT_FORMAT_VERSION was not bumped."
        echo ""
        echo "Any MR that modifies the GOON encoder (crates/query-engine/formatters/src/goon/**.rs,"
        echo "crates/query-engine/formatters/src/graph.rs, or crates/query-engine/formatters/src/lib.rs)"
        echo "in a way that affects the output shape must also bump config/GOON_OUTPUT_FORMAT_VERSION."
        echo ""
        echo "If this change does not affect the GOON output shape (e.g. comments, refactoring,"
        echo "test-only changes), add [skip goon-format-version-check] to the MR description,"
        echo "or set SKIP_GOON_FORMAT_VERSION_CHECK=1 when running locally."
        exit 1
    fi
else
    echo "✅ No GOON format files changed — no GOON_OUTPUT_FORMAT_VERSION bump required."
fi
