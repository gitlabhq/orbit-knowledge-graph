#!/usr/bin/env bash
# Verify that config/RAW_OUTPUT_FORMAT_VERSION is bumped when response-format-
# affecting files change. Used in CI on merge requests and as a lefthook hook.
set -euo pipefail

BASE_REF="${1:-origin/main}"

response_files_changed() {
    git diff --name-only "$BASE_REF"...HEAD \
        | grep -qE '^(crates/query-engine/formatters/src/(graph|lib)\.rs|crates/gkg-server/schemas/query_response\.json)$'
}

skip_requested() {
    [[ "${SKIP_RESPONSE_SCHEMA_VERSION_CHECK:-}" == "1" ]] && return 0
    local mr_desc
    mr_desc="${CI_MERGE_REQUEST_DESCRIPTION:-}"
    [[ "$mr_desc" == *"[skip response-schema-version-check]"* ]]
}

version_bumped() {
    git diff "$BASE_REF"...HEAD -- config/RAW_OUTPUT_FORMAT_VERSION | grep -q '^+[0-9]'
}

if skip_requested; then
    echo "✅ [skip response-schema-version-check] found in MR description — skipping."
    exit 0
fi

if response_files_changed; then
    if version_bumped; then
        echo "✅ Response format files changed and config/RAW_OUTPUT_FORMAT_VERSION was bumped."
    else
        echo "❌ Response format files changed but config/RAW_OUTPUT_FORMAT_VERSION was not bumped."
        echo ""
        echo "Any MR that modifies the response format (crates/query-engine/formatters/src/graph.rs,"
        echo "crates/query-engine/formatters/src/lib.rs, or crates/gkg-server/schemas/query_response.json)"
        echo "in a way that affects the output shape must also bump config/RAW_OUTPUT_FORMAT_VERSION."
        echo ""
        echo "If this change does not affect the response shape (e.g. comments, refactoring,"
        echo "test-only changes), add [skip response-schema-version-check] to the MR"
        echo "description, or set SKIP_RESPONSE_SCHEMA_VERSION_CHECK=1 when running locally."
        exit 1
    fi
else
    echo "✅ No response format files changed — no RAW_OUTPUT_FORMAT_VERSION bump required."
fi
