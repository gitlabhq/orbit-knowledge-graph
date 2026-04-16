#!/usr/bin/env bash
# Verify that config/RAW_OUTPUT_FORMAT_VERSION (and GOON_OUTPUT_FORMAT_VERSION
# when it exists) is bumped when response-format-affecting files change.
# Used in CI on merge requests and as a lefthook pre-commit hook.
set -euo pipefail

BASE_REF="${1:-origin/main}"

valid_semver() {
    echo "$1" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+$'
}

raw_files_changed() {
    git diff --name-only "$BASE_REF"...HEAD \
        | grep -qE '^(crates/query-engine/formatters/src/graph\.rs|crates/gkg-server/schemas/query_response\.json)'
}

goon_files_changed() {
    git diff --name-only "$BASE_REF"...HEAD \
        | grep -qE '^(crates/query-engine/formatters/src/goon\.rs|crates/gkg-server/schemas/goon_format\.md)'
}

version_bumped() {
    local file="$1"
    git diff "$BASE_REF"...HEAD -- "$file" | grep -q '^+[0-9]'
}

skip_requested() {
    [[ "${SKIP_RESPONSE_SCHEMA_VERSION_CHECK:-}" == "1" ]] && return 0
    local mr_desc
    mr_desc="${CI_MERGE_REQUEST_DESCRIPTION:-}"
    [[ "$mr_desc" == *"[skip response-schema-version-check]"* ]]
}

if skip_requested; then
    echo "OK [skip response-schema-version-check] found -- skipping."
    exit 0
fi

exit_code=0

# Validate RAW_OUTPUT_FORMAT_VERSION is valid semver
if [[ -f config/RAW_OUTPUT_FORMAT_VERSION ]]; then
    raw_ver="$(tr -d '[:space:]' < config/RAW_OUTPUT_FORMAT_VERSION)"
    if ! valid_semver "$raw_ver"; then
        echo "FAIL config/RAW_OUTPUT_FORMAT_VERSION ('$raw_ver') is not valid semver."
        exit_code=1
    fi
fi

# Check RAW format changes
if raw_files_changed; then
    if version_bumped config/RAW_OUTPUT_FORMAT_VERSION; then
        echo "OK RAW format files changed and config/RAW_OUTPUT_FORMAT_VERSION was bumped."
    else
        echo "FAIL RAW format files changed but config/RAW_OUTPUT_FORMAT_VERSION was not bumped."
        echo ""
        echo "Any MR that modifies crates/query-engine/formatters/src/graph.rs or"
        echo "crates/gkg-server/schemas/query_response.json must also bump"
        echo "config/RAW_OUTPUT_FORMAT_VERSION."
        echo ""
        echo "If the change does not affect the response shape (e.g. refactoring,"
        echo "comments, test-only changes), add [skip response-schema-version-check]"
        echo "to the MR description, or set SKIP_RESPONSE_SCHEMA_VERSION_CHECK=1."
        exit_code=1
    fi
else
    echo "OK No RAW format files changed -- no RAW_OUTPUT_FORMAT_VERSION bump required."
fi

# Check GOON format changes (no-op until config/GOON_OUTPUT_FORMAT_VERSION exists)
if [[ -f config/GOON_OUTPUT_FORMAT_VERSION ]]; then
    goon_ver="$(tr -d '[:space:]' < config/GOON_OUTPUT_FORMAT_VERSION)"
    if ! valid_semver "$goon_ver"; then
        echo "FAIL config/GOON_OUTPUT_FORMAT_VERSION ('$goon_ver') is not valid semver."
        exit_code=1
    fi

    if goon_files_changed; then
        if version_bumped config/GOON_OUTPUT_FORMAT_VERSION; then
            echo "OK GOON format files changed and config/GOON_OUTPUT_FORMAT_VERSION was bumped."
        else
            echo "FAIL GOON format files changed but config/GOON_OUTPUT_FORMAT_VERSION was not bumped."
            exit_code=1
        fi
    else
        echo "OK No GOON format files changed -- no GOON_OUTPUT_FORMAT_VERSION bump required."
    fi
fi

exit $exit_code
