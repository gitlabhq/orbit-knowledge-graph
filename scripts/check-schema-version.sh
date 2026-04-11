#!/usr/bin/env bash
# Verify that config/SCHEMA_VERSION is bumped when schema-affecting files change.
# Used in CI on merge requests and as a lefthook pre-commit hook.
set -euo pipefail

BASE_REF="${1:-origin/main}"

schema_files_changed() {
    git diff --name-only "$BASE_REF"...HEAD \
        | grep -qE '^(config/graph\.sql|config/graph_local\.sql|config/ontology/)'
}

skip_requested() {
    # SKIP_SCHEMA_VERSION_CHECK=1 works locally (lefthook) and in CI.
    # [skip schema-version-check] in the MR description only works in CI
    # where CI_MERGE_REQUEST_DESCRIPTION is set.
    [[ "${SKIP_SCHEMA_VERSION_CHECK:-}" == "1" ]] && return 0
    local mr_desc
    mr_desc="${CI_MERGE_REQUEST_DESCRIPTION:-}"
    [[ "$mr_desc" == *"[skip schema-version-check]"* ]]
}

version_bumped() {
    git diff "$BASE_REF"...HEAD -- config/SCHEMA_VERSION | grep -q '^+[0-9]'
}

if skip_requested; then
    echo "✅ [skip schema-version-check] found in MR description — skipping."
    exit 0
fi

if schema_files_changed; then
    if version_bumped; then
        echo "✅ Schema-affecting files changed and config/SCHEMA_VERSION was bumped."
    else
        echo "❌ Schema-affecting files changed but config/SCHEMA_VERSION was not bumped."
        echo ""
        echo "Any MR that modifies config/graph.sql, config/graph_local.sql, or"
        echo "config/ontology/ in a way that affects the ClickHouse schema must also"
        echo "bump config/SCHEMA_VERSION."
        echo ""
        echo "If this change does not affect the ClickHouse schema (e.g. comments,"
        echo "formatting, or ontology description updates), add"
        echo "[skip schema-version-check] to the MR description, or set"
        echo "SKIP_SCHEMA_VERSION_CHECK=1 when running locally."
        exit 1
    fi
else
    echo "✅ No schema-affecting files changed — no SCHEMA_VERSION bump required."
fi
