#!/usr/bin/env bash
# Verify that config/SCHEMA_VERSION is bumped when schema-affecting files change.
# Used in CI on merge requests and as a lefthook pre-commit hook.
set -euo pipefail

BASE_REF="${1:-origin/main}"

schema_files_changed() {
    git diff --name-only "$BASE_REF"...HEAD \
        | grep -qE '^(config/graph\.sql|config/ontology/)'
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

# The schema version drives table prefixing (v1_gl_user, v2_gl_user, etc.)
# and the migration orchestrator uses it to decide when to create new
# table sets. Skipping a version would leave a gap that the orchestrator
# never creates tables for. A downgrade would point the binary at tables
# that may not exist or contain stale data from a previous layout.
check_monotonic() {
    local old new
    old=$(git show "$BASE_REF":config/SCHEMA_VERSION 2>/dev/null | tr -d '[:space:]')
    new=$(cat config/SCHEMA_VERSION | tr -d '[:space:]')
    old="${old:-0}"

    if [ "$new" -le "$old" ]; then
        echo "❌ SCHEMA_VERSION must increase: was $old, now $new."
        exit 1
    fi

    local expected=$((old + 1))
    if [ "$new" -ne "$expected" ]; then
        echo "❌ SCHEMA_VERSION must increment by 1: was $old, expected $expected, got $new."
        exit 1
    fi
}

if skip_requested; then
    echo "✅ [skip schema-version-check] found in MR description — skipping."
    exit 0
fi

if schema_files_changed; then
    if version_bumped; then
        check_monotonic
        echo "✅ Schema-affecting files changed and SCHEMA_VERSION bumped to $(cat config/SCHEMA_VERSION | tr -d '[:space:]')."
    else
        echo "❌ Schema-affecting files changed but config/SCHEMA_VERSION was not bumped."
        echo ""
        echo "Any MR that modifies config/graph.sql or config/ontology/ in a way"
        echo "that affects the ClickHouse schema or stored data values must also"
        echo "bump config/SCHEMA_VERSION. This includes DDL shape changes,"
        echo "edge type renames, and ETL mapping changes."
        echo ""
        echo "If this change does not affect the schema or stored values (e.g."
        echo "comments, formatting, or ontology description updates), add"
        echo "[skip schema-version-check] to the MR description, or set"
        echo "SKIP_SCHEMA_VERSION_CHECK=1 when running locally."
        exit 1
    fi
else
    echo "✅ No schema-affecting files changed — no SCHEMA_VERSION bump required."
fi
