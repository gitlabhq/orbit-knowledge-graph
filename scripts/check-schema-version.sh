#!/usr/bin/env bash
# Verify that config/SCHEMA_VERSION is bumped when schema-affecting files change.
# Used in CI on merge requests and as a lefthook pre-commit hook.
set -euo pipefail

BASE_REF="${1:-origin/main}"

read_version() {
    local v
    v=$(cat "$1" 2>/dev/null | tr -d '[:space:]')
    v="${v:-0}"
    [[ "$v" =~ ^[0-9]+$ ]] || { echo "❌ $1 must be numeric, got: '$v'"; exit 1; }
    echo "$v"
}

schema_files_changed() {
    git diff --name-only "$BASE_REF"...HEAD | grep -qE '^(config/graph\.sql|config/ontology/)'
}

skip_requested() {
    [[ "${SKIP_SCHEMA_VERSION_CHECK:-}" == "1" ]] && return 0
    [[ "${CI_MERGE_REQUEST_DESCRIPTION:-}" == *"[skip schema-version-check]"* ]]
}

version_bumped() {
    git diff "$BASE_REF"...HEAD -- config/SCHEMA_VERSION | grep -q '^+[0-9]'
}

# Schema version drives table prefixing (v1_gl_user, v2_gl_user) and the
# migration orchestrator. Skips leave gaps; downgrades point at stale tables.
check_monotonic() {
    local old new expected
    old=$(git show "$BASE_REF":config/SCHEMA_VERSION 2>/dev/null | tr -d '[:space:]')
    old="${old:-0}"
    new=$(read_version config/SCHEMA_VERSION)
    expected=$((old + 1))

    if [ "$new" -ne "$expected" ]; then
        echo "❌ SCHEMA_VERSION must increment by 1: was $old, expected $expected, got $new."
        exit 1
    fi
}

if skip_requested; then
    echo "✅ [skip schema-version-check] — skipping."
    exit 0
fi

if schema_files_changed; then
    if version_bumped; then
        check_monotonic
        echo "✅ SCHEMA_VERSION bumped to $(read_version config/SCHEMA_VERSION)."
    else
        echo "❌ Schema-affecting files changed but SCHEMA_VERSION was not bumped."
        echo ""
        echo "Bump config/SCHEMA_VERSION for DDL shape changes, edge type renames,"
        echo "and ETL mapping changes. If this change is cosmetic, add"
        echo "[skip schema-version-check] to the MR description."
        exit 1
    fi
else
    echo "✅ No schema-affecting files changed."
fi
