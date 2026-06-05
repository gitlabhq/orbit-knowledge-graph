#!/usr/bin/env bash
# Verify that config/QUERY_DSL_VERSION is bumped when query-DSL-affecting
# files change. Used in CI on merge requests and as a lefthook hook.
set -euo pipefail

BASE_REF="${1:-origin/main}"

query_dsl_files_changed() {
    git diff --name-only "$BASE_REF"...HEAD \
        | grep -qE '^(config/schemas/graph_query\.schema\.json|crates/query-engine/compiler/src/(input\.rs|passes/validate\.rs))$'
}

source "$(dirname "$0")/ci-skip-utils.sh"
skip_requested() { ci_skip_requested "query-dsl-version-check"; }

version_bumped() {
    git diff "$BASE_REF"...HEAD -- config/QUERY_DSL_VERSION | grep -q '^+[0-9]'
}

if skip_requested; then
    echo "✅ [skip query-dsl-version-check] found in MR description — skipping."
    exit 0
fi

if query_dsl_files_changed; then
    if version_bumped; then
        echo "✅ Query DSL files changed and config/QUERY_DSL_VERSION was bumped."
    else
        echo "❌ Query DSL files changed but config/QUERY_DSL_VERSION was not bumped."
        echo ""
        echo "Any MR that modifies the query DSL (config/schemas/graph_query.schema.json,"
        echo "crates/query-engine/compiler/src/input.rs, or"
        echo "crates/query-engine/compiler/src/passes/validate.rs) in a way that affects"
        echo "accepted query shape must also bump config/QUERY_DSL_VERSION."
        echo ""
        echo "If this change does not affect the query DSL shape (e.g. comments,"
        echo "refactoring, test-only changes), add [skip query-dsl-version-check] to"
        echo "the MR description, or set SKIP_QUERY_DSL_VERSION_CHECK=1 locally."
        exit 1
    fi
else
    echo "✅ No query DSL files changed — no QUERY_DSL_VERSION bump required."
fi
