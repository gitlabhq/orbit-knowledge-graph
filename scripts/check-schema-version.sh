#!/usr/bin/env bash
# Verify that SCHEMA_VERSION is bumped when config/graph.sql changes.
# Used in CI on merge requests.
set -euo pipefail

BASE_REF="${1:-origin/main}"

graph_changed() {
    git diff --name-only "$BASE_REF"...HEAD | grep -q '^config/graph\.sql$'
}

version_changed() {
    git diff "$BASE_REF"...HEAD -- crates/indexer/src/schema_version.rs \
        | grep -qE '^\+pub const SCHEMA_VERSION: u64'
}

if graph_changed; then
    if version_changed; then
        echo "✅ config/graph.sql changed and SCHEMA_VERSION was bumped."
    else
        echo "❌ config/graph.sql changed but SCHEMA_VERSION was not bumped."
        echo ""
        echo "Any MR that modifies config/graph.sql must also bump the"
        echo "SCHEMA_VERSION constant in crates/indexer/src/schema_version.rs."
        exit 1
    fi
else
    echo "✅ config/graph.sql was not changed — no SCHEMA_VERSION bump required."
fi
