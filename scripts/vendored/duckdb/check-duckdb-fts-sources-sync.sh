#!/usr/bin/env bash
set -euo pipefail

# Verify the vendored DuckDB FTS source archive matches its pins in
# config/versions.yaml. Read-only: must not modify any files.
#
# Called by `mise check:vendored duckdb` which sets:
#   VENDOR_VERSIONS_FILE  — absolute path to config/versions.yaml
#   VENDOR_DIR            — absolute path to the vendor directory
#   VENDOR_VERSION        — duckdb version (e.g. v1.5.5)
#   VENDOR_NAME           — "duckdb"
#
# Can also be called directly; falls back to repo-relative paths.

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)

VERSIONS_FILE="${VENDOR_VERSIONS_FILE:-$REPO_ROOT/config/versions.yaml}"
VENDOR_DIR="${VENDOR_DIR:-$REPO_ROOT/$(yq '.vendored.duckdb.vendor_dir' "$VERSIONS_FILE")}"

ARCHIVE="$VENDOR_DIR/duckdb-fts-sources.tar.gz"
EXPECTED_SHA256=$(yq '.vendored.duckdb.extensions.fts.source_archive_sha256' "$VERSIONS_FILE")
ACTUAL_SHA256=$(sha256sum "$ARCHIVE" | awk '{ print $1 }')
WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

if [[ "$ACTUAL_SHA256" != "$EXPECTED_SHA256" ]]; then
    echo "Vendored FTS archive checksum does not match config/versions.yaml" >&2
    echo "  expected: $EXPECTED_SHA256" >&2
    echo "  actual:   $ACTUAL_SHA256" >&2
    exit 1
fi

VENDOR_VERSIONS_FILE="$VERSIONS_FILE" \
VENDOR_DIR="$VENDOR_DIR" \
VENDOR_VERSION="$(yq '.vendored.duckdb.version' "$VERSIONS_FILE")" \
VENDOR_NAME="duckdb" \
    "$REPO_ROOT/scripts/vendored/duckdb/vendor-duckdb-fts-sources.sh" "$WORK_DIR/duckdb-fts-sources.tar.gz"
cmp "$ARCHIVE" "$WORK_DIR/duckdb-fts-sources.tar.gz"
echo "$ARCHIVE matches its pinned upstream DuckDB and duckdb-fts revisions"
