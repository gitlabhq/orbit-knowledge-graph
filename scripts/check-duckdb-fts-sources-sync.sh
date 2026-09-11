#!/usr/bin/env bash
set -euo pipefail

PIN="crates/duckdb-client/third_party/duckdb-fts-sources.PIN"
ARCHIVE="crates/duckdb-client/third_party/duckdb-fts-sources.tar.gz"
DUCKDB_PIN=$(awk '$1 == "duckdb:" { print $2 }' config/versions.yaml)
SOURCE_DUCKDB_PIN=$(awk '$1 == "duckdb:" { print $2 }' "$PIN")
EXPECTED_SHA256=$(awk '$1 == "archive_sha256:" { print $2 }' "$PIN")
ACTUAL_SHA256=$(sha256sum "$ARCHIVE" | awk '{ print $1 }')
WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

if [[ "$SOURCE_DUCKDB_PIN" != "$DUCKDB_PIN" ]]; then
    echo "Vendored FTS sources must match DuckDB $DUCKDB_PIN" >&2
    exit 1
fi
if [[ "$ACTUAL_SHA256" != "$EXPECTED_SHA256" ]]; then
    echo "Vendored FTS archive checksum does not match $PIN" >&2
    exit 1
fi

./scripts/vendor-duckdb-fts-sources.sh "$WORK_DIR/duckdb-fts-sources.tar.gz"
cmp "$ARCHIVE" "$WORK_DIR/duckdb-fts-sources.tar.gz"
echo "$ARCHIVE matches its pinned upstream DuckDB and duckdb-fts revisions"
