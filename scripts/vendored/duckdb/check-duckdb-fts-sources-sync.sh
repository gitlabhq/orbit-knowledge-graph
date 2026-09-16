#!/usr/bin/env bash
set -euo pipefail

# Verify the vendored DuckDB FTS source archive matches its pins in
# config/versions.yaml. Read-only: must not modify any files.
#
# Called by `mise check:vendored -- duckdb` via scripts/vendored/run.sh
# which sets:
#   VENDOR_VERSIONS_FILE  — absolute path to config/versions.yaml
#   VENDOR_DIR            — absolute path to the vendor directory
#   VENDOR_VERSION        — duckdb version (e.g. v1.5.5)
#   VENDOR_NAME           — "duckdb"
#
# Must be invoked through the runner; requires VENDOR_* env vars.
#
# The archive-build recipe below must match fts-vendor.sh's
# vendor_fts_source_archive. It is duplicated here because the vendor
# script writes checksums back (violating the check-script read-only
# contract). Keep the two in sync when changing the archive layout.

VERSIONS_FILE="${VENDOR_VERSIONS_FILE:?Set VENDOR_VERSIONS_FILE or call via scripts/vendored/run.sh}"
VENDOR_DIR="${VENDOR_DIR:?Set VENDOR_DIR or call via scripts/vendored/run.sh}"
DUCKDB_VERSION="${VENDOR_VERSION:?Set VENDOR_VERSION or call via scripts/vendored/run.sh}"

ARCHIVE="$VENDOR_DIR/duckdb-fts-sources.tar.gz"
EXPECTED_SHA256=$(yq '.vendored.duckdb.extensions.fts.source_archive_sha256' "$VERSIONS_FILE")
ACTUAL_SHA256=$(sha256sum "$ARCHIVE" | awk '{ print $1 }')

if [[ "$ACTUAL_SHA256" != "$EXPECTED_SHA256" ]]; then
    echo "Vendored FTS archive checksum does not match config/versions.yaml" >&2
    echo "  expected: $EXPECTED_SHA256" >&2
    echo "  actual:   $ACTUAL_SHA256" >&2
    exit 1
fi

WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

FTS_REVISION=$(yq '.vendored.duckdb.extensions.fts.source_revision' "$VERSIONS_FILE")
SOURCE_ROOT="$WORK_DIR/stage/duckdb-fts-sources"

export LC_ALL=C
git -c advice.detachedHead=false clone --quiet --depth 1 --branch "$DUCKDB_VERSION" \
    --filter=blob:none --sparse https://github.com/duckdb/duckdb.git "$WORK_DIR/duckdb"
git -C "$WORK_DIR/duckdb" sparse-checkout set third_party/snowball

git -C "$WORK_DIR" init --quiet duckdb-fts
git -C "$WORK_DIR/duckdb-fts" remote add origin https://github.com/duckdb/duckdb-fts.git
git -C "$WORK_DIR/duckdb-fts" fetch --quiet --depth 1 origin "$FTS_REVISION"
git -C "$WORK_DIR/duckdb-fts" checkout --quiet --detach FETCH_HEAD

mkdir -p "$SOURCE_ROOT/fts/include"
cp -R "$WORK_DIR/duckdb/third_party/snowball" "$SOURCE_ROOT/snowball"
rm -f "$SOURCE_ROOT/snowball/CMakeLists.txt"
cp "$WORK_DIR/duckdb-fts/extension/fts/"{fts_extension.cpp,fts_indexing.cpp,indexing.sql} \
    "$SOURCE_ROOT/fts/"
cp "$WORK_DIR/duckdb-fts/extension/fts/include/"{fts_extension.hpp,fts_indexing.hpp} \
    "$SOURCE_ROOT/fts/include/"
cp "$WORK_DIR/duckdb-fts/LICENSE" "$SOURCE_ROOT/fts/"
find "$SOURCE_ROOT" -type d -exec chmod 0755 {} +
find "$SOURCE_ROOT" -type f -exec chmod 0644 {} +
tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner --format=ustar \
    -C "$WORK_DIR/stage" -cf - duckdb-fts-sources | gzip -n > "$WORK_DIR/duckdb-fts-sources.tar.gz"

cmp "$ARCHIVE" "$WORK_DIR/duckdb-fts-sources.tar.gz"
echo "$ARCHIVE matches its pinned upstream DuckDB and duckdb-fts revisions"
