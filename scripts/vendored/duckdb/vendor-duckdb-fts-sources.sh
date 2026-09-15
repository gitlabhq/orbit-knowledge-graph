#!/usr/bin/env bash
set -euo pipefail

# Regenerate the vendored DuckDB FTS source archive.
#
# Called by `mise vendor duckdb` which sets:
#   VENDOR_VERSIONS_FILE  — absolute path to config/versions.yaml
#   VENDOR_DIR            — absolute path to the vendor directory
#   VENDOR_VERSION        — duckdb version (e.g. v1.5.5)
#   VENDOR_NAME           — "duckdb"
#
# Can also be called directly; falls back to repo-relative paths.

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)

VERSIONS_FILE="${VENDOR_VERSIONS_FILE:-$REPO_ROOT/config/versions.yaml}"
DUCKDB_PIN="${VENDOR_VERSION:-$(yq '.vendored.duckdb.version' "$VERSIONS_FILE")}"
VENDOR_DIR="${VENDOR_DIR:-$REPO_ROOT/$(yq '.vendored.duckdb.vendor_dir' "$VERSIONS_FILE")}"
FTS_REVISION=$(yq '.vendored.duckdb.extensions.fts.source_revision' "$VERSIONS_FILE")

ARCHIVE="$VENDOR_DIR/duckdb-fts-sources.tar.gz"
OUTPUT=${1:-$ARCHIVE}
WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT
SOURCE_ROOT="$WORK_DIR/stage/duckdb-fts-sources"

export LC_ALL=C
git -c advice.detachedHead=false clone --quiet --depth 1 --branch "$DUCKDB_PIN" \
    --filter=blob:none --sparse https://github.com/duckdb/duckdb.git "$WORK_DIR/duckdb"
git -C "$WORK_DIR/duckdb" sparse-checkout set third_party/snowball

git -C "$WORK_DIR" init --quiet duckdb-fts
git -C "$WORK_DIR/duckdb-fts" remote add origin https://github.com/duckdb/duckdb-fts.git
git -C "$WORK_DIR/duckdb-fts" fetch --quiet --depth 1 origin "$FTS_REVISION"
git -C "$WORK_DIR/duckdb-fts" checkout --quiet --detach FETCH_HEAD

mkdir -p "$SOURCE_ROOT/fts/include"
cp -R "$WORK_DIR/duckdb/third_party/snowball" "$SOURCE_ROOT/snowball"
rm "$SOURCE_ROOT/snowball/CMakeLists.txt"
cp "$WORK_DIR/duckdb-fts/extension/fts/"{fts_extension.cpp,fts_indexing.cpp,indexing.sql} \
    "$SOURCE_ROOT/fts/"
cp "$WORK_DIR/duckdb-fts/extension/fts/include/"{fts_extension.hpp,fts_indexing.hpp} \
    "$SOURCE_ROOT/fts/include/"
cp "$WORK_DIR/duckdb-fts/LICENSE" "$SOURCE_ROOT/fts/"
find "$SOURCE_ROOT" -type d -exec chmod 0755 {} +
find "$SOURCE_ROOT" -type f -exec chmod 0644 {} +
mkdir -p "$(dirname "$OUTPUT")"
tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner --format=ustar \
    -C "$WORK_DIR/stage" -cf - duckdb-fts-sources | gzip -n > "$OUTPUT"

SHA256=$(sha256sum "$OUTPUT" | awk '{ print $1 }')
if [[ $# -eq 0 ]]; then
    yq -i ".vendored.duckdb.extensions.fts.source_archive_sha256 = \"$SHA256\"" "$VERSIONS_FILE"
fi
echo "$OUTPUT: $SHA256"
