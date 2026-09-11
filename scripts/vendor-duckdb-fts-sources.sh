#!/usr/bin/env bash
set -euo pipefail

# Regenerate the committed archive from the repository root with:
# ./scripts/vendor-duckdb-fts-sources.sh

PIN="crates/duckdb-client/third_party/duckdb-fts-sources.PIN"
ARCHIVE="crates/duckdb-client/third_party/duckdb-fts-sources.tar.gz"
OUTPUT=${1:-$ARCHIVE}
DUCKDB_PIN=$(awk '$1 == "duckdb:" { print $2 }' "$PIN")
FTS_REVISION=$(awk '$1 == "duckdb_fts_revision:" { print $2 }' "$PIN")
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
    awk -v sha="$SHA256" '$1 == "archive_sha256:" { $2 = sha } { print }' "$PIN" > "$PIN.tmp"
    mv "$PIN.tmp" "$PIN"
fi
echo "$OUTPUT: $SHA256"
