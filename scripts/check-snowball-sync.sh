#!/usr/bin/env bash
set -euo pipefail

SNOWBALL_DIR="crates/duckdb-client/third_party/snowball"
FTS_DIR="crates/duckdb-client/third_party/fts"
DUCKDB_PIN=$(awk '$1 == "duckdb:" { print $2 }' config/versions.yaml)
SNOWBALL_PIN=$(cat "$SNOWBALL_DIR/PIN")
FTS_DUCKDB_PIN=$(awk '$1 == "duckdb:" { print $2 }' "$FTS_DIR/PIN")
FTS_REVISION=$(awk '$1 == "revision:" { print $2 }' "$FTS_DIR/PIN")
WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

if [[ "$SNOWBALL_PIN" != "$DUCKDB_PIN" || "$FTS_DUCKDB_PIN" != "$DUCKDB_PIN" ]]; then
    echo "Vendored Snowball and FTS pins must match DuckDB $DUCKDB_PIN" >&2
    exit 1
fi

git -c advice.detachedHead=false clone --quiet --depth 1 --branch "$DUCKDB_PIN" \
    --filter=blob:none --sparse https://github.com/duckdb/duckdb.git "$WORK_DIR/duckdb"
git -C "$WORK_DIR/duckdb" sparse-checkout set third_party/snowball
rm "$WORK_DIR/duckdb/third_party/snowball/CMakeLists.txt"
diff -ru --exclude PIN "$SNOWBALL_DIR" "$WORK_DIR/duckdb/third_party/snowball"
echo "$SNOWBALL_DIR matches DuckDB $DUCKDB_PIN"

git -C "$WORK_DIR" init --quiet duckdb-fts
git -C "$WORK_DIR/duckdb-fts" remote add origin https://github.com/duckdb/duckdb-fts.git
git -C "$WORK_DIR/duckdb-fts" fetch --quiet --depth 1 origin "$FTS_REVISION"
git -C "$WORK_DIR/duckdb-fts" checkout --quiet --detach FETCH_HEAD
mkdir -p "$WORK_DIR/fts/include"
cp "$WORK_DIR/duckdb-fts/extension/fts/"{fts_extension.cpp,fts_indexing.cpp,indexing.sql} "$WORK_DIR/fts/"
cp "$WORK_DIR/duckdb-fts/extension/fts/include/"{fts_extension.hpp,fts_indexing.hpp} "$WORK_DIR/fts/include/"
cp "$WORK_DIR/duckdb-fts/LICENSE" "$WORK_DIR/fts/"
diff -ru --exclude PIN "$FTS_DIR" "$WORK_DIR/fts"
echo "$FTS_DIR matches duckdb-fts $FTS_REVISION for DuckDB $DUCKDB_PIN"
