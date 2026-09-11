#!/usr/bin/env bash
set -euo pipefail

VENDORED_DIR="crates/duckdb-client/third_party/snowball"
DUCKDB_TAG=$(cat "$VENDORED_DIR/PIN")
WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

git -c advice.detachedHead=false clone --quiet --depth 1 --branch "$DUCKDB_TAG" --filter=blob:none --sparse \
    https://github.com/duckdb/duckdb.git "$WORK_DIR/duckdb"
git -C "$WORK_DIR/duckdb" sparse-checkout set third_party/snowball
rm "$WORK_DIR/duckdb/third_party/snowball/CMakeLists.txt"

diff -ru --exclude PIN "$VENDORED_DIR" "$WORK_DIR/duckdb/third_party/snowball"
echo "$VENDORED_DIR matches DuckDB $DUCKDB_TAG"
