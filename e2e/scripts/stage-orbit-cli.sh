#!/usr/bin/env bash
set -euo pipefail

OUT="${1:?usage: stage-orbit-cli.sh <dir>}"
BIN=target/debug/orbit

LIBDUCKDB=$(ldd "$BIN" | awk '$1 ~ /^libduckdb/ && $3 ~ /^\// { print $3; exit }')
[[ -n "$LIBDUCKDB" ]] || { echo "$BIN does not resolve libduckdb"; exit 1; }

mkdir -p "$OUT"
objcopy --strip-debug "$BIN" "$OUT/orbit"
cp "$LIBDUCKDB" "$OUT/"
ls -la "$OUT"
