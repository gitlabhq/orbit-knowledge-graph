#!/usr/bin/env bash
#
# Shared helpers for CLI integration test scripts.
# Source this at the top of each test script.
#
# Assertions are DuckDB macros in harness.sql. This file provides
# process orchestration (indexing, concurrent spawning, etc).

set -euo pipefail

export DYLD_LIBRARY_PATH="${ORBIT_LIB_PATH:-}"
export LD_LIBRARY_PATH="${ORBIT_LIB_PATH:-}"

SCRIPT_DIR="${SCRIPT_DIR:-$(cd "$(dirname "$0")" && pwd)}"

# ── Common queries ───────────────────────────────────────────────
Q_FILES='{"query_type":"search","node":{"id":"f","entity":"File","columns":["id","name","path","branch","commit_sha","content"]},"limit":50}'
Q_FILES_SIMPLE='{"query_type":"search","node":{"id":"f","entity":"File","columns":["id","name","path"]},"limit":50}'
Q_TRAVERSAL='{"query_type":"traversal","nodes":[{"id":"f","entity":"File","columns":["id","name"]},{"id":"d","entity":"Definition","columns":["id","name"]}],"relationships":[{"type":"DEFINES","from":"f","to":"d"}],"limit":10}'

# ── DuckDB harness ───────────────────────────────────────────────
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
export ORBIT_DATA_DIR="$TMP/orbit"

_DB="$TMP/harness.duckdb"
duckdb "$_DB" ".read $SCRIPT_DIR/harness.sql" 2>/dev/null

db()    { duckdb "$_DB" "$1" 2>/dev/null; }
dbval() { duckdb -noheader -list "$_DB" "$1" 2>/dev/null; }

add()          { db "INSERT INTO results VALUES ('$1', $2, '${3:-}')"; }
orbit_query()  { "$ORBIT" query --raw "$1" > "$2" 2>/dev/null; }
emit_results() { dbval "SELECT json FROM test_output"; }
all_identical(){ dbval "
    WITH per_file AS (
        SELECT filename, list_sort(list(n.id)) AS ids
        FROM read_json('$1', filename=true), unnest(nodes) AS t(n)
        GROUP BY filename
    )
    SELECT count(DISTINCT ids) = 1 FROM per_file"; }

# ── Process helpers ──────────────────────────────────────────────

index_repos() {
    for r in "$@"; do
        local name; name=$(basename "$r")
        "$ORBIT" index "$r" > /dev/null 2>&1 \
            && add "index_$name" true \
            || add "index_$name" false "indexing failed"
    done
}

run_concurrent_queries() {
    local query="$1" n="$2" prefix="$3" pids=()
    for i in $(seq 1 "$n"); do
        orbit_query "$query" "$TMP/${prefix}${i}.json" & pids+=($!)
    done
    local ok=true
    for pid in "${pids[@]}"; do wait "$pid" || ok=false; done
    $ok && all_identical "$TMP/${prefix}*.json" || echo "false"
}

run_sequential_queries() {
    local query="$1" n="$2" prefix="$3"
    for i in $(seq 1 "$n"); do orbit_query "$query" "$TMP/${prefix}${i}.json"; done
    all_identical "$TMP/${prefix}*.json"
}

run_concurrent_writers() {
    local repo="$1" n="$2" pids=() ok=0
    for _ in $(seq 1 "$n"); do "$ORBIT" index "$repo" > /dev/null 2>&1 & pids+=($!); done
    for pid in "${pids[@]}"; do wait "$pid" && ok=$((ok + 1)) || true; done
    if [ "$ok" -eq "$n" ]; then echo "all"
    elif [ "$ok" -gt 0 ];  then echo "some"
    else echo "none"; fi
}


