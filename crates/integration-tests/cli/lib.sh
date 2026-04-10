#!/usr/bin/env bash
#
# Shared helpers for CLI integration test scripts.
# Source this at the top of each test script.

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

# Run SQL against the harness DB (has results table + macros).
db()    { duckdb "$_DB" "$1" 2>/dev/null; }
dbval() { duckdb -noheader -list "$_DB" "$1" 2>/dev/null; }

# ── Primitives ───────────────────────────────────────────────────

add()         { db "INSERT INTO results VALUES ('$1', $2, '${3:-}')"; }
orbit_query() { "$ORBIT" query --raw "$1" > "$2" 2>/dev/null; }
emit_results(){ dbval "SELECT json FROM test_output"; }

# ── Assertions (single DuckDB call each, using harness macros) ──

# Assert nodes matching a WHERE clause exist.
assert_has() {
    db "INSERT INTO results
        SELECT '$1', c > 0, CASE WHEN c > 0 THEN c || ' matches' ELSE 'not found' END
        FROM (SELECT count(*)::INT AS c FROM orbit_nodes('$2') WHERE $3)"
}

# Assert exact node count.
assert_count() {
    db "INSERT INTO results
        SELECT '$1', c = $4, CASE WHEN c = $4 THEN '${5:-}' ELSE 'expected $4, got ' || c END
        FROM (SELECT count(*)::INT AS c FROM orbit_nodes('$2') WHERE $3)"
}

# Assert edges exist.
assert_edges() {
    db "INSERT INTO results
        SELECT '$1', c > 0, CASE WHEN c > 0 THEN c || ' edges' ELSE 'no edges' END
        FROM (SELECT count(*)::INT AS c FROM orbit_edges('$2'))"
}

# Check all JSON files matching a glob have the same node IDs.
all_identical() { dbval "SELECT ok FROM files_same('$1')"; }

# ── Concurrency helpers ─────────────────────────────────────────

run_concurrent_queries() {
    local query="$1" n="$2" prefix="$3" pids=()
    for i in $(seq 1 "$n"); do
        orbit_query "$query" "$TMP/${prefix}${i}.json" &
        pids+=($!)
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

# ── Repo helpers ─────────────────────────────────────────────────

index_repos() {
    for r in "$@"; do
        local name; name=$(basename "$r")
        "$ORBIT" index "$r" > /dev/null 2>&1 \
            && add "index_$name" true \
            || add "index_$name" false "indexing failed"
    done
}

add_worktree() {
    local repo="$1" branch="$2" wt="$3" base="${4:-}"
    if [ -n "$base" ]; then
        cd "$repo" && git worktree add -q -b "$branch" "$wt" "$base" 2>/dev/null
    else
        cd "$repo" && git worktree add -q -b "$branch" "$wt"
    fi
}

init_test_repo() {
    local repo="$1"
    mkdir -p "$repo/src"
    cat > "$repo/src/main.py" << 'PY'
def hello():
    print("hello")

class App:
    def run(self):
        hello()
PY
    cat > "$repo/src/utils.py" << 'PY'
import os

def read_file(path):
    return open(path).read()
PY
    cd "$repo"
    git init -q
    git config user.email "test@test.com"
    git config user.name "Test"
    git add -A && git commit -q -m "initial"
    cd - > /dev/null
}
