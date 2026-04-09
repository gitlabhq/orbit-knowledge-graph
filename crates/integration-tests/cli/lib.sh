#!/usr/bin/env bash
#
# Shared helpers for CLI integration test scripts.
# Source this at the top of each test script.

set -euo pipefail

export DYLD_LIBRARY_PATH="${ORBIT_LIB_PATH:-}"
export LD_LIBRARY_PATH="${ORBIT_LIB_PATH:-}"

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

export ORBIT_DATA_DIR="$TMP/orbit"

_RESULTS_DB="$TMP/results.duckdb"
duckdb "$_RESULTS_DB" "CREATE TABLE results (name TEXT, ok BOOLEAN, detail TEXT)" 2>/dev/null

# Record a test result.
# Usage: add <name> <true|false> [detail]
add() {
    local name="$1" ok="$2" detail="${3:-}"
    duckdb "$_RESULTS_DB" \
        "INSERT INTO results VALUES ('$name', $ok, '${detail//\'/\'\'}')" 2>/dev/null
}

# Count nodes matching a SQL filter in an orbit JSON output file.
# Usage: count_nodes <json-file> <sql-where-clause>
count_nodes() {
    duckdb -noheader -list -c \
        "SELECT count(*) FROM (SELECT unnest(nodes) AS n FROM read_json('$1')) WHERE $2" \
        2>/dev/null || echo 0
}

# Count edges in an orbit JSON output file.
# Usage: count_edges <json-file>
count_edges() {
    duckdb -noheader -list -c \
        "SELECT count(*) FROM (SELECT unnest(edges) AS e FROM read_json('$1'))" \
        2>/dev/null || echo 0
}

# Emit the final JSON results to stdout. Call this at the end of every test script.
emit_results() {
    duckdb -noheader -list "$_RESULTS_DB" "
        SELECT json_object(
            'pass', (SELECT count(*)::INT FROM results WHERE ok),
            'fail', (SELECT count(*)::INT FROM results WHERE NOT ok),
            'tests', (SELECT json_group_array(
                json_object('name', name, 'ok', ok, 'detail', coalesce(detail, ''))
            ) FROM results)
        )
    " 2>/dev/null
}

# Create a minimal git repo with Python files for testing.
# Usage: init_test_repo <path>
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
