#!/usr/bin/env bash
#
# Shared helpers for CLI integration test scripts.
# Source this at the top of each test script.

set -euo pipefail

export DYLD_LIBRARY_PATH="${ORBIT_LIB_PATH:-}"
export LD_LIBRARY_PATH="${ORBIT_LIB_PATH:-}"

# ── Common queries ───────────────────────────────────────────────
Q_FILES='{"query_type":"search","node":{"id":"f","entity":"File","columns":["id","name","path"]},"limit":20}'
Q_FILES_BRANCH='{"query_type":"search","node":{"id":"f","entity":"File","columns":["id","name","branch"]},"limit":20}'
Q_FILES_SHA='{"query_type":"search","node":{"id":"f","entity":"File","columns":["id","commit_sha"]},"limit":20}'
Q_FILES_CONTENT='{"query_type":"search","node":{"id":"f","entity":"File","columns":["id","name","branch","content"]},"limit":20}'
Q_TRAVERSAL='{"query_type":"traversal","nodes":[{"id":"f","entity":"File","columns":["id","name"]},{"id":"d","entity":"Definition","columns":["id","name"]}],"relationships":[{"type":"DEFINES","from":"f","to":"d"}],"limit":10}'

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

# Run an orbit query and write raw JSON output to a file.
# Usage: orbit_query <query-json> <output-file>
orbit_query() {
    "$ORBIT" query --raw "$1" > "$2" 2>/dev/null
}

# Run N concurrent orbit queries, wait for all, check results are identical.
# Usage: run_concurrent_queries <query-json> <n> <prefix>
# Returns: "true" if all succeeded and returned identical results, "false" otherwise.
run_concurrent_queries() {
    local query="$1" n="$2" prefix="$3"
    local pids=()
    for i in $(seq 1 "$n"); do
        orbit_query "$query" "$TMP/${prefix}$i.json" &
        pids+=($!)
    done
    local all_ok=true
    for pid in "${pids[@]}"; do wait "$pid" || all_ok=false; done

    local files=()
    for i in $(seq 1 "$n"); do files+=("$TMP/${prefix}$i.json"); done

    $all_ok && all_identical "${files[@]}" || echo "false"
}

# Run N sequential orbit queries and check all results are identical.
# Usage: run_sequential_queries <query-json> <n> <prefix>
run_sequential_queries() {
    local query="$1" n="$2" prefix="$3"
    local files=()
    for i in $(seq 1 "$n"); do
        orbit_query "$query" "$TMP/${prefix}$i.json"
        files+=("$TMP/${prefix}$i.json")
    done
    all_identical "${files[@]}"
}

# Check if all files have identical content.
# Usage: all_identical <file1> <file2> [file3...]
all_identical() {
    duckdb -noheader -list -c "
        SELECT count(DISTINCT content) = 1
        FROM read_text([$(printf "'%s'," "$@" | sed 's/,$//')])
    " 2>/dev/null
}

# Run concurrent orbit index processes, return "both"/"one"/"none".
# Usage: run_concurrent_writers <repo-path> <n>
run_concurrent_writers() {
    local repo="$1" n="$2"
    local pids=() ok=0
    for _ in $(seq 1 "$n"); do
        "$ORBIT" index "$repo" > /dev/null 2>&1 &
        pids+=($!)
    done
    for pid in "${pids[@]}"; do wait "$pid" && ok=$((ok + 1)) || true; done
    if [ "$ok" -eq "$n" ]; then echo "all"
    elif [ "$ok" -gt 0 ];  then echo "some"
    else                        echo "none"
    fi
}

# Index repos and record pass/fail for each.
# Usage: index_repos <repo1> [repo2...]
index_repos() {
    for r in "$@"; do
        local name
        name=$(basename "$r")
        "$ORBIT" index "$r" > /dev/null 2>&1 \
            && add "index_$name" true \
            || add "index_$name" false "indexing failed"
    done
}

# Assert that a JSON result file has nodes matching a filter.
# Usage: assert_has <test-name> <json-file> <sql-filter> [detail]
assert_has() {
    local name="$1" file="$2" filter="$3" detail="${4:-}"
    local c
    c=$(count_nodes "$file" "$filter")
    [ "$c" -gt 0 ] && add "$name" true "${detail:-$c matches}" \
                    || add "$name" false "not found"
}

# Assert node count equals expected.
# Usage: assert_count <test-name> <json-file> <sql-filter> <expected> [detail]
assert_count() {
    local name="$1" file="$2" filter="$3" expected="$4" detail="${5:-}"
    local c
    c=$(count_nodes "$file" "$filter")
    [ "$c" -eq "$expected" ] && add "$name" true "${detail:-$c matches}" \
                             || add "$name" false "expected $expected, got $c"
}

# Assert content contains a substring for nodes matching a filter.
# Usage: assert_content <test-name> <json-file> <sql-filter> <substring>
assert_content() {
    local name="$1" file="$2" filter="$3" substr="$4"
    local c
    c=$(count_nodes "$file" "$filter AND contains(n.content, '$substr')")
    [ "$c" -gt 0 ] && add "$name" true \
                    || add "$name" false "'$substr' not found"
}

# Create a git worktree, write files, and commit.
# Usage: add_worktree <repo> <branch> <worktree-path> [base-ref]
# Returns: sets WT_SHA to the new commit hash.
add_worktree() {
    local repo="$1" branch="$2" wt_path="$3" base="${4:-}"
    if [ -n "$base" ]; then
        cd "$repo" && git worktree add -q -b "$branch" "$wt_path" "$base" 2>/dev/null
    else
        cd "$repo" && git worktree add -q -b "$branch" "$wt_path"
    fi
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
