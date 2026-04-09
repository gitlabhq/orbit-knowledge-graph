#!/usr/bin/env bash
#
# Stress test for concurrent DuckDB access across processes.
#
# Spawns real orbit processes to validate:
#   - Multiple readers can query simultaneously
#   - Readers succeed while writers hold the lock
#   - Multiple writers retry and all succeed
#   - Data stays consistent throughout
#
# Usage:
#   ./scripts/test_concurrency.sh [options] <path/to/repo>
#
# Options:
#   --readers N      Concurrent reader processes (default: 5)
#   --writers N      Concurrent writer processes (default: 3)
#   --rounds N       Read+write rounds (default: 3)
#   --query-burst N  Queries per reader per round (default: 10)

set -euo pipefail

READERS=5
WRITERS=3
ROUNDS=3
BURST=10
REPO=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --readers)     READERS="$2"; shift 2;;
        --writers)     WRITERS="$2"; shift 2;;
        --rounds)      ROUNDS="$2"; shift 2;;
        --query-burst) BURST="$2"; shift 2;;
        -*)            echo "Unknown option: $1" >&2; exit 1;;
        *)             REPO="$1"; shift;;
    esac
done

if [[ -z "$REPO" ]]; then
    echo "Usage: $0 [options] <path/to/repo>" >&2
    exit 1
fi

# Resolve paths and build.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
export DYLD_LIBRARY_PATH="$PROJECT_ROOT/target/debug/deps"
ORBIT="$PROJECT_ROOT/target/debug/orbit"
DB="$HOME/.orbit/graph.duckdb"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

echo "Building orbit..."
cargo build -p orbit -q 2>/dev/null

PASS=0; FAIL=0
pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }

# Print unique errors from collected log files matching a glob.
print_errors() {
    local pattern="$1"
    for f in $pattern; do
        [[ -s "$f" ]] || continue
        echo "    $(basename "$f"):"
        sort -u "$f" | head -3 | sed 's/^/      /'
    done
}

# Run N background workers. Each runs $cmd and writes "ok" or "fail" to
# $TMP/${prefix}_${i}. Stderr goes to $TMP/${prefix}_${i}.err.
run_workers() {
    local n="$1" prefix="$2"; shift 2
    local cmd=("$@")
    local pids=()
    for i in $(seq 1 "$n"); do
        (
            if "${cmd[@]}" > /dev/null 2>> "$TMP/${prefix}_${i}.err"; then
                echo "ok"
            else
                echo "fail"
            fi > "$TMP/${prefix}_${i}"
        ) &
        pids+=($!)
    done
    for pid in "${pids[@]}"; do wait "$pid" || true; done
}

# Tally results from run_workers output files.
tally() {
    local prefix="$1" n="$2"
    local ok=0 fail=0
    for i in $(seq 1 "$n"); do
        [[ -f "$TMP/${prefix}_${i}" ]] && [[ "$(cat "$TMP/${prefix}_${i}")" == "ok" ]] \
            && ok=$((ok + 1)) || fail=$((fail + 1))
    done
    echo "$ok $fail"
}

# Run N readers, each doing $BURST queries. Tracks ok/err counts.
run_readers() {
    local n="$1" prefix="$2" burst="$3"
    local queries=("$SEARCH" "$TRAVERSAL" "$CONTENT")
    local pids=()
    for i in $(seq 1 "$n"); do
        (
            local ok=0 err=0
            for j in $(seq 1 "$burst"); do
                local q="${queries[$(( j % 3 ))]}"
                if "$ORBIT" query "$q" > /dev/null 2>> "$TMP/${prefix}_${i}.err"; then
                    ok=$((ok + 1))
                else
                    err=$((err + 1))
                fi
            done
            echo "$ok $err" > "$TMP/${prefix}_${i}"
        ) &
        pids+=($!)
    done
    for pid in "${pids[@]}"; do wait "$pid" || true; done
}

# Tally reader results (ok/err counts per file).
tally_readers() {
    local prefix="$1" n="$2"
    local total_ok=0 total_err=0
    for i in $(seq 1 "$n"); do
        if [[ -f "$TMP/${prefix}_${i}" ]]; then
            read -r ok err < "$TMP/${prefix}_${i}"
            total_ok=$((total_ok + ok))
            total_err=$((total_err + err))
        fi
    done
    echo "$total_ok $total_err"
}

# ── Queries ──────────────────────────────────────────────────────
SEARCH='{"query_type":"search","node":{"id":"f","entity":"File","columns":["id","name","path"]},"limit":5}'
TRAVERSAL='{"query_type":"traversal","nodes":[{"id":"f","entity":"File","columns":["id","name"]},{"id":"d","entity":"Definition","columns":["id","name","definition_type"]}],"relationships":[{"type":"DEFINES","from":"f","to":"d"}],"limit":5}'
CONTENT='{"query_type":"search","node":{"id":"f","entity":"File","columns":["id","name","content"]},"limit":2}'

echo ""
echo "=== DuckDB concurrency stress test ==="
echo "Repo:    $REPO"
echo "Config:  ${READERS}r x ${WRITERS}w x ${ROUNDS} rounds, ${BURST} queries/burst"
echo ""

rm -f "$DB"

# ── 1. Seed ──────────────────────────────────────────────────────
echo "--- 1. Seed index"
if "$ORBIT" index "$REPO" 2>/dev/null; then pass "seed"; else fail "seed"; fi

# ── 2. Reader storm (no writers) ─────────────────────────────────
echo "--- 2. $READERS concurrent readers x $BURST queries"
run_readers "$READERS" "t2_reader" "$BURST"
read -r rok rerr <<< "$(tally_readers t2_reader "$READERS")"
if [[ $rerr -eq 0 ]]; then
    pass "$rok queries, 0 errors"
else
    fail "$rok ok, $rerr errors"
    print_errors "$TMP/t2_reader_*.err"
fi

# ── 3. Mixed readers + writers ───────────────────────────────────
echo "--- 3. $ROUNDS rounds: $WRITERS writers + $READERS readers x $BURST"
for round in $(seq 1 "$ROUNDS"); do
    run_workers "$WRITERS" "t3r${round}_w" "$ORBIT" index "$REPO"
    sleep 0.05
    run_readers "$READERS" "t3r${round}_r" "$BURST"
    # Wait for stragglers
    wait 2>/dev/null || true

    read -r wok wfail <<< "$(tally "t3r${round}_w" "$WRITERS")"
    read -r rok rerr <<< "$(tally_readers "t3r${round}_r" "$READERS")"

    label="round $round: writers=$wok/$WRITERS, reads=$rok ok/$rerr err"
    if [[ $wfail -eq 0 && $rerr -eq 0 ]]; then
        pass "$label"
    else
        fail "$label"
        print_errors "$TMP/t3r${round}_*.err"
    fi
done

# ── 4. Rapid sequential re-index ─────────────────────────────────
echo "--- 4. Rapid re-index x5"
rapid_ok=true
for _ in $(seq 1 5); do
    "$ORBIT" index "$REPO" > /dev/null 2>/dev/null || { rapid_ok=false; break; }
done
if $rapid_ok; then
    count=$("$ORBIT" query "$SEARCH" 2>/dev/null | grep -c '"type"' || true)
    if [[ $count -gt 0 ]]; then pass "$count nodes intact"; else fail "no data"; fi
else
    fail "re-index failed"
fi

# ── 5. Sequential read consistency ───────────────────────────────
echo "--- 5. 20 sequential reads, check consistency"
"$ORBIT" query "$SEARCH" > "$TMP/baseline.json" 2>/dev/null
consistent=true
for i in $(seq 2 20); do
    "$ORBIT" query "$SEARCH" > "$TMP/check.json" 2>/dev/null
    diff -q "$TMP/baseline.json" "$TMP/check.json" > /dev/null 2>&1 || { consistent=false; break; }
done
if $consistent; then pass "identical"; else fail "diverged at read $i"; fi

# ── Summary ──────────────────────────────────────────────────────
echo ""
echo "=== $PASS passed, $FAIL failed ==="
exit "$FAIL"
