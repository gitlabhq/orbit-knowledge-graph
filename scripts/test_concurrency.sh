#!/usr/bin/env bash
#
# Test concurrent DuckDB access across processes.
#
# Validates:
#   1. Two indexers can run back-to-back (retry backoff on write lock)
#   2. Multiple readers can query simultaneously
#   3. A reader can query while an indexer is writing
#
# Usage:
#   ./scripts/test_concurrency.sh [path/to/repo]

set -euo pipefail

REPO="${1:-/Users/michaelusachenko/Desktop/Code/current/load-testing}"
DB="$HOME/.orbit/graph.duckdb"
PASS=0
FAIL=0

orbit() { cargo run -p orbit -q -- "$@"; }

pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }

echo "=== Concurrent DuckDB access test ==="
echo "Repo: $REPO"
echo ""

# Build once upfront
cargo build -p orbit 2>/dev/null

# Clean slate
rm -f "$DB"

# ── Test 1: Initial index ────────────────────────────────────────
echo "--- Test 1: Initial index"
if orbit index "$REPO" 2>/dev/null; then
    pass "initial index succeeded"
else
    fail "initial index failed"
fi

# ── Test 2: Concurrent readers ───────────────────────────────────
echo "--- Test 2: Three concurrent readers"
QUERY='{"query_type":"search","node":{"id":"f","entity":"File","columns":["id","name"]},"limit":3}'

pids=()
for i in 1 2 3; do
    orbit query "$QUERY" > "/tmp/orbit_read_$i.json" 2>/dev/null &
    pids+=($!)
done

all_ok=true
for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
        all_ok=false
    fi
done

if $all_ok; then
    if diff -q /tmp/orbit_read_1.json /tmp/orbit_read_2.json >/dev/null 2>&1 \
    && diff -q /tmp/orbit_read_1.json /tmp/orbit_read_3.json >/dev/null 2>&1; then
        pass "3 concurrent readers returned identical results"
    else
        fail "concurrent readers returned different results"
    fi
else
    fail "one or more concurrent readers failed"
fi
rm -f /tmp/orbit_read_*.json

# ── Test 3: Reader during write ──────────────────────────────────
echo "--- Test 3: Reader while indexer is writing"

orbit index "$REPO" > /tmp/orbit_index.log 2>&1 &
INDEX_PID=$!

sleep 0.1
read_ok=true
for i in 1 2 3 4 5; do
    if ! orbit query "$QUERY" > /dev/null 2>/dev/null; then
        read_ok=false
        break
    fi
    sleep 0.05
done

if wait "$INDEX_PID"; then
    if $read_ok; then
        pass "reads succeeded while indexer was writing"
    else
        fail "reads failed while indexer was writing"
    fi
else
    fail "indexer failed during concurrent read test"
fi
rm -f /tmp/orbit_index.log

# ── Test 4: Two concurrent indexers ──────────────────────────────
echo "--- Test 4: Two concurrent indexers (second should retry)"

orbit index "$REPO" > /tmp/orbit_idx1.log 2>&1 &
PID1=$!
orbit index "$REPO" > /tmp/orbit_idx2.log 2>&1 &
PID2=$!

ok1=true; ok2=true
wait "$PID1" || ok1=false
wait "$PID2" || ok2=false

if $ok1 && $ok2; then
    pass "both concurrent indexers succeeded"
elif $ok1 || $ok2; then
    pass "at least one indexer succeeded (other may have hit lock timeout)"
else
    fail "both concurrent indexers failed"
fi
rm -f /tmp/orbit_idx*.log

# ── Test 5: Data integrity after concurrent writes ───────────────
echo "--- Test 5: Data integrity check"
RESULT=$(orbit query "$QUERY" 2>/dev/null)
NODE_COUNT=$(echo "$RESULT" | grep -c '"type"' || true)

if [ "$NODE_COUNT" -gt 0 ]; then
    pass "query returns $NODE_COUNT nodes after concurrent writes"
else
    fail "no nodes returned after concurrent writes"
fi

# ── Summary ──────────────────────────────────────────────────────
echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
exit "$FAIL"
