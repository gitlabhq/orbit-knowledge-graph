#!/usr/bin/env bash
#
# Stress test for concurrent DuckDB access across processes.
#
# Usage:
#   ./scripts/test_concurrency.sh [options] [path/to/repo]
#
# Options:
#   --readers N      Number of concurrent readers (default: 5)
#   --writers N      Number of concurrent indexers (default: 3)
#   --rounds N       Number of read/write rounds (default: 3)
#   --query-burst N  Queries per reader per round (default: 10)

set -euo pipefail

READERS=5
WRITERS=3
ROUNDS=3
BURST=10
REPO=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --readers)  READERS="$2"; shift 2;;
        --writers)  WRITERS="$2"; shift 2;;
        --rounds)   ROUNDS="$2"; shift 2;;
        --query-burst) BURST="$2"; shift 2;;
        -*) echo "Unknown option: $1"; exit 1;;
        *)  REPO="$1"; shift;;
    esac
done

REPO="${REPO:-/Users/michaelusachenko/Desktop/Code/current/load-testing}"
DB="$HOME/.orbit/graph.duckdb"
TMPDIR=$(mktemp -d)
PASS=0
FAIL=0

# Build once, run the binary directly for speed.
echo "Building orbit..."
cargo build -p orbit 2>/dev/null

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
export DYLD_LIBRARY_PATH="$PROJECT_ROOT/target/debug/deps"
ORBIT="$PROJECT_ROOT/target/debug/orbit"

pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }

cleanup() { rm -rf "$TMPDIR"; }
trap cleanup EXIT

echo ""
echo "=== DuckDB concurrency stress test ==="
echo "Repo:        $REPO"
echo "Readers:     $READERS"
echo "Writers:     $WRITERS"
echo "Rounds:      $ROUNDS"
echo "Query burst: $BURST"
echo ""

rm -f "$DB"

SEARCH='{"query_type":"search","node":{"id":"f","entity":"File","columns":["id","name","path"]},"limit":5}'
TRAVERSAL='{"query_type":"traversal","nodes":[{"id":"f","entity":"File","columns":["id","name"]},{"id":"d","entity":"Definition","columns":["id","name","definition_type"]}],"relationships":[{"type":"DEFINES","from":"f","to":"d"}],"limit":5}'
CONTENT='{"query_type":"search","node":{"id":"f","entity":"File","columns":["id","name","content"]},"limit":2}'

# ── Test 1: Seed index ───────────────────────────────────────────
echo "--- Test 1: Seed index"
if "$ORBIT" index "$REPO" 2>/dev/null; then
    pass "seed index"
else
    fail "seed index"
fi

# ── Test 2: Concurrent reader storm ──────────────────────────────
echo "--- Test 2: $READERS concurrent readers x $BURST queries each"
pids=()
for i in $(seq 1 "$READERS"); do
    (
        ok=0; err=0
        queries=("$SEARCH" "$TRAVERSAL" "$CONTENT")
        for j in $(seq 1 "$BURST"); do
            Q="${queries[$(( j % 3 ))]}"
            if "$ORBIT" query "$Q" > /dev/null 2>> "$TMPDIR/reader_${i}_errors.log"; then
                ok=$((ok + 1))
            else
                err=$((err + 1))
            fi
        done
        echo "$ok $err" > "$TMPDIR/reader_$i.result"
    ) &
    pids+=($!)
done

total_ok=0; total_err=0
for pid in "${pids[@]}"; do
    wait "$pid" || true
done
for i in $(seq 1 "$READERS"); do
    if [[ -f "$TMPDIR/reader_$i.result" ]]; then
        read -r ok err < "$TMPDIR/reader_$i.result"
        total_ok=$((total_ok + ok))
        total_err=$((total_err + err))
    fi
done

if [[ $total_err -eq 0 ]]; then
    pass "$total_ok queries across $READERS readers, 0 errors"
else
    fail "$total_ok ok, $total_err errors across $READERS readers"
    for errlog in "$TMPDIR"/reader_*_errors.log; do
        [[ -f "$errlog" ]] || continue
        echo "    errors from $(basename "$errlog"):"
        sort -u "$errlog" | head -5 | sed 's/^/      /'
    done
fi

# ── Test 3: Readers + writers interleaved ────────────────────────
echo "--- Test 3: $ROUNDS rounds of $WRITERS writers + $READERS readers"
for round in $(seq 1 "$ROUNDS"); do
    pids=()

    for w in $(seq 1 "$WRITERS"); do
        (
            if "$ORBIT" index "$REPO" > /dev/null 2>/dev/null; then
                echo "ok" > "$TMPDIR/round${round}_writer_$w"
            else
                echo "fail" > "$TMPDIR/round${round}_writer_$w"
            fi
        ) &
        pids+=($!)
    done

    sleep 0.05

    for r in $(seq 1 "$READERS"); do
        (
            ok=0; err=0
            for b in $(seq 1 "$BURST"); do
                if "$ORBIT" query "$SEARCH" > /dev/null 2>> "$TMPDIR/round${round}_reader_${r}_errors.log"; then
                    ok=$((ok + 1))
                else
                    err=$((err + 1))
                fi
            done
            echo "$ok $err" > "$TMPDIR/round${round}_reader_$r"
        ) &
        pids+=($!)
    done

    for pid in "${pids[@]}"; do
        wait "$pid" || true
    done

    w_ok=0; w_fail=0; r_ok=0; r_err=0
    for w in $(seq 1 "$WRITERS"); do
        f="$TMPDIR/round${round}_writer_$w"
        [[ -f "$f" ]] && [[ "$(cat "$f")" == "ok" ]] && w_ok=$((w_ok + 1)) || w_fail=$((w_fail + 1))
    done
    for r in $(seq 1 "$READERS"); do
        f="$TMPDIR/round${round}_reader_$r"
        if [[ -f "$f" ]]; then
            read -r ok err < "$f"
            r_ok=$((r_ok + ok))
            r_err=$((r_err + err))
        fi
    done

    label="round $round: writers=$w_ok/$WRITERS, reads=$r_ok ok/$r_err err"
    if [[ $w_fail -eq 0 && $r_err -eq 0 ]]; then
        pass "$label"
    else
        fail "$label"
        # Print unique error messages
        for errlog in "$TMPDIR"/round${round}_reader_*_errors.log; do
            [[ -f "$errlog" ]] || continue
            echo "    errors from $(basename "$errlog"):"
            sort -u "$errlog" | head -5 | sed 's/^/      /'
        done
    fi
done

# ── Test 4: Rapid re-index cycle ─────────────────────────────────
echo "--- Test 4: Rapid re-index x5 then query"
rapid_ok=true
for _ in $(seq 1 5); do
    if ! "$ORBIT" index "$REPO" > /dev/null 2>/dev/null; then
        rapid_ok=false
        break
    fi
done
if $rapid_ok; then
    RESULT=$("$ORBIT" query "$SEARCH" 2>/dev/null)
    NODE_COUNT=$(echo "$RESULT" | grep -c '"type"' || true)
    if [[ $NODE_COUNT -gt 0 ]]; then
        pass "5 rapid re-indexes, $NODE_COUNT nodes intact"
    else
        fail "data gone after rapid re-indexes"
    fi
else
    fail "rapid re-index failed"
fi

# ── Test 5: Query consistency ────────────────────────────────────
echo "--- Test 5: Query result consistency across 20 sequential reads"
consistent=true
"$ORBIT" query "$SEARCH" > "$TMPDIR/baseline.json" 2>/dev/null
for i in $(seq 2 20); do
    "$ORBIT" query "$SEARCH" > "$TMPDIR/check_$i.json" 2>/dev/null
    if ! diff -q "$TMPDIR/baseline.json" "$TMPDIR/check_$i.json" > /dev/null 2>&1; then
        consistent=false
        break
    fi
done
if $consistent; then
    pass "20 sequential reads returned identical results"
else
    fail "sequential reads returned inconsistent results"
fi

# ── Summary ──────────────────────────────────────────────────────
echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
exit "$FAIL"
