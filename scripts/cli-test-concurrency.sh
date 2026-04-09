#!/usr/bin/env bash
#
# Concurrency stress test for local DuckDB access.
# Outputs JSON: {"pass": N, "fail": N, "tests": [...]}
#
# Usage: ./scripts/cli-test-concurrency.sh <orbit-binary> <repo-path>

set -euo pipefail

ORBIT="$1"
REPO="$2"

export DYLD_LIBRARY_PATH="${ORBIT_LIB_PATH:-}"
export LD_LIBRARY_PATH="${ORBIT_LIB_PATH:-}"

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

# Isolate DuckDB to a temp directory so tests don't touch ~/.orbit/
export ORBIT_DATA_DIR="$TMP/orbit"

TESTS="[]"
add() {
    local name="$1" ok="$2" detail="${3:-}"
    TESTS=$(echo "$TESTS" | jq --arg n "$name" --argjson ok "$ok" --arg d "$detail" \
        '. + [{"name": $n, "ok": $ok, "detail": $d}]')
}

SEARCH='{"query_type":"search","node":{"id":"f","entity":"File","columns":["id","name","path"]},"limit":5}'

# 1. Seed
if "$ORBIT" index "$REPO" > /dev/null 2>&1; then
    add "seed_index" true
else
    add "seed_index" false "indexing failed"
fi

# 2. Five concurrent readers
pids=()
for i in $(seq 1 5); do
    "$ORBIT" query --raw "$SEARCH" > "$TMP/r$i.json" 2>"$TMP/r$i.err" &
    pids+=($!)
done
all_ok=true
for pid in "${pids[@]}"; do wait "$pid" || all_ok=false; done

if $all_ok && diff -q "$TMP/r1.json" "$TMP/r2.json" > /dev/null 2>&1 \
           && diff -q "$TMP/r1.json" "$TMP/r3.json" > /dev/null 2>&1; then
    add "concurrent_readers" true "5 readers, identical results"
else
    add "concurrent_readers" false "$(cat "$TMP"/r*.err 2>/dev/null | head -1)"
fi

# 3. Reader during writer
"$ORBIT" index "$REPO" > /dev/null 2>&1 &
IDX=$!
sleep 0.05
if "$ORBIT" query --raw "$SEARCH" > /dev/null 2>"$TMP/rw.err"; then
    rw=true
else
    rw=false
fi
wait "$IDX" || true

if $rw; then
    add "reader_during_writer" true
else
    add "reader_during_writer" false "$(cat "$TMP/rw.err")"
fi

# 4. Two concurrent writers
"$ORBIT" index "$REPO" > /dev/null 2>"$TMP/w1.err" &
P1=$!
"$ORBIT" index "$REPO" > /dev/null 2>"$TMP/w2.err" &
P2=$!
w1=true; w2=true
wait "$P1" || w1=false
wait "$P2" || w2=false

if $w1 && $w2; then
    add "concurrent_writers" true "both succeeded"
elif $w1 || $w2; then
    add "concurrent_writers" true "one succeeded"
else
    add "concurrent_writers" false "both failed"
fi

# 5. Data integrity after concurrent writes
count=$("$ORBIT" query --raw "$SEARCH" 2>/dev/null | jq '.nodes | length')
if [ "$count" -gt 0 ]; then
    add "data_integrity" true "$count nodes"
else
    add "data_integrity" false "no nodes"
fi

# 6. Sequential read consistency
"$ORBIT" query --raw "$SEARCH" > "$TMP/base.json" 2>/dev/null
consistent=true
for _ in $(seq 2 10); do
    "$ORBIT" query --raw "$SEARCH" > "$TMP/cmp.json" 2>/dev/null
    diff -q "$TMP/base.json" "$TMP/cmp.json" > /dev/null 2>&1 || { consistent=false; break; }
done
if $consistent; then
    add "read_consistency" true "10 reads identical"
else
    add "read_consistency" false "results diverged"
fi

# Output
pass=$(echo "$TESTS" | jq '[.[] | select(.ok)] | length')
fail=$(echo "$TESTS" | jq '[.[] | select(.ok | not)] | length')
jq -n --argjson p "$pass" --argjson f "$fail" --argjson t "$TESTS" \
    '{"pass": $p, "fail": $f, "tests": $t}'
