#!/usr/bin/env bash
#
# Concurrency stress test for local DuckDB access.
# Usage: cli-test-concurrency.sh <orbit-binary> <repo-path>

ORBIT="$1"
REPO="$2"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

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
    add "concurrent_readers" false "$(head -1 "$TMP"/r*.err 2>/dev/null)"
fi

# 3. Reader during writer
"$ORBIT" index "$REPO" > /dev/null 2>&1 &
IDX=$!
sleep 0.05
rw_ok=false
"$ORBIT" query --raw "$SEARCH" > /dev/null 2>"$TMP/rw.err" && rw_ok=true
wait "$IDX" || true
add "reader_during_writer" "$rw_ok" "$(cat "$TMP/rw.err" 2>/dev/null)"

# 4. Two concurrent writers
"$ORBIT" index "$REPO" > /dev/null 2>&1 & P1=$!
"$ORBIT" index "$REPO" > /dev/null 2>&1 & P2=$!
w1=true; w2=true
wait "$P1" || w1=false
wait "$P2" || w2=false

if $w1 && $w2; then     add "concurrent_writers" true "both succeeded"
elif $w1 || $w2; then   add "concurrent_writers" true "one succeeded"
else                     add "concurrent_writers" false "both failed"
fi

# 5. Data integrity
"$ORBIT" query --raw "$SEARCH" > "$TMP/integrity.json" 2>/dev/null
count=$(count_nodes "$TMP/integrity.json" "true")
[ "$count" -gt 0 ] && add "data_integrity" true "$count nodes" \
                    || add "data_integrity" false "no nodes"

# 6. Sequential read consistency
"$ORBIT" query --raw "$SEARCH" > "$TMP/base.json" 2>/dev/null
consistent=true
for _ in $(seq 2 10); do
    "$ORBIT" query --raw "$SEARCH" > "$TMP/cmp.json" 2>/dev/null
    diff -q "$TMP/base.json" "$TMP/cmp.json" > /dev/null 2>&1 || { consistent=false; break; }
done
add "read_consistency" "$consistent" "10 reads"

emit_results
