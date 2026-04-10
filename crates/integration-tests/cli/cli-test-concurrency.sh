#!/usr/bin/env bash
#
# Concurrency stress test for local DuckDB access.
# Usage: cli-test-concurrency.sh <orbit-binary> <repo-path>

ORBIT="$1"
REPO="$2"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

NUM_READERS=5
NUM_CONSISTENCY_READS=10

# 1. Seed
"$ORBIT" index "$REPO" > /dev/null 2>&1 \
    && add "seed_index" true \
    || add "seed_index" false "indexing failed"

# 2. Concurrent readers
pids=()
for i in $(seq 1 "$NUM_READERS"); do
    orbit_query "$Q_FILES" "$TMP/r$i.json" &
    pids+=($!)
done
all_ok=true
for pid in "${pids[@]}"; do wait "$pid" || all_ok=false; done

if $all_ok && diff -q "$TMP/r1.json" "$TMP/r2.json" > /dev/null 2>&1 \
           && diff -q "$TMP/r1.json" "$TMP/r3.json" > /dev/null 2>&1; then
    add "concurrent_readers" true "$NUM_READERS readers, identical results"
else
    add "concurrent_readers" false "results differ or query failed"
fi

# 3. Reader during writer
"$ORBIT" index "$REPO" > /dev/null 2>&1 &
IDX=$!
sleep 0.05
rw_ok=false
orbit_query "$Q_FILES" /dev/null && rw_ok=true
wait "$IDX" || true
add "reader_during_writer" "$rw_ok"

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
orbit_query "$Q_FILES" "$TMP/integrity.json"
count=$(count_nodes "$TMP/integrity.json" "true")
[ "$count" -gt 0 ] && add "data_integrity" true "$count nodes" \
                    || add "data_integrity" false "no nodes"

# 6. Sequential read consistency
orbit_query "$Q_FILES" "$TMP/base.json"
consistent=true
for _ in $(seq 2 "$NUM_CONSISTENCY_READS"); do
    orbit_query "$Q_FILES" "$TMP/cmp.json"
    diff -q "$TMP/base.json" "$TMP/cmp.json" > /dev/null 2>&1 || { consistent=false; break; }
done
add "read_consistency" "$consistent" "$NUM_CONSISTENCY_READS reads"

emit_results
