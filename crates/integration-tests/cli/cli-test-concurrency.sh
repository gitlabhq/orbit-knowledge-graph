#!/usr/bin/env bash
#
# Concurrency stress test for local DuckDB access.
# Usage: cli-test-concurrency.sh <orbit-binary> <repo-path>

ORBIT="$1"
REPO="$2"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

# 1. Seed
"$ORBIT" index "$REPO" > /dev/null 2>&1 \
    && add "seed_index" true \
    || add "seed_index" false "indexing failed"

# 2. Concurrent readers
[ "$(run_concurrent_queries "$Q_FILES" 5 r)" = "true" ] \
    && add "concurrent_readers" true "5 readers, identical" \
    || add "concurrent_readers" false "results differ or failed"

# 3. Reader during writer
"$ORBIT" index "$REPO" > /dev/null 2>&1 & IDX=$!
sleep 0.05
orbit_query "$Q_FILES" /dev/null && rw=true || rw=false
wait "$IDX" || true
add "reader_during_writer" "$rw"

# 4. Concurrent writers
result=$(run_concurrent_writers "$REPO" 2)
[ "$result" != "none" ] \
    && add "concurrent_writers" true "$result succeeded" \
    || add "concurrent_writers" false "both failed"

# 5. Data integrity
orbit_query "$Q_FILES" "$TMP/integrity.json"
count=$(count_nodes "$TMP/integrity.json" "true")
[ "$count" -gt 0 ] && add "data_integrity" true "$count nodes" \
                    || add "data_integrity" false "no nodes"

# 6. Sequential read consistency
[ "$(run_sequential_queries "$Q_FILES" 10 seq)" = "true" ] \
    && add "read_consistency" true "10 reads identical" \
    || add "read_consistency" false "results diverged"

emit_results
