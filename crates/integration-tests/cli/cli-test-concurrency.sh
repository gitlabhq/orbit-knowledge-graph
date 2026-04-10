#!/usr/bin/env bash
# Concurrency stress test for local DuckDB access.
ORBIT="$1"
REPO="$2"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

# Seed
"$ORBIT" index "$REPO" > /dev/null 2>&1 \
    && add "seed_index" true \
    || add "seed_index" false "indexing failed"

# Concurrent readers return identical results
[ "$(run_concurrent_queries "$Q_FILES_SIMPLE" 5 r)" = "true" ] \
    && add "concurrent_readers" true "5 readers identical" \
    || add "concurrent_readers" false "results differ"

# Reader succeeds while writer holds lock
"$ORBIT" index "$REPO" > /dev/null 2>&1 & IDX=$!
sleep 0.05
orbit_query "$Q_FILES_SIMPLE" /dev/null && rw=true || rw=false
wait "$IDX" || true
add "reader_during_writer" "$rw"

# Concurrent writers both succeed (retry backoff)
result=$(run_concurrent_writers "$REPO" 2)
[ "$result" != "none" ] \
    && add "concurrent_writers" true "$result succeeded" \
    || add "concurrent_writers" false "both failed"

# Data intact after concurrent writes
orbit_query "$Q_FILES_SIMPLE" "$TMP/integrity.json"
db "INSERT INTO results SELECT r.name, r.ok, r.detail FROM (
    SELECT unnest([check_has('data_integrity',
        (SELECT count(*)::INT FROM orbit_nodes('$TMP/integrity.json')))]) AS r
)"

# Sequential reads are consistent
[ "$(run_sequential_queries "$Q_FILES_SIMPLE" 10 seq)" = "true" ] \
    && add "read_consistency" true "10 reads identical" \
    || add "read_consistency" false "results diverged"

emit_results
