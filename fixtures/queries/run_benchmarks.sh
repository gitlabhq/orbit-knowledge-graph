#!/usr/bin/env bash
# Benchmark suite for query engine optimizations.
# Runs A/B comparisons against a local ClickHouse with synthetic data loaded.
#
# Usage:
#   ./fixtures/queries/run_benchmarks.sh
#
# Prerequisites:
#   - ClickHouse running on localhost:9000 (password: password)
#   - Synthetic data loaded (via xtask synth load)
#   - orbit CLI built (cargo build -p orbit)

set -euo pipefail

CH_HOST="${CH_HOST:-127.0.0.1}"
CH_PORT="${CH_PORT:-9000}"
CH_PASS="${CH_PASS:-password}"

ch() {
  clickhouse client --host "$CH_HOST" --port "$CH_PORT" --password "$CH_PASS" "$@"
}

echo "=== Checking ClickHouse ==="
EDGE_COUNT=$(ch -q "SELECT count() FROM gl_edge")
echo "Edge count: $EDGE_COUNT"

echo ""
echo "=== Sampling traversal path ==="
# Pick a real deep traversal path (3+ segments) with the most edges
TPATH=$(ch -q "SELECT traversal_path FROM gl_edge WHERE length(splitByChar('/', traversal_path)) >= 5 GROUP BY traversal_path ORDER BY count() DESC LIMIT 1")
echo "Traversal path: $TPATH"
TPATH_COVERAGE=$(ch -q "SELECT concat(toString(round(countIf(startsWith(traversal_path, '$TPATH')) / count() * 100, 2)), '%') FROM gl_edge")
echo "Coverage: $TPATH_COVERAGE of edges"

echo ""
echo "=== Sampling IDs (under $TPATH) ==="
USERS=$(ch -q "SELECT arrayStringConcat(arraySlice(groupArray(toString(source_id)), 1, 3), ', ') FROM gl_edge WHERE startsWith(traversal_path, '$TPATH') AND source_kind = 'User'")
MRS_OPEN=$(ch -q "SELECT arrayStringConcat(arraySlice(groupArray(toString(id)), 1, 3), ', ') FROM gl_merge_request WHERE state = 'opened' AND startsWith(traversal_path, '$TPATH')")
echo "Users: $USERS"
echo "MRs (opened): $MRS_OPEN"

echo ""
echo "=== Compiling DSL queries via orbit CLI ==="
DSL_JSON=$(cargo run -p orbit -- query fixtures/queries/optimization_showcase.json -t "$TPATH" --format json 2>/dev/null)
DSL_COUNT=$(echo "$DSL_JSON" | python3 -c "import json,sys; r=json.load(sys.stdin); print(f'{sum(1 for x in r if \"error\" not in x)} ok, {sum(1 for x in r if \"error\" in x)} errors')")
echo "DSL queries: $DSL_COUNT"

echo ""
echo "=== Truncating query_log ==="
ch -q "TRUNCATE TABLE system.query_log"

# -----------------------------------------------------------------------
# 1. Cascading SIP
# -----------------------------------------------------------------------
echo ""
echo "--- 1a. merge + SIP ---"
ch -q "
SELECT * FROM (
WITH _root_ids AS (SELECT u.id AS id FROM gl_user AS u WHERE u.id IN [$USERS]),
_cascade_mr AS (SELECT _ce.target_id AS id FROM gl_edge AS _ce WHERE startsWith(_ce.traversal_path, '$TPATH') AND _ce.source_id IN (SELECT id FROM _root_ids) AND _ce.relationship_kind = 'AUTHORED'),
_cascade_p AS (SELECT _ce.target_id AS id FROM gl_edge AS _ce WHERE startsWith(_ce.traversal_path, '$TPATH') AND _ce.source_id IN (SELECT id FROM _cascade_mr) AND _ce.relationship_kind = 'IN_PROJECT')
SELECT u.id, mr.id, p.id FROM gl_user AS u
INNER JOIN gl_edge AS e0 ON u.id = e0.source_id AND e0.relationship_kind = 'AUTHORED'
INNER JOIN gl_merge_request AS mr ON e0.target_id = mr.id
INNER JOIN gl_edge AS e1 ON mr.id = e1.source_id AND e1.relationship_kind = 'IN_PROJECT'
INNER JOIN gl_project AS p ON e1.target_id = p.id
WHERE startsWith(e0.traversal_path, '$TPATH') AND startsWith(mr.traversal_path, '$TPATH') AND startsWith(e1.traversal_path, '$TPATH') AND startsWith(p.traversal_path, '$TPATH')
  AND u.id IN [$USERS] AND mr.state = 'opened' AND p.archived = false
  AND e0.source_id IN (SELECT id FROM _root_ids) AND e1.source_id IN (SELECT id FROM _cascade_mr)
  AND p.id IN (SELECT id FROM _cascade_p) AND mr.id IN (SELECT id FROM _cascade_mr)
LIMIT 50) FORMAT Null SETTINGS log_comment='merge_sip', join_algorithm='full_sorting_merge'"

echo "--- 1b. merge, no SIP ---"
ch -q "
SELECT * FROM (
SELECT u.id, mr.id, p.id FROM gl_user AS u
INNER JOIN gl_edge AS e0 ON u.id = e0.source_id AND e0.relationship_kind = 'AUTHORED'
INNER JOIN gl_merge_request AS mr ON e0.target_id = mr.id
INNER JOIN gl_edge AS e1 ON mr.id = e1.source_id AND e1.relationship_kind = 'IN_PROJECT'
INNER JOIN gl_project AS p ON e1.target_id = p.id
WHERE startsWith(e0.traversal_path, '$TPATH') AND startsWith(mr.traversal_path, '$TPATH') AND startsWith(e1.traversal_path, '$TPATH') AND startsWith(p.traversal_path, '$TPATH')
  AND u.id IN [$USERS] AND mr.state = 'opened' AND p.archived = false
LIMIT 50) FORMAT Null SETTINGS log_comment='merge_nosip', join_algorithm='full_sorting_merge'"

echo "--- 1c. hash, no SIP ---"
ch -q "
SELECT * FROM (
SELECT u.id, mr.id, p.id FROM gl_user AS u
INNER JOIN gl_edge AS e0 ON u.id = e0.source_id AND e0.relationship_kind = 'AUTHORED'
INNER JOIN gl_merge_request AS mr ON e0.target_id = mr.id
INNER JOIN gl_edge AS e1 ON mr.id = e1.source_id AND e1.relationship_kind = 'IN_PROJECT'
INNER JOIN gl_project AS p ON e1.target_id = p.id
WHERE startsWith(e0.traversal_path, '$TPATH') AND startsWith(mr.traversal_path, '$TPATH') AND startsWith(e1.traversal_path, '$TPATH') AND startsWith(p.traversal_path, '$TPATH')
  AND u.id IN [$USERS] AND mr.state = 'opened' AND p.archived = false
LIMIT 50) FORMAT Null SETTINGS log_comment='hash_nosip', join_algorithm='hash'"

# -----------------------------------------------------------------------
# 2. Neighbors UNION ALL vs OR
# -----------------------------------------------------------------------
echo ""
echo "--- 2a. neighbors UNION ALL ---"
ch -q "
SELECT * FROM (
SELECT e.target_id, e.target_kind, e.relationship_kind, 1 AS out, mr.id FROM gl_merge_request AS mr INNER JOIN gl_edge AS e ON mr.id = e.source_id AND e.source_kind = 'MergeRequest' WHERE startsWith(mr.traversal_path, '$TPATH') AND startsWith(e.traversal_path, '$TPATH') AND mr.id IN [$MRS_OPEN]
UNION ALL
SELECT e.source_id, e.source_kind, e.relationship_kind, 0 AS out, mr.id FROM gl_merge_request AS mr INNER JOIN gl_edge AS e ON mr.id = e.target_id AND e.target_kind = 'MergeRequest' WHERE startsWith(mr.traversal_path, '$TPATH') AND startsWith(e.traversal_path, '$TPATH') AND mr.id IN [$MRS_OPEN]
LIMIT 100) FORMAT Null SETTINGS log_comment='nbr_union', join_algorithm='full_sorting_merge'"

echo "--- 2b. neighbors OR (may OOM) ---"
ch -q "
SELECT * FROM (
SELECT CASE WHEN mr.id = e.source_id THEN e.target_id ELSE e.source_id END,
       CASE WHEN mr.id = e.source_id THEN e.target_kind ELSE e.source_kind END,
       e.relationship_kind,
       CASE WHEN mr.id = e.source_id THEN 1 ELSE 0 END, mr.id
FROM gl_merge_request AS mr
INNER JOIN gl_edge AS e ON (mr.id = e.source_id AND e.source_kind = 'MergeRequest') OR (mr.id = e.target_id AND e.target_kind = 'MergeRequest')
WHERE startsWith(mr.traversal_path, '$TPATH') AND startsWith(e.traversal_path, '$TPATH') AND mr.id IN [$MRS_OPEN]
LIMIT 100) FORMAT Null SETTINGS log_comment='nbr_or', join_algorithm='hash', max_memory_usage=20000000000" 2>&1 || echo "  (OOM or failed as expected)"

# -----------------------------------------------------------------------
# 3. countIf vs WHERE+count
# -----------------------------------------------------------------------
echo ""
echo "--- 3a. countIf ---"
ch -q "
SELECT * FROM (
SELECT p.name, countIf(mr.id, mr.state = 'merged' AND mr.draft = false) AS cnt, p.id
FROM gl_merge_request AS mr
INNER JOIN gl_edge AS e0 ON mr.id = e0.source_id AND e0.relationship_kind = 'IN_PROJECT'
INNER JOIN gl_project AS p ON e0.target_id = p.id
WHERE startsWith(mr.traversal_path, '$TPATH') AND startsWith(e0.traversal_path, '$TPATH') AND startsWith(p.traversal_path, '$TPATH')
GROUP BY p.name, p.id ORDER BY cnt DESC LIMIT 20
) FORMAT Null SETTINGS log_comment='agg_countif', join_algorithm='full_sorting_merge'"

echo "--- 3b. WHERE + count ---"
ch -q "
SELECT * FROM (
SELECT p.name, count(mr.id) AS cnt, p.id
FROM gl_merge_request AS mr
INNER JOIN gl_edge AS e0 ON mr.id = e0.source_id AND e0.relationship_kind = 'IN_PROJECT'
INNER JOIN gl_project AS p ON e0.target_id = p.id
WHERE startsWith(mr.traversal_path, '$TPATH') AND startsWith(e0.traversal_path, '$TPATH') AND startsWith(p.traversal_path, '$TPATH')
  AND mr.state = 'merged' AND mr.draft = false
GROUP BY p.name, p.id ORDER BY cnt DESC LIMIT 20
) FORMAT Null SETTINGS log_comment='agg_where', join_algorithm='full_sorting_merge'"

# -----------------------------------------------------------------------
# 4. Compiler-generated DSL queries
# -----------------------------------------------------------------------
echo ""
echo "--- 4. DSL queries (via orbit compile) ---"
echo "$DSL_JSON" | python3 -c "
import json, sys, subprocess
results = json.load(sys.stdin)
for r in results:
    if 'error' in r:
        continue
    sql = r['rendered_sql']
    label = r['label']
    print(f'  running dsl_{label}...')
    subprocess.run([
        'clickhouse', 'client',
        '--host', '$CH_HOST', '--port', '$CH_PORT', '--password', '$CH_PASS',
        '-q', f\"SELECT * FROM ({sql}) FORMAT Null SETTINGS log_comment='dsl_{label}', join_algorithm='full_sorting_merge'\"
    ], capture_output=True)
"

# -----------------------------------------------------------------------
# Collect results
# -----------------------------------------------------------------------
echo ""
echo "=== Flushing query log ==="
sleep 2
ch -q "SYSTEM FLUSH LOGS"
sleep 1

echo ""
echo "=== RESULTS ==="
echo ""
ch -q "
SELECT
    log_comment AS query,
    query_duration_ms AS ms,
    read_rows,
    formatReadableSize(read_bytes) AS read_size,
    result_rows AS results,
    ProfileEvents['SelectedMarks'] AS marks,
    formatReadableSize(memory_usage) AS peak_mem
FROM system.query_log
WHERE type = 'QueryFinish'
  AND log_comment != ''
  AND query NOT LIKE '%system.query_log%'
  AND query NOT LIKE '%SYSTEM%'
  AND query NOT LIKE '%TRUNCATE%'
ORDER BY log_comment
FORMAT PrettyCompactMonoBlock
"

echo ""
echo "=== EXPLAIN: neighbors UNION ALL (projection usage) ==="
ch -q "
EXPLAIN indexes=1, projections=1
SELECT e.target_id, e.target_kind, e.relationship_kind, 1 AS out, mr.id FROM gl_merge_request AS mr INNER JOIN gl_edge AS e ON mr.id = e.source_id AND e.source_kind = 'MergeRequest' WHERE startsWith(mr.traversal_path, '$TPATH') AND startsWith(e.traversal_path, '$TPATH') AND mr.id IN [$MRS_OPEN]
UNION ALL
SELECT e.source_id, e.source_kind, e.relationship_kind, 0 AS out, mr.id FROM gl_merge_request AS mr INNER JOIN gl_edge AS e ON mr.id = e.target_id AND e.target_kind = 'MergeRequest' WHERE startsWith(mr.traversal_path, '$TPATH') AND startsWith(e.traversal_path, '$TPATH') AND mr.id IN [$MRS_OPEN]
LIMIT 100
SETTINGS use_query_condition_cache=0, use_skip_indexes_on_data_read=0
" 2>&1 | grep -E 'ReadFrom|Condition:|Parts:|Granules:|Projection|Description:'

echo ""
echo "Done."
