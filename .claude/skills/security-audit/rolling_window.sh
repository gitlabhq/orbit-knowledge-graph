#!/usr/bin/env bash
# rolling_window.sh — Index quarterly snapshots and run pattern queries
#
# Usage: ./rolling_window.sh <repo_path> <orbit_binary> <output_dir>
#
# Indexes the repo at each quarterly boundary (Q1-Q4, 2020-2025),
# runs a set of security pattern queries, and saves metrics per snapshot.
# Total: ~24 indexes × 16s = ~6 minutes.

set -euo pipefail

REPO="$1"
ORBIT="$2"
OUTDIR="$3"

GRAPH_DB="$HOME/.orbit/graph.duckdb"
WORKTREE_DIR="/tmp/orbit-rolling-$$"
METRICS_FILE="$OUTDIR/metrics.jsonl"

mkdir -p "$OUTDIR"
> "$METRICS_FILE"

cleanup() {
  if [ -d "$WORKTREE_DIR" ]; then
    git -C "$REPO" worktree remove --force "$WORKTREE_DIR" 2>/dev/null || rm -rf "$WORKTREE_DIR"
  fi
}
trap cleanup EXIT

# ── Find quarterly boundary commits ─────────────────────────────
echo "Finding quarterly snapshots..."
SNAPSHOTS=""
for year in 2020 2021 2022 2023 2024 2025; do
  for month in 01 04 07 10; do
    quarter="Q$(( (10#$month - 1) / 3 + 1 ))"
    label="${year}-${quarter}"
    commit=$(git -C "$REPO" log --all --before="${year}-${month}-01" --format='%H' -1 2>/dev/null || echo "")
    if [ -n "$commit" ]; then
      SNAPSHOTS="${SNAPSHOTS}${label}|${commit}\n"
    fi
  done
done

total=$(echo -e "$SNAPSHOTS" | grep -c '|' || echo 0)
echo "Found $total quarterly snapshots"
echo ""

# ── Process each snapshot ────────────────────────────────────────
i=0
echo -e "$SNAPSHOTS" | while IFS='|' read -r label commit; do
  [ -z "$commit" ] && continue
  i=$((i + 1))
  
  echo -n "[$i/$total] $label (${commit:0:7}) ... "

  # Clean slate
  rm -f "$GRAPH_DB"
  cleanup 2>/dev/null || true

  # Create worktree and index
  git -C "$REPO" worktree add --detach "$WORKTREE_DIR" "$commit" 2>/dev/null
  index_json=$("$ORBIT" index "$WORKTREE_DIR" --stats 2>/dev/null || echo '{}')
  index_time=$(echo "$index_json" | python3 -c "import sys,json; print(json.load(sys.stdin).get('time_seconds',-1))" 2>/dev/null || echo "-1")

  # Run pattern queries
  metrics=$(duckdb "$GRAPH_DB" -json -c "
    SELECT
      -- Codebase size
      (SELECT COUNT(*) FROM gl_definition WHERE file_path LIKE '%.rb') as ruby_defs,
      (SELECT COUNT(*) FROM gl_edge WHERE relationship_kind = 'CALLS') as total_calls,
      (SELECT COUNT(*) FROM gl_edge WHERE relationship_kind = 'EXTENDS') as total_extends,
      (SELECT COUNT(*) FROM gl_file) as total_files,

      -- Attack surface sizes
      (SELECT COUNT(*) FROM gl_definition WHERE file_path LIKE 'app/controllers/%' AND definition_type = 'Method') as controller_methods,
      (SELECT COUNT(*) FROM gl_definition WHERE file_path LIKE 'app/graphql/%' AND definition_type = 'Class') as graphql_classes,
      (SELECT COUNT(*) FROM gl_definition WHERE file_path LIKE 'app/graphql/mutations/%' AND definition_type = 'Class') as graphql_mutations,
      (SELECT COUNT(*) FROM gl_definition WHERE file_path LIKE 'lib/api/%' AND definition_type IN ('Class', 'Module')) as api_classes,
      (SELECT COUNT(*) FROM gl_definition WHERE file_path LIKE 'app/services/%' AND definition_type IN ('Class', 'Module')) as service_classes,
      (SELECT COUNT(*) FROM gl_definition WHERE file_path LIKE 'app/finders/%' AND definition_type = 'Class') as finder_classes,
      (SELECT COUNT(*) FROM gl_definition WHERE file_path LIKE 'app/workers/%' AND definition_type = 'Class') as worker_classes,
      (SELECT COUNT(*) FROM gl_definition WHERE (file_path LIKE '%serializer%' OR file_path LIKE 'lib/api/entities/%') AND definition_type IN ('Class', 'Module')) as serializer_classes,

      -- AUTHZ: controller → service calls without authorize
      (SELECT COUNT(DISTINCT s.fqn) FROM gl_definition s
       JOIN gl_edge e ON e.source_id = s.id AND e.source_kind = 'Definition'
       JOIN gl_definition t ON e.target_id = t.id AND e.target_kind = 'Definition'
       WHERE s.file_path LIKE 'app/controllers/%' AND s.definition_type = 'Method'
         AND t.file_path LIKE 'app/services/%' AND e.relationship_kind = 'CALLS') as ctrl_to_svc_callers,

      -- AUTHZ: authorize_*! method definitions (more = more enforcement)
      (SELECT COUNT(*) FROM gl_definition WHERE fqn LIKE '%authorize_%!' AND file_path LIKE 'app/%') as authorize_bang_methods,

      -- Mixin complexity: concern includes in controllers
      (SELECT COUNT(*) FROM gl_edge e
       JOIN gl_definition s ON e.source_id = s.id AND e.source_kind = 'Definition'
       JOIN gl_definition t ON e.target_id = t.id AND e.target_kind = 'Definition'
       WHERE e.relationship_kind = 'EXTENDS'
         AND s.file_path LIKE 'app/controllers/%'
         AND t.file_path LIKE 'app/controllers/concerns/%') as controller_concern_includes,

      -- DOS: Gitlab::Json usage
      (SELECT COUNT(DISTINCT s.fqn) FROM gl_edge e
       JOIN gl_definition s ON e.source_id = s.id AND e.source_kind = 'Definition'
       JOIN gl_definition t ON e.target_id = t.id AND e.target_kind = 'Definition'
       WHERE e.relationship_kind = 'CALLS' AND t.fqn = 'Gitlab::Json::parse'
         AND s.file_path NOT LIKE '%spec%') as json_parse_callers,
      (SELECT COUNT(DISTINCT s.fqn) FROM gl_edge e
       JOIN gl_definition s ON e.source_id = s.id AND e.source_kind = 'Definition'
       JOIN gl_definition t ON e.target_id = t.id AND e.target_kind = 'Definition'
       WHERE e.relationship_kind = 'CALLS' AND t.fqn = 'Gitlab::Json::safe_parse'
         AND s.file_path NOT LIKE '%spec%') as json_safe_parse_callers,

      -- INJECTION: Popen callers
      (SELECT COUNT(DISTINCT s.fqn) FROM gl_edge e
       JOIN gl_definition s ON e.source_id = s.id AND e.source_kind = 'Definition'
       JOIN gl_definition t ON e.target_id = t.id AND e.target_kind = 'Definition'
       WHERE e.relationship_kind = 'CALLS' AND t.fqn LIKE 'Gitlab::Popen%'
         AND s.file_path NOT LIKE '%spec%') as popen_callers,

      -- AUTH: auth concern complexity
      (SELECT COUNT(*) FROM gl_definition
       WHERE file_path LIKE 'app/controllers/concerns/authenticates%'
         AND definition_type = 'Method') as auth_concern_methods,

      -- Cross-file call density (coupling metric)
      (SELECT COUNT(*) FROM gl_edge e
       JOIN gl_definition s ON e.source_id = s.id AND e.source_kind = 'Definition'
       JOIN gl_definition t ON e.target_id = t.id AND e.target_kind = 'Definition'
       WHERE e.relationship_kind = 'CALLS' AND s.file_path != t.file_path
         AND s.file_path LIKE '%.rb' AND t.file_path LIKE '%.rb') as cross_file_calls
  " 2>/dev/null || echo '[{}]')

  # Clean up worktree
  git -C "$REPO" worktree remove --force "$WORKTREE_DIR" 2>/dev/null || true

  # Write metrics line
  echo "$metrics" | python3 -c "
import json, sys
data = json.load(sys.stdin)
row = data[0] if data else {}
row['snapshot'] = '$label'
row['commit'] = '$commit'
row['index_time'] = $index_time
print(json.dumps(row))
" >> "$METRICS_FILE"

  echo "${index_time}s"
done

echo ""
echo "Done. $total snapshots indexed."
