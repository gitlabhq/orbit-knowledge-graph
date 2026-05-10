#!/usr/bin/env bash
# phase2_rolling.sh — Run per-category blast radius queries against quarterly snapshots
#
# Reuses snapshot commits from rolling_window.sh. For each snapshot:
#   1. Wipe DuckDB, create worktree, orbit index
#   2. For each security category (from Orbit Phase 1 corpus):
#      - Count definitions in category files
#      - Count callers from OUTSIDE category files (blast radius)
#      - Count unique caller files
#      - Count EXTENDS/INCLUDES edges involving category files
#   3. Save all metrics to JSONL

set -euo pipefail

REPO="$1"
ORBIT="$2"
OUTDIR="$3"

GRAPH_DB="$HOME/.orbit/graph.duckdb"
WORKTREE_DIR="/tmp/orbit-phase2-$$"
METRICS="$OUTDIR/phase2_metrics.jsonl"
CAT_QUERIES="/tmp/cat_queries.json"

mkdir -p "$OUTDIR"
> "$METRICS"

cleanup() {
  if [ -d "$WORKTREE_DIR" ]; then
    git -C "$REPO" worktree remove --force "$WORKTREE_DIR" 2>/dev/null || rm -rf "$WORKTREE_DIR"
  fi
}
trap cleanup EXIT

# Categories to query (from Phase 1 Orbit corpus)
CATEGORIES="auth authz xss dos ci secrets upload import graphql api package serialization network service config"

# Build snapshot list
SNAPSHOTS=""
for year in 2020 2021 2022 2023 2024 2025; do
  for month in 01 04 07 10; do
    quarter="Q$(( (10#$month - 1) / 3 + 1 ))"
    label="${year}-${quarter}"
    commit=$(git -C "$REPO" log --all --before="${year}-${month}-01" --format='%H' -1 2>/dev/null || echo "")
    [ -n "$commit" ] && SNAPSHOTS="${SNAPSHOTS}${label}|${commit}\n"
  done
done

total=$(echo -e "$SNAPSHOTS" | grep -c '|' || echo 0)
echo "Phase 2: $total snapshots × $(echo $CATEGORIES | wc -w | tr -d ' ') categories"

i=0
echo -e "$SNAPSHOTS" | while IFS='|' read -r label commit; do
  [ -z "$commit" ] && continue
  i=$((i + 1))
  echo -n "[$i/$total] $label ... "

  rm -f "$GRAPH_DB"
  cleanup 2>/dev/null || true
  git -C "$REPO" worktree add --detach "$WORKTREE_DIR" "$commit" 2>/dev/null

  "$ORBIT" index "$WORKTREE_DIR" 2>/dev/null
  
  # Run per-category queries using Python for clean JSON handling
  python3 - "$GRAPH_DB" "$label" "$commit" "$METRICS" "$CAT_QUERIES" << 'PYEOF'
import json, sys, subprocess

db = sys.argv[1]
label = sys.argv[2]
commit = sys.argv[3]
metrics_file = sys.argv[4]
cat_queries = json.load(open(sys.argv[5]))

def duckdb_val(sql):
    """Run a DuckDB query, return the first value."""
    try:
        r = subprocess.run(['duckdb', db, '-json', '-c', sql],
                          capture_output=True, text=True, timeout=30)
        if r.stdout.strip():
            rows = json.loads(r.stdout)
            if rows:
                return list(rows[0].values())[0]
    except:
        pass
    return 0

row = {'snapshot': label, 'commit': commit}

# Global metrics
row['ruby_defs'] = duckdb_val("SELECT COUNT(*) FROM gl_definition WHERE file_path LIKE '%.rb'")
row['total_calls'] = duckdb_val("SELECT COUNT(*) FROM gl_edge WHERE relationship_kind = 'CALLS'")
row['total_extends'] = duckdb_val("SELECT COUNT(*) FROM gl_edge WHERE relationship_kind = 'EXTENDS'")

for cat, cq in cat_queries.items():
    files = cq['files']
    if not files:
        continue
    
    file_in = "','".join(files)
    dir_likes = cq['dir_likes']
    
    # Definitions in category files (exact match)
    defs = duckdb_val(f"""
        SELECT COUNT(*) FROM gl_definition 
        WHERE file_path IN ('{file_in}')
    """)
    
    # Definitions in category directories (broader surface)
    dir_defs = duckdb_val(f"""
        SELECT COUNT(*) FROM gl_definition 
        WHERE {dir_likes}
    """) if dir_likes else 0
    
    # Callers from OUTSIDE these files into these files (blast radius)
    callers = duckdb_val(f"""
        SELECT COUNT(DISTINCT s.fqn)
        FROM gl_edge e
        JOIN gl_definition s ON e.source_id = s.id AND e.source_kind = 'Definition'
        JOIN gl_definition t ON e.target_id = t.id AND e.target_kind = 'Definition'
        WHERE t.file_path IN ('{file_in}')
          AND s.file_path NOT IN ('{file_in}')
          AND e.relationship_kind = 'CALLS'
    """)
    
    # Unique caller files
    caller_files = duckdb_val(f"""
        SELECT COUNT(DISTINCT s.file_path)
        FROM gl_edge e
        JOIN gl_definition s ON e.source_id = s.id AND e.source_kind = 'Definition'
        JOIN gl_definition t ON e.target_id = t.id AND e.target_kind = 'Definition'
        WHERE t.file_path IN ('{file_in}')
          AND s.file_path NOT IN ('{file_in}')
          AND e.relationship_kind = 'CALLS'
    """)
    
    # EXTENDS/INCLUDES involving these files
    extends = duckdb_val(f"""
        SELECT COUNT(*)
        FROM gl_edge e
        JOIN gl_definition s ON e.source_id = s.id AND e.source_kind = 'Definition'
        JOIN gl_definition t ON e.target_id = t.id AND e.target_kind = 'Definition'
        WHERE (t.file_path IN ('{file_in}') OR s.file_path IN ('{file_in}'))
          AND e.relationship_kind IN ('EXTENDS', 'INCLUDES')
    """)
    
    row[f'{cat}_defs'] = defs
    row[f'{cat}_dir_defs'] = dir_defs
    row[f'{cat}_callers'] = callers
    row[f'{cat}_caller_files'] = caller_files
    row[f'{cat}_extends'] = extends

with open(metrics_file, 'a') as f:
    f.write(json.dumps(row) + '\n')

print(f"ok ({row['ruby_defs']} defs)", file=sys.stderr)
PYEOF

  git -C "$REPO" worktree remove --force "$WORKTREE_DIR" 2>/dev/null || true
  echo ""
done

echo "Done: $METRICS"
