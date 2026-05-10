#!/usr/bin/env bash
# analyze_commit.sh — Analyze a single security fix commit using orbit code indexer
#
# Usage: ./analyze_commit.sh <repo_path> <orbit_binary> <commit_hash> <output_dir>
#
# For each commit:
#   1. Extracts the diff (files changed, functions touched)
#   2. Creates a git worktree at that commit
#   3. Runs orbit index on the worktree
#   4. Queries DuckDB for callers of changed functions (blast radius)
#   5. Runs git blame on the parent to find when vulnerable code was introduced
#   6. Outputs structured JSON to <output_dir>/<commit_hash>.json
#
# The worktree is cleaned up after each run. The DuckDB graph at
# ~/.orbit/graph.duckdb is overwritten each time — callers should
# process results sequentially or copy the DB between runs.

set -euo pipefail

REPO="$1"
ORBIT="$2"
COMMIT="$3"
OUTDIR="$4"

GRAPH_DB="$HOME/.orbit/graph.duckdb"
WORKTREE_DIR="/tmp/orbit-security-audit-$$"
OUTFILE="$OUTDIR/${COMMIT}.json"

cleanup() {
  if [ -d "$WORKTREE_DIR" ]; then
    git -C "$REPO" worktree remove --force "$WORKTREE_DIR" 2>/dev/null || rm -rf "$WORKTREE_DIR"
  fi
}
trap cleanup EXIT

mkdir -p "$OUTDIR"

# ── 1. Extract commit metadata ──────────────────────────────────
commit_msg=$(git -C "$REPO" log -1 --format='%s' "$COMMIT")
commit_date=$(git -C "$REPO" log -1 --format='%aI' "$COMMIT")
commit_author=$(git -C "$REPO" log -1 --format='%an <%ae>' "$COMMIT")

# Get the actual fix commit (child of merge commit, or the commit itself)
parent_count=$(git -C "$REPO" rev-list --count "${COMMIT}^@" 2>/dev/null || echo "1")
if [ "$parent_count" -gt 1 ]; then
  # Merge commit — get the second parent (the branch being merged)
  fix_commit=$(git -C "$REPO" rev-parse "${COMMIT}^2" 2>/dev/null || echo "$COMMIT")
else
  fix_commit="$COMMIT"
fi

# ── 2. Extract changed files ────────────────────────────────────
changed_files=$(git -C "$REPO" diff --name-only "${COMMIT}~1..${COMMIT}" 2>/dev/null || echo "")
ruby_files=$(echo "$changed_files" | grep '\.rb$' || true)
spec_files=$(echo "$changed_files" | grep '_spec\.rb$' || true)
source_files=$(echo "$ruby_files" | grep -v '_spec\.rb$' || true)

file_count=$(echo "$changed_files" | grep -c '.' 2>/dev/null || echo "0")
file_count=$(echo "$file_count" | tr -d '[:space:]')
ruby_count=$(echo "$ruby_files" | grep -c '.' 2>/dev/null || echo "0")
ruby_count=$(echo "$ruby_count" | tr -d '[:space:]')
source_count=$(echo "$source_files" | grep -c '.' 2>/dev/null || echo "0")
source_count=$(echo "$source_count" | tr -d '[:space:]')

# ── 3. Extract diff for classification ──────────────────────────
diff_stat=$(git -C "$REPO" diff --stat "${COMMIT}~1..${COMMIT}" 2>/dev/null | tail -1)
diff_text=$(git -C "$REPO" diff "${COMMIT}~1..${COMMIT}" -- $source_files 2>/dev/null | head -500)

# ── 4. Create worktree and index ─────────────────────────────────
rm -f "$GRAPH_DB"
git -C "$REPO" worktree add --detach "$WORKTREE_DIR" "$COMMIT" 2>/dev/null

index_output=$("$ORBIT" index "$WORKTREE_DIR" --stats 2>/dev/null || echo '{"error": "index failed"}')
index_time=$(echo "$index_output" | python3 -c "import sys,json; print(json.load(sys.stdin).get('time_seconds', -1))" 2>/dev/null || echo "-1")

# ── 5. Query blast radius for changed source files ───────────────
callers_json="[]"
definitions_json="[]"
extends_json="[]"

if [ -n "$source_files" ] && [ -f "$GRAPH_DB" ]; then
  # Build SQL IN clause for changed files
  file_list=$(echo "$source_files" | while read -r f; do
    [ -n "$f" ] && printf "'%s'," "$f"
  done | sed 's/,$//')

  if [ -n "$file_list" ]; then
    # Get definitions in changed files
    definitions_json=$(duckdb "$GRAPH_DB" -json -c "
      SELECT fqn, file_path, definition_type, name, start_line, end_line
      FROM gl_definition
      WHERE file_path IN ($file_list)
      ORDER BY file_path, start_line" 2>/dev/null || echo "[]")

    # Get callers from OTHER files that call into changed files
    callers_json=$(duckdb "$GRAPH_DB" -json -c "
      SELECT DISTINCT
        s.fqn as caller_fqn,
        s.file_path as caller_file,
        s.definition_type as caller_type,
        t.fqn as callee_fqn,
        t.file_path as callee_file,
        e.relationship_kind
      FROM gl_edge e
      JOIN gl_definition s ON e.source_id = s.id AND e.source_kind = 'Definition'
      JOIN gl_definition t ON e.target_id = t.id AND e.target_kind = 'Definition'
      WHERE t.file_path IN ($file_list)
        AND s.file_path NOT IN ($file_list)
        AND e.relationship_kind = 'CALLS'
      ORDER BY s.file_path" 2>/dev/null || echo "[]")

    # Get inheritance/mixin edges involving changed files
    extends_json=$(duckdb "$GRAPH_DB" -json -c "
      SELECT DISTINCT
        s.fqn as child_fqn,
        s.file_path as child_file,
        t.fqn as parent_fqn,
        t.file_path as parent_file,
        e.relationship_kind
      FROM gl_edge e
      JOIN gl_definition s ON e.source_id = s.id AND e.source_kind = 'Definition'
      JOIN gl_definition t ON e.target_id = t.id AND e.target_kind = 'Definition'
      WHERE (t.file_path IN ($file_list) OR s.file_path IN ($file_list))
        AND e.relationship_kind IN ('EXTENDS', 'INCLUDES')
      ORDER BY s.file_path" 2>/dev/null || echo "[]")
  fi
fi

# ── 6. Git blame on parent to find vulnerability introduction ────
blame_json="[]"
if [ -n "$source_files" ]; then
  blame_entries=""
  while IFS= read -r src_file; do
    [ -z "$src_file" ] && continue
    # Get changed line ranges from diff
    changed_lines=$(git -C "$REPO" diff "${COMMIT}~1..${COMMIT}" -- "$src_file" 2>/dev/null \
      | grep '^@@' | sed 's/^@@ -\([0-9]*\),*\([0-9]*\).*/\1,\2/' || true)

    # Blame the parent for those regions
    while IFS=',' read -r start count; do
      [ -z "$start" ] && continue
      end=$((start + ${count:-1}))
      blame_out=$(git -C "$REPO" blame -l --date=short "${COMMIT}~1" -L "${start},${end}" -- "$src_file" 2>/dev/null | head -5 || true)
      if [ -n "$blame_out" ]; then
        intro_commit=$(echo "$blame_out" | head -1 | awk '{print $1}' | tr -d '^')
        intro_date=$(git -C "$REPO" log -1 --format='%aI' "$intro_commit" 2>/dev/null || echo "unknown")
        intro_msg=$(git -C "$REPO" log -1 --format='%s' "$intro_commit" 2>/dev/null || echo "unknown")
        blame_entries="${blame_entries}{\"file\":\"$src_file\",\"intro_commit\":\"$intro_commit\",\"intro_date\":\"$intro_date\",\"intro_msg\":$(echo "$intro_msg" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read().strip()))' 2>/dev/null || echo '""')},"
      fi
    done <<< "$changed_lines"
  done <<< "$source_files"

  if [ -n "$blame_entries" ]; then
    blame_json="[${blame_entries%,}]"
  fi
fi

# ── 7. Assemble output via temp files (avoids quoting issues) ────
TMPDIR_ASM=$(mktemp -d)
echo "$callers_json" > "$TMPDIR_ASM/callers.json"
echo "$definitions_json" > "$TMPDIR_ASM/definitions.json"
echo "$extends_json" > "$TMPDIR_ASM/extends.json"
echo "$blame_json" > "$TMPDIR_ASM/blame.json"
echo "$commit_msg" > "$TMPDIR_ASM/message.txt"
echo "$commit_author" > "$TMPDIR_ASM/author.txt"
printf '%s\n' $source_files > "$TMPDIR_ASM/changed.txt"

python3 - "$TMPDIR_ASM" "$COMMIT" "$fix_commit" "$commit_date" "$diff_stat" \
  "$file_count" "$ruby_count" "$source_count" "$index_time" "$OUTFILE" <<'PYEOF'
import json, sys, os

tmpdir = sys.argv[1]

def load_json(name):
    try:
        return json.load(open(os.path.join(tmpdir, name)))
    except Exception:
        return []

def load_text(name):
    try:
        return open(os.path.join(tmpdir, name)).read().strip()
    except Exception:
        return ""

callers = load_json("callers.json")
definitions = load_json("definitions.json")
extends = load_json("extends.json")
blame = load_json("blame.json")
message = load_text("message.txt")
author = load_text("author.txt")
changed = [l for l in load_text("changed.txt").split('\n') if l]

caller_files = len(set(c.get('caller_file', '') for c in callers))

result = {
    "commit": sys.argv[2],
    "fix_commit": sys.argv[3],
    "date": sys.argv[4],
    "author": author,
    "message": message,
    "diff_stat": sys.argv[5],
    "files": {
        "total": int(sys.argv[6]),
        "ruby": int(sys.argv[7]),
        "source": int(sys.argv[8]),
        "changed": changed,
    },
    "blast_radius": {
        "caller_edges": len(callers),
        "caller_files": caller_files,
        "definitions_in_changed_files": len(definitions),
    },
    "index_time_seconds": float(sys.argv[9]),
    "callers": callers,
    "definitions": definitions,
    "extends": extends,
    "blame": blame,
}
json.dump(result, open(sys.argv[10], 'w'), indent=2)
print(sys.argv[10])
PYEOF

rm -rf "$TMPDIR_ASM"
