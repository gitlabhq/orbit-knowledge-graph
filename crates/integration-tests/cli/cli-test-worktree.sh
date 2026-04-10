#!/usr/bin/env bash
#
# Worktree and branch/commit tracking test.
# Usage: cli-test-worktree.sh <orbit-binary>

ORBIT="$1"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

REPO="$TMP/my-project"
WT_FEAT="$TMP/my-project-feature"
WT_FIX="$TMP/my-project-fix"
FEAT_BRANCH="feature/tests"
FIX_BRANCH="fix/utils"

# ── Setup ────────────────────────────────────────────────────────
init_test_repo "$REPO"
MAIN_SHA=$(cd "$REPO" && git rev-parse HEAD)
MAIN_BRANCH=$(cd "$REPO" && git symbolic-ref --short HEAD)

add_worktree "$REPO" "$FEAT_BRANCH" "$WT_FEAT"
cat > "$WT_FEAT/src/tests.py" << 'PY'
def test_hello():
    pass
PY
cd "$WT_FEAT" && git add -A && git commit -q -m "add tests"
FEAT_SHA=$(git rev-parse HEAD)

add_worktree "$REPO" "$FIX_BRANCH" "$WT_FIX" "$MAIN_SHA"
cat > "$WT_FIX/src/utils.py" << 'PY'
def patched():
    return True
PY
cd "$WT_FIX" && git add -A && git commit -q -m "patch utils"
FIX_SHA=$(git rev-parse HEAD)
cd "$TMP"

# ── Index & query ────────────────────────────────────────────────
index_repos "$REPO" "$WT_FEAT" "$WT_FIX"

orbit_query "$Q_FILES" "$TMP/files.json"
orbit_query "$Q_TRAVERSAL" "$TMP/trav.json"

# ── Batch assertions via single DuckDB call ──────────────────────
db "
-- Branches exist
INSERT INTO results SELECT 'branch_${MAIN_BRANCH}', c > 0, c || ' matches' FROM (SELECT count(*)::INT AS c FROM orbit_nodes('$TMP/files.json') WHERE n.branch = '${MAIN_BRANCH}');
INSERT INTO results SELECT 'branch_${FEAT_BRANCH}', c > 0, c || ' matches' FROM (SELECT count(*)::INT AS c FROM orbit_nodes('$TMP/files.json') WHERE n.branch = '${FEAT_BRANCH}');
INSERT INTO results SELECT 'branch_${FIX_BRANCH}', c > 0, c || ' matches' FROM (SELECT count(*)::INT AS c FROM orbit_nodes('$TMP/files.json') WHERE n.branch = '${FIX_BRANCH}');

-- Commits tracked
INSERT INTO results SELECT 'commit_${MAIN_SHA:0:8}', c > 0, c || ' matches' FROM (SELECT count(*)::INT AS c FROM orbit_nodes('$TMP/files.json') WHERE n.commit_sha = '${MAIN_SHA}');
INSERT INTO results SELECT 'commit_${FEAT_SHA:0:8}', c > 0, c || ' matches' FROM (SELECT count(*)::INT AS c FROM orbit_nodes('$TMP/files.json') WHERE n.commit_sha = '${FEAT_SHA}');
INSERT INTO results SELECT 'commit_${FIX_SHA:0:8}', c > 0, c || ' matches' FROM (SELECT count(*)::INT AS c FROM orbit_nodes('$TMP/files.json') WHERE n.commit_sha = '${FIX_SHA}');

-- Branch-specific file
INSERT INTO results SELECT 'branch_specific_file', c = 1, CASE WHEN c = 1 THEN 'tests.py on feature only' ELSE 'expected 1, got ' || c END FROM (SELECT count(*)::INT AS c FROM orbit_nodes('$TMP/files.json') WHERE n.name = 'tests.py');

-- Unique IDs per branch
INSERT INTO results SELECT 'unique_ids', c = 3, CASE WHEN c = 3 THEN '3 main.py across 3 branches' ELSE 'expected 3, got ' || c END FROM (SELECT count(*)::INT AS c FROM orbit_nodes('$TMP/files.json') WHERE n.name = 'main.py');

-- Content resolution
INSERT INTO results SELECT 'content_fix', c > 0, c || ' matches' FROM (SELECT count(*)::INT AS c FROM orbit_nodes('$TMP/files.json') WHERE n.name = 'utils.py' AND n.branch = '${FIX_BRANCH}' AND contains(n.content, 'patched'));
INSERT INTO results SELECT 'content_feat', c > 0, c || ' matches' FROM (SELECT count(*)::INT AS c FROM orbit_nodes('$TMP/files.json') WHERE n.name = 'tests.py' AND contains(n.content, 'test_hello'));

-- Traversal
INSERT INTO results SELECT 'traversal', c > 0, c || ' edges' FROM (SELECT count(*)::INT AS c FROM orbit_edges('$TMP/trav.json'));
"

emit_results
