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
F="$TMP/files.json"
db "
INSERT INTO results SELECT r.* FROM (
    SELECT check_has('branch_${MAIN_BRANCH}', (SELECT count(*)::INT FROM orbit_nodes('$F') WHERE n.branch = '${MAIN_BRANCH}')) AS r
    UNION ALL SELECT check_has('branch_${FEAT_BRANCH}', (SELECT count(*)::INT FROM orbit_nodes('$F') WHERE n.branch = '${FEAT_BRANCH}'))
    UNION ALL SELECT check_has('branch_${FIX_BRANCH}', (SELECT count(*)::INT FROM orbit_nodes('$F') WHERE n.branch = '${FIX_BRANCH}'))
    UNION ALL SELECT check_has('commit_${MAIN_SHA:0:8}', (SELECT count(*)::INT FROM orbit_nodes('$F') WHERE n.commit_sha = '${MAIN_SHA}'))
    UNION ALL SELECT check_has('commit_${FEAT_SHA:0:8}', (SELECT count(*)::INT FROM orbit_nodes('$F') WHERE n.commit_sha = '${FEAT_SHA}'))
    UNION ALL SELECT check_has('commit_${FIX_SHA:0:8}', (SELECT count(*)::INT FROM orbit_nodes('$F') WHERE n.commit_sha = '${FIX_SHA}'))
    UNION ALL SELECT check_count('branch_specific_file', (SELECT count(*)::INT FROM orbit_nodes('$F') WHERE n.name = 'tests.py'), 1, 'tests.py on feature only')
    UNION ALL SELECT check_count('unique_ids', (SELECT count(*)::INT FROM orbit_nodes('$F') WHERE n.name = 'main.py'), 3, '3 main.py across 3 branches')
    UNION ALL SELECT check_has('content_fix', (SELECT count(*)::INT FROM orbit_nodes('$F') WHERE n.name = 'utils.py' AND n.branch = '${FIX_BRANCH}' AND contains(n.content, 'patched')))
    UNION ALL SELECT check_has('content_feat', (SELECT count(*)::INT FROM orbit_nodes('$F') WHERE n.name = 'tests.py' AND contains(n.content, 'test_hello')))
    UNION ALL SELECT check_edges('traversal', (SELECT count(*)::INT FROM orbit_edges('$TMP/trav.json')))
);
"

emit_results
