#!/usr/bin/env bash
# Worktree and branch/commit tracking test.
ORBIT="$1"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

REPO="$TMP/my-project" WT_FEAT="$TMP/wt-feat" WT_FIX="$TMP/wt-fix"
FEAT_BRANCH="feature/tests" FIX_BRANCH="fix/utils"

# Setup
init_test_repo "$REPO"
MAIN_SHA=$(cd "$REPO" && git rev-parse HEAD)
MAIN_BRANCH=$(cd "$REPO" && git symbolic-ref --short HEAD)

add_worktree "$REPO" "$FEAT_BRANCH" "$WT_FEAT"
echo 'def test_hello(): pass' > "$WT_FEAT/src/tests.py"
cd "$WT_FEAT" && git add -A && git commit -q -m "add tests"
FEAT_SHA=$(git rev-parse HEAD)

add_worktree "$REPO" "$FIX_BRANCH" "$WT_FIX" "$MAIN_SHA"
echo 'def patched(): return True' > "$WT_FIX/src/utils.py"
cd "$WT_FIX" && git add -A && git commit -q -m "patch utils"
FIX_SHA=$(git rev-parse HEAD)
cd "$TMP"

# Index & query
index_repos "$REPO" "$WT_FEAT" "$WT_FIX"
orbit_query "$Q_FILES" "$TMP/f.json"
orbit_query "$Q_TRAVERSAL" "$TMP/t.json"

# Assert -- generate expectations SQL, run in one shot
cat > "$TMP/expect.sql" << SQL
WITH nodes AS (SELECT unnest(nodes) AS n FROM read_json('$TMP/f.json')),
c AS (SELECT
    count(*) FILTER (WHERE n.branch = '${MAIN_BRANCH}')::INT AS br_main,
    count(*) FILTER (WHERE n.branch = '${FEAT_BRANCH}')::INT AS br_feat,
    count(*) FILTER (WHERE n.branch = '${FIX_BRANCH}')::INT  AS br_fix,
    count(*) FILTER (WHERE n.commit_sha = '${MAIN_SHA}')::INT AS sha_main,
    count(*) FILTER (WHERE n.commit_sha = '${FEAT_SHA}')::INT AS sha_feat,
    count(*) FILTER (WHERE n.commit_sha = '${FIX_SHA}')::INT  AS sha_fix,
    count(*) FILTER (WHERE n.name = 'tests.py')::INT AS tests_ct,
    count(*) FILTER (WHERE n.name = 'main.py')::INT  AS main_ct,
    count(*) FILTER (WHERE n.name = 'utils.py' AND n.branch = '${FIX_BRANCH}' AND contains(n.content, 'patched'))::INT AS fix_ct,
    count(*) FILTER (WHERE n.name = 'tests.py' AND contains(n.content, 'test_hello'))::INT AS feat_ct
FROM nodes),
checks AS (SELECT unnest([
    check_has('branch_${MAIN_BRANCH}', br_main),
    check_has('branch_${FEAT_BRANCH}', br_feat),
    check_has('branch_${FIX_BRANCH}',  br_fix),
    check_has('commit_${MAIN_SHA:0:8}', sha_main),
    check_has('commit_${FEAT_SHA:0:8}', sha_feat),
    check_has('commit_${FIX_SHA:0:8}',  sha_fix),
    check_count('branch_specific_file', tests_ct, 1, 'tests.py on feature only'),
    check_count('unique_ids', main_ct, 3, '3 main.py across 3 branches'),
    check_has('content_fix', fix_ct),
    check_has('content_feat', feat_ct),
    check_edges('traversal', (SELECT count(*)::INT FROM orbit_edges('$TMP/t.json')))
]) AS r FROM c)
INSERT INTO results SELECT r.name, r.ok, r.detail FROM checks;
SQL
db ".read $TMP/expect.sql"

emit_results
