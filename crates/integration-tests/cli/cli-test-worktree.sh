#!/usr/bin/env bash
# Worktree and branch/commit tracking test.
# Usage: cli-test-worktree.sh <orbit> <repo> <wt-feat> <wt-fix> <main-branch> <main-sha> <feat-sha> <fix-sha>
ORBIT="$1"
REPO="$2"
WT_FEAT="$3"
WT_FIX="$4"
MAIN_BRANCH="$5"
MAIN_SHA="$6"
FEAT_SHA="$7"
FIX_SHA="$8"
FEAT_BRANCH="feature/tests"
FIX_BRANCH="fix/utils"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

# Index & query
index_repos "$REPO" "$WT_FEAT" "$WT_FIX"
orbit_query "$Q_FILES" "$TMP/f.json"
orbit_query "$Q_TRAVERSAL" "$TMP/t.json"

# Assert
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
