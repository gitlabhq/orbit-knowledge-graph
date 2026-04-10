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

# ── Index & query once ───────────────────────────────────────────
index_repos "$REPO" "$WT_FEAT" "$WT_FIX"

orbit_query "$Q_FILES" "$TMP/files.json"
orbit_query "$Q_TRAVERSAL" "$TMP/trav.json"

# ── Assertions (all against the single files.json) ───────────────
for b in "$MAIN_BRANCH" "$FEAT_BRANCH" "$FIX_BRANCH"; do
    assert_has "branch_$b" "$TMP/files.json" "n.branch = '$b'"
done

for sha in "$MAIN_SHA" "$FEAT_SHA" "$FIX_SHA"; do
    assert_has "commit_${sha:0:8}" "$TMP/files.json" "n.commit_sha = '$sha'"
done

assert_count "branch_specific_file" "$TMP/files.json" "n.name = 'tests.py'" 1 "tests.py on feature only"
assert_count "unique_ids" "$TMP/files.json" "n.name = 'main.py'" 3 "3 main.py across 3 branches"
assert_has "content_fix" "$TMP/files.json" "n.name = 'utils.py' AND n.branch = '$FIX_BRANCH' AND contains(n.content, 'patched')"
assert_has "content_feat" "$TMP/files.json" "n.name = 'tests.py' AND contains(n.content, 'test_hello')"
assert_edges "traversal" "$TMP/trav.json"

emit_results
