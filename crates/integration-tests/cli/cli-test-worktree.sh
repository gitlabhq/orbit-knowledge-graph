#!/usr/bin/env bash
#
# Worktree and branch/commit tracking test.
# Usage: cli-test-worktree.sh <orbit-binary>

ORBIT="$1"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

# ── Setup ────────────────────────────────────────────────────────
REPO="$TMP/my-project"
WT_FEAT="$TMP/my-project-feature"
WT_FIX="$TMP/my-project-fix"
FEAT_BRANCH="feature/tests"
FIX_BRANCH="fix/utils"

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

# ── Tests ────────────────────────────────────────────────────────

index_repos "$REPO" "$WT_FEAT" "$WT_FIX"

orbit_query "$Q_FILES_BRANCH" "$TMP/files.json"
orbit_query "$Q_FILES_SHA" "$TMP/shas.json"
orbit_query "$Q_FILES_CONTENT" "$TMP/content.json"
orbit_query "$Q_TRAVERSAL" "$TMP/trav.json"

# Branches exist
for b in "$MAIN_BRANCH" "$FEAT_BRANCH" "$FIX_BRANCH"; do
    assert_has "branch_$b" "$TMP/files.json" "n.branch = '$b'"
done

# Commits tracked
for sha in "$MAIN_SHA" "$FEAT_SHA" "$FIX_SHA"; do
    assert_has "commit_${sha:0:8}" "$TMP/shas.json" "n.commit_sha = '$sha'"
done

# tests.py only on feature branch
assert_count "branch_specific_file" "$TMP/files.json" "n.name = 'tests.py'" 1 "tests.py on feature only"

# main.py appears once per branch (3 total, unique IDs)
assert_count "unique_ids" "$TMP/files.json" "n.name = 'main.py'" 3 "3 main.py across 3 branches"

# Content resolves from correct worktree
assert_content "content_fix_branch" "$TMP/content.json" "n.name = 'utils.py' AND n.branch = '$FIX_BRANCH'" "patched"
assert_content "content_feat_branch" "$TMP/content.json" "n.name = 'tests.py'" "test_hello"

# Traversal works
ec=$(count_edges "$TMP/trav.json")
[ "$ec" -gt 0 ] && add "traversal" true "$ec edges" || add "traversal" false "no edges"

emit_results
