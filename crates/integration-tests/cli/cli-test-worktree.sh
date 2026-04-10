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

# Feature worktree: adds tests.py
cd "$REPO" && git worktree add -q -b "$FEAT_BRANCH" "$WT_FEAT"
cat > "$WT_FEAT/src/tests.py" << 'PY'
def test_hello():
    pass
PY
cd "$WT_FEAT" && git add -A && git commit -q -m "add tests"
FEAT_SHA=$(git rev-parse HEAD)

# Fix worktree: modifies utils.py (from initial commit, no tests.py)
cd "$REPO" && git worktree add -q -b "$FIX_BRANCH" "$WT_FIX" "$MAIN_SHA" 2>/dev/null
cat > "$WT_FIX/src/utils.py" << 'PY'
def patched():
    return True
PY
cd "$WT_FIX" && git add -A && git commit -q -m "patch utils"
FIX_SHA=$(git rev-parse HEAD)

cd "$TMP"

# ── 1. Index ─────────────────────────────────────────────────────
for r in "$REPO" "$WT_FEAT" "$WT_FIX"; do
    name=$(basename "$r")
    "$ORBIT" index "$r" > /dev/null 2>&1 \
        && add "index_$name" true \
        || add "index_$name" false "indexing failed"
done

# ── 2. Branch tracking ──────────────────────────────────────────
orbit_query "$Q_FILES_BRANCH" "$TMP/files.json"

for branch in "$MAIN_BRANCH" "$FEAT_BRANCH" "$FIX_BRANCH"; do
    c=$(count_nodes "$TMP/files.json" "n.branch = '$branch'")
    [ "$c" -gt 0 ] && add "branch_$branch" true "$c files" \
                    || add "branch_$branch" false "not found"
done

# ── 3. Commit SHA tracking ──────────────────────────────────────
orbit_query "$Q_FILES_SHA" "$TMP/shas.json"

for sha in "$MAIN_SHA" "$FEAT_SHA" "$FIX_SHA"; do
    c=$(count_nodes "$TMP/shas.json" "n.commit_sha = '$sha'")
    [ "$c" -gt 0 ] && add "commit_${sha:0:8}" true \
                    || add "commit_${sha:0:8}" false "not found"
done

# ── 4. Branch-specific files ────────────────────────────────────
tests_count=$(count_nodes "$TMP/files.json" "n.name = 'tests.py'")
[ "$tests_count" -eq 1 ] && add "branch_specific_file" true "tests.py on feature only" \
                          || add "branch_specific_file" false "tests.py count: $tests_count"

# ── 5. Unique IDs per branch ────────────────────────────────────
main_count=$(count_nodes "$TMP/files.json" "n.name = 'main.py'")
[ "$main_count" -eq 3 ] && add "unique_ids" true "3 main.py across 3 branches" \
                         || add "unique_ids" false "main.py count: $main_count"

# ── 6. Content resolution ───────────────────────────────────────
orbit_query "$Q_FILES_CONTENT" "$TMP/content.json"

fix_ok=$(count_nodes "$TMP/content.json" "n.name = 'utils.py' AND n.branch = '$FIX_BRANCH' AND contains(n.content, 'patched')")
feat_ok=$(count_nodes "$TMP/content.json" "n.name = 'tests.py' AND contains(n.content, 'test_hello')")

[ "$fix_ok" -gt 0 ]  && add "content_fix_branch" true  || add "content_fix_branch" false "patched not found"
[ "$feat_ok" -gt 0 ] && add "content_feat_branch" true || add "content_feat_branch" false "test_hello not found"

# ── 7. Traversal ────────────────────────────────────────────────
orbit_query "$Q_TRAVERSAL" "$TMP/trav.json"

ec=$(count_edges "$TMP/trav.json")
[ "$ec" -gt 0 ] && add "traversal" true "$ec edges" || add "traversal" false "no edges"

emit_results
