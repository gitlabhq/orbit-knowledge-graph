#!/usr/bin/env bash
#
# Worktree and branch/commit tracking test.
# Outputs JSON: {"pass": N, "fail": N, "tests": [...]}
#
# Creates a temp repo with worktrees, indexes them, and validates
# branch tracking, commit SHAs, content resolution, and unique IDs.
#
# Usage: ./scripts/cli-test-worktree.sh <orbit-binary>

set -euo pipefail

ORBIT="$1"

export DYLD_LIBRARY_PATH="${ORBIT_LIB_PATH:-}"
export LD_LIBRARY_PATH="${ORBIT_LIB_PATH:-}"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

TESTS="[]"
add() {
    local name="$1" ok="$2" detail="${3:-}"
    TESTS=$(echo "$TESTS" | jq --arg n "$name" --argjson ok "$ok" --arg d "$detail" \
        '. + [{"name": $n, "ok": $ok, "detail": $d}]')
}

export ORBIT_DATA_DIR="$TMP/orbit"

REPO="$TMP/my-project"
WT_FEAT="$TMP/my-project-feature"
WT_FIX="$TMP/my-project-fix"

# Setup: repo with two worktrees
mkdir -p "$REPO/src"
cd "$REPO"
git init -q
git config user.email "test@test.com"
git config user.name "Test"

cat > src/main.py << 'PY'
def hello():
    print("hello")

class App:
    def run(self):
        hello()
PY

cat > src/utils.py << 'PY'
import os

def read_file(path):
    return open(path).read()
PY

git add -A && git commit -q -m "initial"
MAIN_SHA=$(git rev-parse HEAD)
MAIN_BRANCH=$(git symbolic-ref --short HEAD)

# Feature worktree: adds tests.py
git worktree add -q -b feature/tests "$WT_FEAT"
cat > "$WT_FEAT/src/tests.py" << 'PY'
def test_hello():
    pass
PY
cd "$WT_FEAT" && git add -A && git commit -q -m "add tests"
FEAT_SHA=$(git rev-parse HEAD)

# Fix worktree: modifies utils.py (branched from initial commit)
cd "$REPO"
git worktree add -q -b fix/utils "$WT_FIX" "$MAIN_SHA" 2>/dev/null
cat > "$WT_FIX/src/utils.py" << 'PY'
def patched():
    return True
PY
cd "$WT_FIX" && git add -A && git commit -q -m "patch utils"
FIX_SHA=$(git rev-parse HEAD)

cd "$TMP"

# 1. Index all three
for r in "$REPO" "$WT_FEAT" "$WT_FIX"; do
    name=$(basename "$r")
    if "$ORBIT" index "$r" > /dev/null 2>&1; then
        add "index_$name" true
    else
        add "index_$name" false "indexing failed"
    fi
done

# 2. Branch tracking
RESULT=$("$ORBIT" query --raw '{"query_type":"search","node":{"id":"f","entity":"File","columns":["id","name","branch"]},"limit":20}' 2>/dev/null)
for branch in "$MAIN_BRANCH" "feature/tests" "fix/utils"; do
    count=$(echo "$RESULT" | jq --arg b "$branch" '[.nodes[] | select(.branch == $b)] | length')
    if [ "$count" -gt 0 ]; then
        add "branch_$branch" true "$count files"
    else
        add "branch_$branch" false "not found"
    fi
done

# 3. Commit SHA tracking
SHA_RESULT=$("$ORBIT" query --raw '{"query_type":"search","node":{"id":"f","entity":"File","columns":["id","commit_sha"]},"limit":20}' 2>/dev/null)
for sha in "$MAIN_SHA" "$FEAT_SHA" "$FIX_SHA"; do
    short="${sha:0:8}"
    if echo "$SHA_RESULT" | jq -e --arg s "$sha" '.nodes[] | select(.commit_sha == $s)' > /dev/null 2>&1; then
        add "commit_$short" true
    else
        add "commit_$short" false "not found"
    fi
done

# 4. Branch-specific files
tests_count=$(echo "$RESULT" | jq '[.nodes[] | select(.name == "tests.py")] | length')
if [ "$tests_count" -eq 1 ]; then
    add "branch_specific_file" true "tests.py on feature only"
else
    add "branch_specific_file" false "tests.py count: $tests_count"
fi

# 5. Unique IDs per branch
unique_main_ids=$(echo "$RESULT" | jq '[.nodes[] | select(.name == "main.py") | .id] | unique | length')
total_main_ids=$(echo "$RESULT" | jq '[.nodes[] | select(.name == "main.py")] | length')
if [ "$unique_main_ids" -eq "$total_main_ids" ] && [ "$total_main_ids" -eq 3 ]; then
    add "unique_ids" true "3 unique IDs for main.py"
else
    add "unique_ids" false "$unique_main_ids unique of $total_main_ids"
fi

# 6. Content resolves from correct worktree
CONTENT=$("$ORBIT" query --raw '{"query_type":"search","node":{"id":"f","entity":"File","columns":["id","name","branch","content"]},"limit":20}' 2>/dev/null)

fix_has_patched=$(echo "$CONTENT" | jq '[.nodes[] | select(.name == "utils.py" and .branch == "fix/utils" and (.content | contains("patched")))] | length')
feat_has_test=$(echo "$CONTENT" | jq '[.nodes[] | select(.name == "tests.py" and (.content | contains("test_hello")))] | length')

if [ "$fix_has_patched" -eq 1 ]; then
    add "content_fix_branch" true
else
    add "content_fix_branch" false "patched not found in fix branch"
fi
if [ "$feat_has_test" -eq 1 ]; then
    add "content_feat_branch" true
else
    add "content_feat_branch" false "test_hello not found in feature branch"
fi

# 7. Traversal works
TRAV=$("$ORBIT" query --raw '{"query_type":"traversal","nodes":[{"id":"f","entity":"File","columns":["id","name"]},{"id":"d","entity":"Definition","columns":["id","name"]}],"relationships":[{"type":"DEFINES","from":"f","to":"d"}],"limit":10}' 2>/dev/null)
edge_count=$(echo "$TRAV" | jq '.edges | length')
if [ "$edge_count" -gt 0 ]; then
    add "traversal" true "$edge_count edges"
else
    add "traversal" false "no edges"
fi

# Output
pass=$(echo "$TESTS" | jq '[.[] | select(.ok)] | length')
fail=$(echo "$TESTS" | jq '[.[] | select(.ok | not)] | length')
jq -n --argjson p "$pass" --argjson f "$fail" --argjson t "$TESTS" \
    '{"pass": $p, "fail": $f, "tests": $t}'
