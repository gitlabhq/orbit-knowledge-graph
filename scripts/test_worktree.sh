#!/usr/bin/env bash
#
# Test worktree and branch/commit tracking in the local indexer.
#
# Creates a temp repo with worktrees, indexes them, and validates
# that branch, commit_sha, and parent_repo grouping work correctly.
#
# Usage:
#   ./scripts/test_worktree.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
export DYLD_LIBRARY_PATH="$PROJECT_ROOT/target/debug/deps"
ORBIT="$PROJECT_ROOT/target/debug/orbit"
TMP=$(mktemp -d)
PASS=0; FAIL=0

pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }

cleanup() { rm -rf "$TMP" ~/.orbit/graph.duckdb; }
trap cleanup EXIT

echo "Building orbit..."
cargo build -p orbit -q 2>/dev/null

echo ""
echo "=== Worktree & commit tracking test ==="

# ── Setup: repo with two worktrees ───────────────────────────────
REPO="$TMP/my-project"
WT_FEAT="$TMP/my-project-feature"
WT_FIX="$TMP/my-project-fix"

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

git add -A && git commit -q -m "initial commit"
MAIN_SHA=$(git rev-parse HEAD)
MAIN_BRANCH=$(git symbolic-ref --short HEAD)

# Feature worktree: adds a test file
git worktree add -q -b feature/tests "$WT_FEAT"
cat > "$WT_FEAT/src/tests.py" << 'PY'
from main import hello, App

def test_hello():
    hello()

class TestApp:
    def test_run(self):
        App().run()
PY
cd "$WT_FEAT" && git add -A && git commit -q -m "add tests"
FEAT_SHA=$(git rev-parse HEAD)

# Fix worktree: branch from initial commit (before tests.py was added)
git worktree add -q -b fix/read-file "$WT_FIX" "$MAIN_SHA" 2>/dev/null
cd "$WT_FIX"
cat > src/utils.py << 'PY'
from pathlib import Path

def read_file(path):
    return Path(path).read_text()

def write_file(path, content):
    Path(path).write_text(content)
PY
git add -A && git commit -q -m "fix read_file, add write_file"
FIX_SHA=$(git rev-parse HEAD)

cd "$PROJECT_ROOT"
rm -f ~/.orbit/graph.duckdb

echo "Repos:"
echo "  main:    $REPO ($MAIN_BRANCH @ ${MAIN_SHA:0:8})"
echo "  feature: $WT_FEAT (feature/tests @ ${FEAT_SHA:0:8})"
echo "  fix:     $WT_FIX (fix/read-file @ ${FIX_SHA:0:8})"
echo ""

# ── 1. Index all three ───────────────────────────────────────────
echo "--- 1. Index all three repos"
for r in "$REPO" "$WT_FEAT" "$WT_FIX"; do
    if "$ORBIT" index "$r" > /dev/null 2>&1; then
        pass "indexed $(basename "$r")"
    else
        fail "indexing $(basename "$r")"
    fi
done

# ── 2. Each has correct branch ───────────────────────────────────
echo "--- 2. Branch tracking"
RESULT=$("$ORBIT" query '{"query_type":"search","node":{"id":"f","entity":"File","columns":["id","name","branch"]},"limit":20}' 2>/dev/null)

for branch in "$MAIN_BRANCH" "feature/tests" "fix/read-file"; do
    count=$(echo "$RESULT" | grep -c "\"branch\": \"$branch\"" || true)
    if [[ $count -gt 0 ]]; then
        pass "branch '$branch' has $count files"
    else
        fail "branch '$branch' not found in results"
    fi
done

# ── 3. Each has correct commit SHA ───────────────────────────────
echo "--- 3. Commit SHA tracking"
RESULT=$("$ORBIT" query '{"query_type":"search","node":{"id":"f","entity":"File","columns":["id","name","commit_sha"]},"limit":20}' 2>/dev/null)

for sha in "$MAIN_SHA" "$FEAT_SHA" "$FIX_SHA"; do
    if echo "$RESULT" | grep -q "$sha"; then
        pass "commit ${sha:0:8} found"
    else
        fail "commit ${sha:0:8} not found"
    fi
done

# ── 4. Feature branch has tests.py, main doesn't ────────────────
echo "--- 4. Branch-specific files"
FEAT_FILES=$("$ORBIT" query '{"query_type":"search","node":{"id":"f","entity":"File","columns":["id","name","branch"]},"limit":20}' 2>/dev/null)

feat_tests=$(echo "$FEAT_FILES" | grep -c "tests.py" || true)
if [[ $feat_tests -eq 1 ]]; then
    pass "tests.py exists only on feature branch"
else
    fail "tests.py count: $feat_tests (expected 1)"
fi

# ── 5. Fix branch has write_file, main doesn't ──────────────────
echo "--- 5. Branch-specific definitions"
DEFS=$("$ORBIT" query '{"query_type":"search","node":{"id":"d","entity":"Definition","columns":["id","name","branch"]},"limit":30}' 2>/dev/null)

write_file_count=$(echo "$DEFS" | grep -c "write_file" || true)
if [[ $write_file_count -eq 1 ]]; then
    pass "write_file exists only on fix branch"
else
    fail "write_file count: $write_file_count (expected 1)"
fi

# ── 6. Different IDs for same file on different branches ─────────
echo "--- 6. Unique IDs per branch"
# Use raw output for easier parsing
RAW=$("$ORBIT" query --raw '{"query_type":"search","node":{"id":"f","entity":"File","columns":["id","name","branch"]},"limit":20}' 2>/dev/null)
main_ids=$(echo "$RAW" | grep -o '"id":-\?[0-9]*' | grep -B1 "main.py" || echo "$RAW" | python3 -c "
import sys,json
data=json.load(sys.stdin)
ids=[n['id'] for n in data.get('nodes',[]) if n.get('name')=='main.py']
for i in ids: print(i)
" 2>/dev/null)
unique_count=$(echo "$RAW" | python3 -c "
import sys,json
data=json.load(sys.stdin)
ids=set(n['id'] for n in data.get('nodes',[]) if n.get('name')=='main.py')
print(len(ids))
" 2>/dev/null)

if [[ "$unique_count" -eq 3 ]]; then
    pass "main.py has 3 unique IDs across 3 branches"
else
    fail "main.py has $unique_count unique IDs (expected 3)"
fi

# ── 7. Traversal works across branches ───────────────────────────
echo "--- 7. Cross-branch traversal"
TRAV=$("$ORBIT" query '{"query_type":"traversal","nodes":[{"id":"f","entity":"File","columns":["id","name","branch"]},{"id":"d","entity":"Definition","columns":["id","name"]}],"relationships":[{"type":"DEFINES","from":"f","to":"d"}],"limit":10}' 2>/dev/null)

edge_count=$(echo "$TRAV" | grep -c '"type": "DEFINES"' || true)
if [[ $edge_count -gt 0 ]]; then
    pass "traversal returned $edge_count DEFINES edges"
else
    fail "traversal returned no edges"
fi

# ── 8. Content resolution works per branch ───────────────────────
echo "--- 8. Content resolution from correct worktree"
CONTENT=$("$ORBIT" query '{"query_type":"search","node":{"id":"f","entity":"File","columns":["id","name","branch","content"]},"limit":20}' 2>/dev/null)

if echo "$CONTENT" | grep -q "write_file"; then
    pass "fix branch content includes write_file"
else
    fail "fix branch content missing write_file"
fi

if echo "$CONTENT" | grep -q "test_hello"; then
    pass "feature branch content includes test_hello"
else
    fail "feature branch content missing test_hello"
fi

# ── Summary ──────────────────────────────────────────────────────
echo ""
echo "=== $PASS passed, $FAIL failed ==="
exit "$FAIL"
