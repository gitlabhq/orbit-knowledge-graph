#!/usr/bin/env bash
# Lefthook entry point for comment_check.py.
#
# Only the checker's "comments flagged" exit code (3) blocks the commit. uv or
# mise failing to start (no network and a cold cache, an untrusted worktree)
# exits with their own codes, and those must never block a commit.
set -u

cd "$(git rev-parse --show-toplevel)" || exit 0
script="$(dirname "$0")/comment_check.py"

mise exec -- uv run --quiet "$script" --staged
status=$?
if [ "$status" -eq 3 ]; then
    exit 1
fi
if [ "$status" -ne 0 ]; then
    echo "comment check: skipped (exit $status); the check could not run." >&2
fi
exit 0
