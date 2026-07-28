#!/usr/bin/env bash
set -euo pipefail

if [ -n "${CI:-}" ]; then
    exec python3 scripts/check-prompt-version-bump.py --ci --no-worktree "$@"
fi

exec python3 scripts/check-prompt-version-bump.py --ci --staged "$@"
