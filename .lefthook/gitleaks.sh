#!/usr/bin/env bash
set -euo pipefail

SCRIPT_NAME=$(basename "$0")
HOOK_TYPE="${1:-}"

if [ -z "$HOOK_TYPE" ]; then
  cat >&2 <<EOF
ERROR: Hook type argument is required.
Usage: ./$SCRIPT_NAME [pre-commit|pre-push]
EOF
  exit 1
fi

if [ "$HOOK_TYPE" == "pre-commit" ]; then
  mise exec -- gitleaks git --pre-commit --staged --no-banner --redact --verbose
elif [ "$HOOK_TYPE" == "pre-push" ]; then
  DEFAULT_BRANCH=$(git symbolic-ref refs/remotes/origin/HEAD 2>/dev/null | sed 's@^refs/remotes/@@' || echo "origin/main")
  BASE_COMMIT=$(git merge-base "$DEFAULT_BRANCH" HEAD)
  mise exec -- gitleaks git --log-opts="$BASE_COMMIT..HEAD" --no-banner --redact --verbose
else
  cat >&2 <<EOF
ERROR: Unsupported hook type '$HOOK_TYPE'.
Usage: ./$SCRIPT_NAME [pre-commit|pre-push]
EOF
  exit 1
fi
