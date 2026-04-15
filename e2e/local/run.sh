#!/usr/bin/env bash
# Run e2e Robot Framework tests against a local GDK instance.
#
# Prerequisites:
#   - GDK running with knowledge_graph feature flag enabled
#   - GKG stack running (webserver, dispatcher, indexer)
#   - pip install robotframework robotframework-requests
#
# Usage:
#   ./e2e/local/run.sh                    # run all tests
#   ./e2e/local/run.sh graph_status       # run one test file
#   ./e2e/local/run.sh --include smoke    # run tests tagged 'smoke'
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TESTS_DIR="$REPO_ROOT/e2e/tests"

: "${GITLAB_URL:=https://gdk.test:3443}"
: "${GITLAB_PAT:=$(cat "$HOME/.gdk_token" 2>/dev/null || echo "")}"

if [[ -z "$GITLAB_PAT" ]]; then
  echo "Error: GITLAB_PAT not set and ~/.gdk_token not found."
  echo "Create a PAT via the GDK UI or set GITLAB_PAT in your environment."
  exit 1
fi

# GDK uses a self-signed certificate
: "${VERIFY_SSL:=false}"

export GITLAB_URL GITLAB_PAT VERIFY_SSL

# Determine what to run
if [[ $# -gt 0 && ! "$1" =~ ^- ]]; then
  TARGET="$TESTS_DIR/$1.robot"
  shift
else
  TARGET="$TESTS_DIR"
fi

exec robot \
  --outputdir "$REPO_ROOT/e2e/local/results" \
  --loglevel INFO \
  "$@" \
  "$TARGET"
