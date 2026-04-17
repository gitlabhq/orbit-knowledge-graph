#!/usr/bin/env bash
# Run e2e Robot Framework tests against a local GDK instance.
#
# Prerequisites:
#   - GDK running with knowledge_graph feature flag enabled
#   - GKG stack running (webserver, dispatcher, indexer)
#   - pip install robotframework robotframework-requests
#
# Usage:
#   ./e2e/local/run.sh                         # run ALL suites under e2e/tests/ and e2e/local/suites/
#   ./e2e/local/run.sh graph_status            # run a single file (searches both dirs)
#   ./e2e/local/run.sh boundary payload state  # run multiple suites from e2e/local/suites/
#   FAST=1 ./e2e/local/run.sh                  # shrink wait timeouts for smoke runs
#   ./e2e/local/run.sh --include smoke         # pass robot flags through
#
# Env (defaults in parens):
#   GITLAB_URL              (https://gdk.test:3443)
#   GITLAB_PAT              (read from ~/.gdk_token)
#   VERIFY_SSL              (false — GDK uses a self-signed cert)
#   WAIT_IDLE_SECS          (300) — waits for ns to reach state=idle
#   DISPATCH_CYCLE_SECS     (150) — dispatcher coalesces, so allow 2 ticks
#   WAIT_CODE_SECS          (180) — code indexing pipeline end-to-end
#   DELETION_WAIT_SECS      (600) — namespace deletion handler latency

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TESTS_DIR="$REPO_ROOT/e2e/tests"
SUITES_DIR="$REPO_ROOT/e2e/local/suites"

: "${GITLAB_URL:=https://gdk.test:3443}"
: "${GITLAB_PAT:=$(cat "$HOME/.gdk_token" 2>/dev/null || echo "")}"
: "${VERIFY_SSL:=false}"
: "${WAIT_IDLE_SECS:=300}"
: "${DISPATCH_CYCLE_SECS:=150}"
: "${WAIT_CODE_SECS:=180}"
: "${DELETION_WAIT_SECS:=600}"

if [[ -z "$GITLAB_PAT" ]]; then
  echo "Error: GITLAB_PAT not set and ~/.gdk_token not found."
  echo "Create a PAT via the GDK UI or set GITLAB_PAT in your environment."
  exit 1
fi

if [[ "${FAST:-0}" == "1" ]]; then
  WAIT_IDLE_SECS=120
  DISPATCH_CYCLE_SECS=90
  WAIT_CODE_SECS=120
  DELETION_WAIT_SECS=180
fi

export GITLAB_URL GITLAB_PAT VERIFY_SSL \
       WAIT_IDLE_SECS DISPATCH_CYCLE_SECS WAIT_CODE_SECS DELETION_WAIT_SECS

SUITE_ARGS=()
ROBOT_FLAGS=()
while [[ $# -gt 0 ]]; do
  if [[ "$1" =~ ^- ]]; then
    ROBOT_FLAGS+=("$1")
    shift
    continue
  fi
  if [[ -f "$SUITES_DIR/$1.robot" ]]; then
    SUITE_ARGS+=("$SUITES_DIR/$1.robot")
  elif [[ -f "$TESTS_DIR/$1.robot" ]]; then
    SUITE_ARGS+=("$TESTS_DIR/$1.robot")
  else
    echo "Error: suite '$1' not found in $SUITES_DIR or $TESTS_DIR"
    exit 1
  fi
  shift
done

if [[ ${#SUITE_ARGS[@]} -eq 0 ]]; then
  SUITE_ARGS=("$TESTS_DIR" "$SUITES_DIR")
fi

mkdir -p "$REPO_ROOT/e2e/local/results"
exec robot \
  --outputdir "$REPO_ROOT/e2e/local/results" \
  --loglevel INFO \
  "${ROBOT_FLAGS[@]}" \
  "${SUITE_ARGS[@]}"
