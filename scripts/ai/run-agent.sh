#!/bin/sh
set -eu
#
# Launches an opencode agent with full process isolation:
#   1. runuser -u agent  → different UID, can't read root's /proc/*/environ
#   2. env -i            → clean /proc/self/environ, only allowlisted vars
#
# The agent can use `glab` to post MR comments — requests route through
# the proxy (localhost:8083) which injects the real PRIVATE-TOKEN.
# GITLAB_TOKEN=proxy-handled is a dummy; the proxy replaces it.
#
# Usage: scripts/ai/run-agent.sh opencode run --agent review ...

WORKSPACE_BIN="$(cd "$(dirname "$0")" && pwd)/node_modules/.bin"

exec runuser -u agent -- env -i \
  HOME=/home/agent \
  PATH="${WORKSPACE_BIN}:/home/agent/.bun/bin:/usr/local/bin:/usr/bin:/bin" \
  USER=agent \
  SHELL=/bin/sh \
  TERM="${TERM:-dumb}" \
  LANG="${LANG:-C.UTF-8}" \
  CI_PROJECT_ID="$CI_PROJECT_ID" \
  CI_PROJECT_NAME="$CI_PROJECT_NAME" \
  CI_PROJECT_PATH="$CI_PROJECT_PATH" \
  CI_MERGE_REQUEST_IID="$CI_MERGE_REQUEST_IID" \
  CI_MERGE_REQUEST_SOURCE_BRANCH_NAME="$CI_MERGE_REQUEST_SOURCE_BRANCH_NAME" \
  ANTHROPIC_API_KEY=proxy-handled \
  GITLAB_TOKEN=proxy-handled \
  OPENCODE_CONFIG_CONTENT="$OPENCODE_CONFIG_CONTENT" \
  "$@"
