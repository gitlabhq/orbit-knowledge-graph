#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/ci/mr-housekeeping/lib.sh
source "$HERE/lib.sh"

# stale runs first so the MRs it closes are not given a milestone in the same run.
TASKS="${HOUSEKEEPING_TASKS:-stale milestone-assignee}"

BOT_USER="$(api "user")"
BOT_USERNAME="$(printf '%s' "$BOT_USER" | jq -r '.username')"
BOT_USER_ID="$(printf '%s' "$BOT_USER" | jq -r '.id')"
export BOT_USERNAME BOT_USER_ID
log "mr-housekeeping: bot=$BOT_USERNAME tasks=\"$TASKS\" dry_run=$DRY_RUN"

# Fail before any task if the token cannot write here: every task needs
# Developer, and with less the run dies on its first write with every MR behind
# it never evaluated. members/all resolves direct, inherited and invited-group
# membership alike; a token minted in another project resolves 404.
ACCESS_LEVEL="$(api "projects/$PROJECT_ID/members/all/$BOT_USER_ID" 2>/dev/null | jq -r '.access_level // 0' || true)"
ACCESS_LEVEL="${ACCESS_LEVEL:-0}"
if [ "$ACCESS_LEVEL" -lt 30 ]; then
  if [ "$DRY_RUN" = "true" ]; then
    log "WARNING: $BOT_USERNAME has access level $ACCESS_LEVEL on project $PROJECT_ID (writes need Developer/30); a non-dry run would fail on its first write"
  else
    log "FATAL: $BOT_USERNAME has access level $ACCESS_LEVEL on project $PROJECT_ID; mr-housekeeping writes need Developer (30)."
    log "Mint the token on this project or an ancestor group (see scripts/ci/mr-housekeeping/README.md). Project bot users cannot be added to other projects."
    exit 1
  fi
fi

FAILED_TASKS=()
for task in $TASKS; do
  case "$task" in
    stale | milestone-assignee) ;;
    *)
      log "Unknown task \"$task\""
      FAILED_TASKS+=("$task")
      continue
      ;;
  esac
  log "--- $task ---"
  bash "$HERE/$task.sh" || FAILED_TASKS+=("$task")
done

if [ "${#FAILED_TASKS[@]}" -gt 0 ]; then
  log "mr-housekeeping: failed tasks: ${FAILED_TASKS[*]}"
  exit 1
fi
log "mr-housekeeping: done"
