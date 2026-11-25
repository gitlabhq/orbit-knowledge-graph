# shellcheck shell=bash disable=SC2034  # variables are for the sourcing scripts
# run.sh exports BOT_USERNAME and BOT_USER_ID before it starts a task.

PROJECT_ID="${CI_PROJECT_ID:?CI_PROJECT_ID is required}"
DRY_RUN="${DRY_RUN:-true}"
EXCLUDE_LABEL="${HOUSEKEEPING_EXCLUDE_LABEL:-Community contribution}"
NOW_EPOCH="$(date -u +%s)"

log() { printf '%s\n' "$*" >&2; }

api() { glab api "$@"; }

to_epoch() {
  local timestamp="${1:-}"
  if [ -z "$timestamp" ] || [ "$timestamp" = "null" ]; then
    echo 0
    return
  fi
  local normalized
  normalized="$(printf '%s' "$timestamp" | sed -E 's/\.[0-9]+//')"
  date -u -d "$normalized" +%s 2>/dev/null || echo 0
}

list_open_mrs() {
  api --paginate "projects/$PROJECT_ID/merge_requests?state=opened&per_page=100" | jq -c '.[]'
}

has_label() {
  local mr="$1" label="$2"
  printf '%s' "$mr" | jq -e --arg label "$label" '.labels | index($label) != null' >/dev/null
}

for_each_open_mr() {
  local task="$1" process_fn="$2"
  local mr_list mr failed=0
  mr_list="$(list_open_mrs)" || return 1
  while IFS= read -r mr; do
    [ -z "$mr" ] && continue
    if ! "$process_fn" "$mr"; then
      log "MR !$(printf '%s' "$mr" | jq -r '.iid'): $task failed, continuing with the remaining MRs"
      failed=$(( failed + 1 ))
    fi
  done <<< "$mr_list"

  if [ "$failed" -gt 0 ]; then
    log "$task: done, $failed MR(s) failed"
    return 1
  fi
  log "$task: done"
}
