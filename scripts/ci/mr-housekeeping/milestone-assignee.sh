#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/ci/mr-housekeeping/lib.sh
source "$HERE/lib.sh"

TODAY="$(date -u -d "@$NOW_EPOCH" +%F)"
SKIP_LABELS="$(jq -n -c --arg exclude "$EXCLUDE_LABEL" --arg closing "${STALE_LABEL_PREFIX:-stale}::scheduled_for_closing" '[$exclude, $closing]')"

jq_rules() { jq -L "$HERE" "$@"; }

# shellcheck disable=SC2329
process_mr() {
  local mr="$1" iid author_id issue_milestones='[]' rule_mr plan
  iid="$(printf '%s' "$mr" | jq -r '.iid')"
  author_id="$(printf '%s' "$mr" | jq -r '.author.id')"

  if printf '%s' "$mr" | jq -e '.milestone == null' >/dev/null; then
    issue_milestones="$(api "projects/$PROJECT_ID/merge_requests/$iid/closes_issues?per_page=100" \
      | jq -c 'map(.milestone | select(.) | {id, title, due_date})')" || return 1
  fi

  rule_mr="$(printf '%s' "$mr" | jq -c '{
    iid, draft, labels,
    assignees: (.assignees // [] | map({id})),
    milestone: (.milestone | if . then {id, title, due_date} else null end),
    author: {id: .author.id, bot: .author.bot}
  }')" || return 1

  # shellcheck disable=SC2016
  plan="$(jq_rules -n -c \
    --argjson mr "$rule_mr" \
    --argjson milestones "$MILESTONES" \
    --argjson issue_milestones "$issue_milestones" \
    --arg today "$TODAY" \
    --argjson skip_labels "$SKIP_LABELS" \
    'include "milestone-assignee";
     plan($mr; $milestones; $issue_milestones; $today; $skip_labels)')" || return 1

  if printf '%s' "$plan" | jq -e 'has("skip")' >/dev/null; then
    log "MR !$iid: $(printf '%s' "$plan" | jq -r '.skip'), skipping"
    return
  fi

  local args=()
  if printf '%s' "$plan" | jq -e '.milestone != null' >/dev/null; then
    log "MR !$iid: $(printf '%s' "$plan" | jq -r '.milestone.reason'), setting milestone $(printf '%s' "$plan" | jq -r '.milestone.title')"
    args+=(-f "milestone_id=$(printf '%s' "$plan" | jq -r '.milestone.id')")
  fi
  if printf '%s' "$plan" | jq -e '.assign' >/dev/null; then
    log "MR !$iid: ready without an assignee, assigning the author"
    args+=(-f "assignee_id=$author_id")
  fi

  if [ "${#args[@]}" -eq 0 ]; then
    log "MR !$iid: nothing to change"
    return
  fi
  if [ "$DRY_RUN" = "true" ]; then return; fi
  api --method PUT "projects/$PROJECT_ID/merge_requests/$iid" "${args[@]}" >/dev/null
}

MILESTONES="$(api --paginate "projects/$PROJECT_ID/milestones?state=active&include_ancestors=true&per_page=100" | jq -s -c 'add // [] | map({id, title, start_date, due_date})')"
# shellcheck disable=SC2016
CURRENT="$(jq_rules -n -r --argjson milestones "$MILESTONES" --arg today "$TODAY" \
  'include "milestone-assignee"; current_milestone($milestones; $today) | .title // empty')"

STATUS=0
if [ -z "$CURRENT" ]; then
  log "milestone-assignee: no active release milestone covers $TODAY"
  STATUS=1
fi
log "milestone-assignee: today=$TODAY current=${CURRENT:-none} dry_run=$DRY_RUN"
for_each_open_mr milestone-assignee process_mr || STATUS=1
exit "$STATUS"
