#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
failures=0

check() {
  if [[ "$2" == "$3" ]]; then printf '[ok] %s\n' "$1"; else printf '[fail] %s: expected %q, got %q\n' "$1" "$2" "$3"; failures=$((failures + 1)); fi
}

TODAY="2026-11-20"
MILESTONES='[
  {"id": 196, "title": "19.6", "start_date": "2026-10-10", "due_date": "2026-11-13"},
  {"id": 197, "title": "19.7", "start_date": "2026-11-14", "due_date": "2026-12-11"},
  {"id": 198, "title": "19.8", "start_date": "2026-12-12", "due_date": "2027-01-08"},
  {"id": 900, "title": "Backlog", "start_date": null, "due_date": null}
]'
SKIP_LABELS='["Community contribution", "stale::scheduled_for_closing"]'
ENDED='{"id": 196, "title": "19.6", "due_date": "2026-11-13"}'
FUTURE='{"id": 198, "title": "19.8", "due_date": "2027-01-08"}'

mr() {
  jq -n -c "{iid: 1, draft: false, labels: [], assignees: [], milestone: null, author: {id: 7, bot: false}} + ($1)"
}

plan_of() {
  jq -L "$HERE" -n -c \
    --argjson mr "$1" \
    --argjson milestones "$MILESTONES" \
    --argjson issue_milestones "${2:-[]}" \
    --arg today "${3:-$TODAY}" \
    --argjson skip_labels "$SKIP_LABELS" \
    'include "milestone-assignee"; plan($mr; $milestones; $issue_milestones; $today; $skip_labels)'
}

milestone_of() { printf '%s' "$1" | jq -r '.milestone.title // "none"'; }
assign_of() { printf '%s' "$1" | jq -r '.assign'; }

current_on() {
  jq -L "$HERE" -n -r --argjson milestones "$MILESTONES" --arg today "$1" \
    'include "milestone-assignee"; current_milestone($milestones; $today) | .title // "none"'
}

check "current milestone mid-cycle" "19.7" "$(current_on 2026-11-20)"
check "current milestone on its last day" "19.7" "$(current_on 2026-12-11)"
check "current milestone on the next one's first day" "19.8" "$(current_on 2026-12-12)"
check "no current milestone past the last one" "none" "$(current_on 2027-02-01)"

p="$(plan_of "$(mr '{}')")"
check "no milestone gets the current one" "19.7" "$(milestone_of "$p")"
check "ready unassigned MR gets the author" "true" "$(assign_of "$p")"

p="$(plan_of "$(mr '{}')" "[null, $FUTURE]")"
check "no milestone copies the linked issue's" "19.8" "$(milestone_of "$p")"

p="$(plan_of "$(mr '{}')" "[$ENDED]")"
check "ended issue milestone is not copied" "19.7" "$(milestone_of "$p")"

p="$(plan_of "$(mr '{}')" '[{"id": 555, "title": "Other group", "due_date": null}]')"
check "issue milestone unavailable to the project is not copied" "19.7" "$(milestone_of "$p")"

p="$(plan_of "$(mr "{milestone: $ENDED}")")"
check "ready MR with an ended milestone moves to current" "19.7" "$(milestone_of "$p")"

p="$(plan_of "$(mr "{draft: true, milestone: $ENDED}")")"
check "draft keeps its ended milestone" "none" "$(milestone_of "$p")"

p="$(plan_of "$(mr '{draft: true}')")"
check "draft without a milestone gets the current one" "19.7" "$(milestone_of "$p")"
check "draft is not assigned" "false" "$(assign_of "$p")"

p="$(plan_of "$(mr "{milestone: $FUTURE}")")"
check "future milestone is kept" "none" "$(milestone_of "$p")"

p="$(plan_of "$(mr '{milestone: {"id": 900, "title": "Backlog", "due_date": null}}')")"
check "milestone without a due date is kept" "none" "$(milestone_of "$p")"

p="$(plan_of "$(mr '{assignees: [{id: 8}]}')")"
check "assigned MR keeps its assignee" "false" "$(assign_of "$p")"

p="$(plan_of "$(mr "{milestone: $ENDED}")" '[]' 2027-02-01)"
check "no current milestone leaves an ended one alone" "none" "$(milestone_of "$p")"

p="$(plan_of "$(mr '{labels: ["bug", "Community contribution"]}')")"
check "community contribution is skipped" '{"skip":"has \"Community contribution\""}' "$p"

p="$(plan_of "$(mr '{labels: ["stale::scheduled_for_closing"]}')")"
check "MR scheduled for closing is skipped" '{"skip":"has \"stale::scheduled_for_closing\""}' "$p"

p="$(plan_of "$(mr '{author: {id: 9, bot: true}}')")"
check "bot user MR is skipped" '{"skip":"bot author"}' "$p"

if [ "$failures" -gt 0 ]; then
  printf '%d check(s) failed\n' "$failures"
  exit 1
fi
