#!/usr/bin/env bash
#
# Milestone sweeper. Runs from a daily scheduled pipeline.
#
# Policy (empty-only): every open, non-draft MR that has no milestone is
# assigned the current release milestone. An MR that already carries a
# milestone is left untouched — even if a human set a past one — so this never
# overwrites a deliberate human choice.
#
# The current milestone is resolved dynamically each run. The project has no
# project-level milestones; it inherits group-level ones on the release cadence
# (titles like "19.2"). "Current" is the active ancestor milestone whose title
# matches ^[0-9]+\.[0-9]+$ (so date-style / "Git 2.x" milestones are excluded)
# and whose window contains today (start_date <= today <= due_date). Group
# milestone IDs are accepted on project MR updates. If no current milestone
# resolves, the run is a non-fatal no-op (exit 0).
#
# The job holds no state — it re-reads MRs and the current milestone every run,
# so re-running is safe and idempotent. Set DRY_RUN=true to log decisions
# without mutating anything.

set -euo pipefail

PROJECT_ID="${CI_PROJECT_ID:?CI_PROJECT_ID is required}"
DRY_RUN="${DRY_RUN:-true}"

# Release-cadence milestones only (e.g. "19.2"); excludes "Git 2.55",
# "Backlog", date-style, and other non-release group milestones.
RELEASE_TITLE_PATTERN='^[0-9]+\.[0-9]+$'

TODAY="$(date -u +%F)"

log() { printf '%s\n' "$*" >&2; }

api() { glab api "$@"; }

# The active ancestor milestone whose title is a release version and whose
# window contains today. include_ancestors=true surfaces the group milestones
# (the project itself has none). Ties broken by earliest due_date so the closest
# release wins if windows ever overlap.
resolve_current_milestone() {
  api --paginate "projects/$PROJECT_ID/milestones?include_ancestors=true&state=active&per_page=100" \
    | jq -s --arg today "$TODAY" --arg pattern "$RELEASE_TITLE_PATTERN" '
        [ .[][]
          | select(.title | test($pattern))
          | select(.start_date != null and .due_date != null)
          | select(.start_date <= $today and $today <= .due_date)
        ]
        | sort_by(.due_date)
        | .[0] // empty
      '
}

set_milestone() {
  local iid="$1" milestone_id="$2"
  if [ "$DRY_RUN" = "true" ]; then return; fi
  api --method PUT "projects/$PROJECT_ID/merge_requests/$iid" \
    -f "milestone_id=$milestone_id" >/dev/null
}

process_mr() {
  local mr="$1" milestone_id="$2"
  local iid is_draft has_milestone

  iid="$(printf '%s' "$mr" | jq -r '.iid')"
  is_draft="$(printf '%s' "$mr" | jq -r '.draft')"
  has_milestone="$(printf '%s' "$mr" | jq -r 'if .milestone == null then "false" else "true" end')"

  if [ "$is_draft" = "true" ]; then
    log "MR !$iid: draft, skipping"
    return
  fi

  if [ "$has_milestone" = "true" ]; then
    log "MR !$iid: already has a milestone, skipping"
    return
  fi

  log "MR !$iid: no milestone, setting to milestone_id=$milestone_id"
  set_milestone "$iid" "$milestone_id"
}

CURRENT_MILESTONE="$(resolve_current_milestone)"

if [ -z "$CURRENT_MILESTONE" ]; then
  log "Milestone sweeper: no current release milestone for $TODAY, nothing to do"
  exit 0
fi

MILESTONE_ID="$(printf '%s' "$CURRENT_MILESTONE" | jq -r '.id')"
MILESTONE_TITLE="$(printf '%s' "$CURRENT_MILESTONE" | jq -r '.title')"

log "Milestone sweeper: current=$MILESTONE_TITLE (id=$MILESTONE_ID) today=$TODAY dry_run=$DRY_RUN"

# A single MR's failure (e.g. a transient 5xx on the PUT) must not abort the
# sweep and silently skip the rest under set -e, so each MR is isolated with
# `|| log`. The upstream MR-list fetch is not guarded, so a total API outage
# still fails the job loudly rather than reporting a false-green empty sweep.
api --paginate "projects/$PROJECT_ID/merge_requests?state=opened&per_page=100" \
  | jq -c '.[]' \
  | while IFS= read -r mr; do
      iid="$(printf '%s' "$mr" | jq -r '.iid')"
      process_mr "$mr" "$MILESTONE_ID" \
        || log "MR !$iid: processing failed, continuing with remaining MRs"
    done

log "Milestone sweeper: done"
