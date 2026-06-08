#!/usr/bin/env bash
#
# Stale merge-request sweeper. Runs from a daily scheduled pipeline.
#
# Phase 1 (warn):  open MRs idle >= STALE_IDLE_DAYS get the
#                  "<prefix>::scheduled_for_closing" label and a warning comment.
# Phase 2 (act):   MRs already carrying that label are closed once it has been
#                  present >= STALE_GRACE_DAYS, unless the author opted out with
#                  "<prefix>::keep" or activity resumed (then the label is removed).
#
# State lives entirely in GitLab labels, so the job is stateless and idempotent.
# Set DRY_RUN=true to log decisions without mutating anything.

set -euo pipefail

PROJECT_ID="${CI_PROJECT_ID:?CI_PROJECT_ID is required}"
IDLE_DAYS="${STALE_IDLE_DAYS:-14}"
GRACE_DAYS="${STALE_GRACE_DAYS:-3}"
PREFIX="${STALE_LABEL_PREFIX:-stale}"
DRY_RUN="${DRY_RUN:-true}"

KEEP_LABEL="${PREFIX}::keep"
SCHEDULED_LABEL="${PREFIX}::scheduled_for_closing"

NOW_EPOCH="$(date -u +%s)"
IDLE_CUTOFF_EPOCH=$(( NOW_EPOCH - IDLE_DAYS * 86400 ))
GRACE_SECONDS=$(( GRACE_DAYS * 86400 ))

log() { printf '%s\n' "$*" >&2; }

# GitLab timestamps vary (".000Z", "+00:00"); strip the fractional part so GNU
# date parses every variant. Empty/null inputs return 0 (treated as ancient by
# callers only where that is safe — see get_last_activity_epoch).
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

api() { glab api "$@"; }

get_last_activity_epoch() {
  local iid="$1" created_at="$2"
  local latest_commit latest_note created_epoch commit_epoch note_epoch

  latest_commit="$(api "projects/$PROJECT_ID/merge_requests/$iid/commits?per_page=1" \
    | jq -r '.[0].committed_date // empty')"
  latest_note="$(api "projects/$PROJECT_ID/merge_requests/$iid/notes?sort=desc&order_by=created_at&per_page=100" \
    | jq -r --arg bot "$BOT_USERNAME" \
      '[.[] | select(.system == false and .author.username != $bot)][0].created_at // empty')"

  created_epoch="$(to_epoch "$created_at")"
  commit_epoch="$(to_epoch "$latest_commit")"
  note_epoch="$(to_epoch "$latest_note")"

  local max="$created_epoch"
  [ "$commit_epoch" -gt "$max" ] && max="$commit_epoch"
  [ "$note_epoch" -gt "$max" ] && max="$note_epoch"
  echo "$max"
}

get_scheduled_label_epoch() {
  local iid="$1"
  local added_at
  added_at="$(api "projects/$PROJECT_ID/merge_requests/$iid/resource_label_events?per_page=100" \
    | jq -r --arg label "$SCHEDULED_LABEL" \
      '[.[] | select(.action == "add" and .label.name == $label)] | last | .created_at // empty')"
  to_epoch "$added_at"
}

add_scheduled_label() {
  local iid="$1"
  if [ "$DRY_RUN" = "true" ]; then return; fi
  api --method PUT "projects/$PROJECT_ID/merge_requests/$iid" \
    -f "add_labels=$SCHEDULED_LABEL" >/dev/null
}

remove_scheduled_label() {
  local iid="$1"
  if [ "$DRY_RUN" = "true" ]; then return; fi
  api --method PUT "projects/$PROJECT_ID/merge_requests/$iid" \
    -f "remove_labels=$SCHEDULED_LABEL" >/dev/null
}

post_comment() {
  local iid="$1" body="$2"
  if [ "$DRY_RUN" = "true" ]; then return; fi
  api --method POST "projects/$PROJECT_ID/merge_requests/$iid/notes" \
    -f "body=$body" >/dev/null
}

close_mr() {
  local iid="$1"
  if [ "$DRY_RUN" = "true" ]; then return; fi
  api --method PUT "projects/$PROJECT_ID/merge_requests/$iid" \
    -f "state_event=close" >/dev/null
}

warn_comment_body() {
  local author="$1"
  cat <<EOF
:wave: @${author} this merge request hasn't had activity in ${IDLE_DAYS} days.

It now has the ~"${SCHEDULED_LABEL}" label and will be **closed in ${GRACE_DAYS} days** unless something changes.

To keep it open, add the ~"${KEEP_LABEL}" label — or just push a commit or leave a comment, which resets the clock.

<sub>Automated by the stale-MR sweeper.</sub>
EOF
}

close_comment_body() {
  cat <<EOF
:lock: Closing this merge request after $(( IDLE_DAYS + GRACE_DAYS )) days without activity.

Reopen it any time the work is still relevant, and add ~"${KEEP_LABEL}" to keep it from being swept again.

<sub>Automated by the stale-MR sweeper.</sub>
EOF
}

process_mr() {
  local mr="$1"
  local iid author created_at labels has_keep has_scheduled

  iid="$(printf '%s' "$mr" | jq -r '.iid')"
  author="$(printf '%s' "$mr" | jq -r '.author.username')"
  created_at="$(printf '%s' "$mr" | jq -r '.created_at')"
  labels="$(printf '%s' "$mr" | jq -r '.labels[]?')"

  has_keep=false
  has_scheduled=false
  printf '%s\n' "$labels" | grep -qxF "$KEEP_LABEL" && has_keep=true
  printf '%s\n' "$labels" | grep -qxF "$SCHEDULED_LABEL" && has_scheduled=true

  if [ "$has_scheduled" = true ]; then
    if [ "$has_keep" = true ]; then
      log "MR !$iid: has $KEEP_LABEL, removing $SCHEDULED_LABEL (kept by author)"
      remove_scheduled_label "$iid"
      return
    fi

    local label_epoch activity_epoch
    label_epoch="$(get_scheduled_label_epoch "$iid")"
    activity_epoch="$(get_last_activity_epoch "$iid" "$created_at")"

    if [ "$label_epoch" -eq 0 ]; then
      log "MR !$iid: $SCHEDULED_LABEL present but no add event found, skipping"
      return
    fi

    if [ "$activity_epoch" -gt "$label_epoch" ]; then
      log "MR !$iid: activity resumed after warning, removing $SCHEDULED_LABEL"
      remove_scheduled_label "$iid"
      return
    fi

    local age=$(( NOW_EPOCH - label_epoch ))
    if [ "$age" -ge "$GRACE_SECONDS" ]; then
      log "MR !$iid: grace elapsed (${age}s >= ${GRACE_SECONDS}s), closing"
      post_comment "$iid" "$(close_comment_body)"
      close_mr "$iid"
    else
      local remaining=$(( (GRACE_SECONDS - age + 86399) / 86400 ))
      log "MR !$iid: in grace period, ~${remaining}d until close"
    fi
    return
  fi

  if [ "$has_keep" = true ]; then
    log "MR !$iid: has $KEEP_LABEL, skipping"
    return
  fi

  local activity_epoch
  activity_epoch="$(get_last_activity_epoch "$iid" "$created_at")"
  if [ "$activity_epoch" -le "$IDLE_CUTOFF_EPOCH" ]; then
    log "MR !$iid: idle since $(date -u -d "@$activity_epoch" +%Y-%m-%d), warning"
    add_scheduled_label "$iid"
    post_comment "$iid" "$(warn_comment_body "$author")"
  else
    log "MR !$iid: active, skipping"
  fi
}

BOT_USERNAME="$(api "user" | jq -r '.username')"
log "Stale-MR sweeper: bot=$BOT_USERNAME idle=${IDLE_DAYS}d grace=${GRACE_DAYS}d prefix=$PREFIX dry_run=$DRY_RUN"

api --paginate "projects/$PROJECT_ID/merge_requests?state=opened&per_page=100" \
  | jq -c '.[]' \
  | while IFS= read -r mr; do
      process_mr "$mr"
    done

log "Stale-MR sweeper: done"
