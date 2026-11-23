#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=scripts/ci/mr-housekeeping/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

IDLE_DAYS="${STALE_IDLE_DAYS:-5}"
GRACE_DAYS="${STALE_GRACE_DAYS:-2}"
PREFIX="${STALE_LABEL_PREFIX:-stale}"

KEEP_LABEL="${PREFIX}::keep"
SCHEDULED_LABEL="${PREFIX}::scheduled_for_closing"

IDLE_CUTOFF_EPOCH=$(( NOW_EPOCH - IDLE_DAYS * 86400 ))
GRACE_SECONDS=$(( GRACE_DAYS * 86400 ))

get_last_activity_epoch() {
  local iid="$1" created_at="$2"
  local latest_commit latest_note created_epoch commit_epoch note_epoch

  latest_commit="$(api "projects/$PROJECT_ID/merge_requests/$iid/commits?per_page=1" \
    | jq -r '.[0].committed_date // empty')" || return 1
  latest_note="$(api "projects/$PROJECT_ID/merge_requests/$iid/notes?sort=desc&order_by=created_at&per_page=100" \
    | jq -r --arg bot "$BOT_USERNAME" \
      '[.[] | select(.system == false and .author.username != $bot)][0].created_at // empty')" || return 1

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
      '[.[] | select(.action == "add" and .label.name == $label)] | last | .created_at // empty')" || return 1
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
  local iid author created_at has_keep has_scheduled

  iid="$(printf '%s' "$mr" | jq -r '.iid')"
  author="$(printf '%s' "$mr" | jq -r '.author.username')"
  created_at="$(printf '%s' "$mr" | jq -r '.created_at')"

  if has_label "$mr" "$EXCLUDE_LABEL"; then
    log "MR !$iid: has \"$EXCLUDE_LABEL\", left to triage-ops, skipping"
    return
  fi

  has_keep=false
  has_scheduled=false
  has_label "$mr" "$KEEP_LABEL" && has_keep=true
  has_label "$mr" "$SCHEDULED_LABEL" && has_scheduled=true

  if [ "$has_scheduled" = true ]; then
    local label_epoch activity_epoch
    label_epoch="$(get_scheduled_label_epoch "$iid")" || return 1
    activity_epoch="$(get_last_activity_epoch "$iid" "$created_at")" || return 1

    if [ "$label_epoch" -eq 0 ]; then
      log "MR !$iid: $SCHEDULED_LABEL present but no add event found, skipping"
      return
    fi

    if [ "$activity_epoch" -gt "$label_epoch" ]; then
      log "MR !$iid: activity resumed after warning, removing $SCHEDULED_LABEL"
      remove_scheduled_label "$iid" || return 1
      return
    fi

    local age=$(( NOW_EPOCH - label_epoch ))
    if [ "$age" -ge "$GRACE_SECONDS" ]; then
      log "MR !$iid: grace elapsed (${age}s >= ${GRACE_SECONDS}s), closing"
      post_comment "$iid" "$(close_comment_body)" || return 1
      close_mr "$iid" || return 1
      remove_scheduled_label "$iid" || return 1
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
  activity_epoch="$(get_last_activity_epoch "$iid" "$created_at")" || return 1
  if [ "$activity_epoch" -le "$IDLE_CUTOFF_EPOCH" ]; then
    log "MR !$iid: idle since $(date -u -d "@$activity_epoch" +%Y-%m-%d), warning"
    add_scheduled_label "$iid" || return 1
    post_comment "$iid" "$(warn_comment_body "$author")" || return 1
  else
    log "MR !$iid: active, skipping"
  fi
}

log "stale: idle=${IDLE_DAYS}d grace=${GRACE_DAYS}d prefix=$PREFIX dry_run=$DRY_RUN"
for_each_open_mr stale process_mr
