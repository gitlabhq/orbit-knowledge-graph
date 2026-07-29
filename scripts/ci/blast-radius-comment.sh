#!/usr/bin/env bash
# Upserts the blast-radius report as a single MR note, keyed by an HTML
# marker, so re-runs update one comment instead of spamming the thread.
set -euo pipefail

REPORT_FILE="${1:?usage: blast-radius-comment.sh <report.md>}"
MARKER="<!-- orbit-blast-radius -->"

body="$(printf '%s\n\n%s' "$MARKER" "$(cat "$REPORT_FILE")")"

notes_path="projects/${CI_MERGE_REQUEST_PROJECT_ID}/merge_requests/${CI_MERGE_REQUEST_IID}/notes"
existing_id="$(glab api "${notes_path}?per_page=100" \
  | jq -r --arg m "$MARKER" '[.[] | select(.body | startswith($m))][0].id // empty')"

if [ -n "$existing_id" ]; then
  glab api -X PUT "${notes_path}/${existing_id}" -f body="$body" >/dev/null
  echo "Updated blast-radius note ${existing_id}"
else
  glab api -X POST "$notes_path" -f body="$body" >/dev/null
  echo "Created blast-radius note"
fi
