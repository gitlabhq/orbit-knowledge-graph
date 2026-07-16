#!/usr/bin/env bash
#
# Open (or refresh) the rolling e2e pin-bump MR. Runs from a scheduled
# pipeline (.gitlab/ci/e2e-pin-bump.yml); DRY_RUN=true logs the diff and
# stops before pushing.

set -euo pipefail

PROJECT_ID="${CI_PROJECT_ID:?CI_PROJECT_ID is required}"
PROJECT_PATH="${CI_PROJECT_PATH:?CI_PROJECT_PATH is required}"
SERVER_HOST="${CI_SERVER_HOST:-gitlab.com}"
DEFAULT_BRANCH="${CI_DEFAULT_BRANCH:-main}"
TOKEN="${AUTOMATION_BOT_TOKEN:?AUTOMATION_BOT_TOKEN is required}"
DRY_RUN="${DRY_RUN:-false}"
BRANCH="automation/e2e-pin-bump"
ASSIGNEE="${E2E_BUMP_ASSIGNEE:-michaelangeloio}"
VERSIONS="e2e/config/versions.yaml"

log() { printf '==> %s\n' "$*" >&2; }

log "Bumping pins"
bash e2e/scripts/bump-gitlab-pins.sh
bash e2e/scripts/bump-siphon-pins.sh
bash e2e/scripts/bump-gkg-pins.sh

if git diff --quiet -- "$VERSIONS"; then
  log "No pin changes; nothing to do."
  exit 0
fi

log "Pin changes:"
git --no-pager diff -- "$VERSIONS" >&2

if [ "$DRY_RUN" = "true" ]; then
  log "DRY_RUN=true — not pushing or opening an MR."
  exit 0
fi

git config user.name "Orbit automation bot"
git config user.email "orbit-automation-bot@noreply.${SERVER_HOST}"

git checkout -B "$BRANCH"
git add "$VERSIONS"
git commit -m "chore(e2e): auto-bump siphon, gitlab, and gkg pins to current"

# Named remote so git never echoes the token back into the job log.
git remote set-url origin "https://oauth2:${TOKEN}@${SERVER_HOST}/${PROJECT_PATH}.git"
git push --force origin "HEAD:${BRANCH}"

existing=$(glab api \
  "projects/${PROJECT_ID}/merge_requests?source_branch=${BRANCH}&target_branch=${DEFAULT_BRANCH}&state=opened" \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d[0]["web_url"] if d else "")')

if [ -n "$existing" ]; then
  log "Refreshed existing MR: $existing"
  exit 0
fi

# Quick actions silently ignore unknown usernames.
assign_line=""
if glab api "users?username=${ASSIGNEE}" \
  | python3 -c 'import json,sys; sys.exit(0 if json.load(sys.stdin) else 1)'; then
  assign_line="/assign ${ASSIGNEE}"
else
  log "WARNING: assignee '${ASSIGNEE}' not found; opening the MR unassigned."
fi

body="$(cat <<EOF
### What does this MR do and why?

Automated bump of the e2e harness pins (siphon chart + image, GitLab devel chart + ref + CNG image digests, gkg image) to the latest upstream builds, opened by the scheduled \`e2e-pin-bump\` pipeline. Keeping these current stops the e2e stack from drifting behind the versions we ship against.

### Related Issues

None; recurring automated bump.

### Testing

The \`e2e\` job runs automatically on the pipeline of this MR and must be green before merging. The CDC config is regenerated from these pins at deploy time.

### Performance Analysis

- [x] This merge request does not introduce any performance regression.

${assign_line}
/label ~"group::context-systems" ~"Category:Orbit"
/label ~"type::maintenance"
EOF
)"

glab mr create \
  --source-branch "$BRANCH" \
  --target-branch "$DEFAULT_BRANCH" \
  --title "chore(e2e): auto-bump siphon, gitlab, and gkg pins to current" \
  --description "$body" \
  --yes >&2

log "Opened new pin-bump MR."
