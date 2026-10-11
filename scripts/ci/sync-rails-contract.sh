#!/usr/bin/env bash
#
# Commit the Rails contract snapshot that the e2e job exported from the pinned
# GitLab to the rolling e2e pin-bump MR. DRY_RUN=true stops before pushing.

set -euo pipefail

PROJECT_PATH="${CI_PROJECT_PATH:?CI_PROJECT_PATH is required}"
SERVER_HOST="${CI_SERVER_HOST:-gitlab.com}"
TOKEN="${AUTOMATION_BOT_TOKEN:?AUTOMATION_BOT_TOKEN is required}"
DRY_RUN="${DRY_RUN:-false}"
BRANCH="automation/e2e-pin-bump"
EXPORT="e2e/diagnostics/rails-contract.json"
SNAPSHOT="config/rails-contract/master.json"

log() { printf '==> %s\n' "$*" >&2; }

[ -s "$EXPORT" ] || { log "Missing $EXPORT from the e2e job"; exit 1; }

# MR pipelines can check out a merged-results commit; build on the branch tip.
# Named remote so git never echoes the token back into the job log.
git remote set-url origin "https://oauth2:${TOKEN}@${SERVER_HOST}/${PROJECT_PATH}.git"
git fetch --quiet origin "$BRANCH"
cp "$EXPORT" /tmp/rails-contract.json
git checkout --quiet -B "$BRANCH" "origin/${BRANCH}"

if cmp -s /tmp/rails-contract.json "$SNAPSHOT"; then
  log "Rails contract snapshot is current; nothing to do."
  exit 0
fi

cp /tmp/rails-contract.json "$SNAPSHOT"
log "Rails contract changes:"
git --no-pager diff --stat -- "$SNAPSHOT" >&2

if [ "$DRY_RUN" = "true" ]; then
  log "DRY_RUN=true — not pushing."
  exit 0
fi

git config user.name "Orbit automation bot"
git config user.email "orbit-automation-bot@noreply.${SERVER_HOST}"
git add "$SNAPSHOT"
git commit -m "chore(ontology): sync the Rails contract snapshot to the pinned GitLab"
git push origin "HEAD:${BRANCH}"
