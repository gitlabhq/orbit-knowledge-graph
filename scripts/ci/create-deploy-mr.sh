#!/usr/bin/env bash
#
# Opens the gl-infra/argocd/apps MR that bumps the gkg image tag for
# orbit-stg and orbit-prd to the version of the current tag pipeline, then
# posts a review request to Slack. Runs from the release-deploy-mr CI job.
#
# Idempotent: if the deploy branch already has an open MR the script logs it
# and exits, so a retried job never opens a duplicate. Set DRY_RUN=true to
# print the diff and MR body without pushing, creating the MR, or posting.

set -euo pipefail

TAG="${CI_COMMIT_TAG:?CI_COMMIT_TAG is required}"
TOKEN="${AUTOMATION_BOT_TOKEN:?AUTOMATION_BOT_TOKEN is required}"
SERVER_HOST="${CI_SERVER_HOST:-gitlab.com}"
DRY_RUN="${DRY_RUN:-true}"

VERSION="${TAG#v}"
APPS_PROJECT="gitlab-com/gl-infra/argocd/apps"
SOURCE_PROJECT_URL="https://${SERVER_HOST}/${CI_PROJECT_PATH:-gitlab-org/orbit/knowledge-graph}"
DEPLOY_BRANCH="gkg-release-${TAG}"

log() { printf '%s\n' "$*" >&2; }

existing_mr="$(glab mr list --repo "$APPS_PROJECT" --source-branch "$DEPLOY_BRANCH" 2>/dev/null | grep -o "https://[^ ]*/merge_requests/[0-9]*" | head -1 || true)"
if [[ -n "$existing_mr" ]]; then
  log "Deploy MR already open for ${TAG}: ${existing_mr}"
  exit 0
fi

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT
git clone --quiet --depth 1 "https://gkg-automation:${TOKEN}@${SERVER_HOST}/${APPS_PROJECT}.git" "$workdir/apps"

read_tag() { grep -m1 '^  tag: "' "$1" | sed 's/.*"\(.*\)".*/\1/'; }

deployed="$(read_tag "$workdir/apps/services/gkg/env/orbit-prd/values.yaml")"
deployed_stg="$(read_tag "$workdir/apps/services/gkg/env/orbit-stg/values.yaml")"
if [[ "$deployed" == "$VERSION" && "$deployed_stg" == "$VERSION" ]]; then
  log "Both environments already run ${VERSION}; nothing to deploy."
  exit 0
fi

for env in orbit-stg orbit-prd; do
  values="$workdir/apps/services/gkg/env/${env}/values.yaml"
  current="$(read_tag "$values")"
  sed -i "s/^  tag: \"${current}\"/  tag: \"${VERSION}\"/" "$values"
  [[ "$(read_tag "$values")" == "$VERSION" ]] || { log "Failed to bump ${env} to ${VERSION}"; exit 1; }
done

# The deployed tag predates the shallow CI clone, so fetch it explicitly to
# read its schema version.
new_schema="$(cat config/SCHEMA_VERSION)"
old_schema=""
if git fetch --quiet origin "refs/tags/v${deployed}:refs/tags/v${deployed}" --no-tags --depth=1 2>/dev/null; then
  old_schema="$(git show "v${deployed}:config/SCHEMA_VERSION" 2>/dev/null || true)"
fi
if [[ -z "$old_schema" ]]; then
  schema_sentence="Schema impact unknown: could not read config/SCHEMA_VERSION at v${deployed}; compare manually before merging."
elif [[ "$old_schema" == "$new_schema" ]]; then
  schema_sentence="No schema migration: both are schema v${new_schema}, plain image swap."
else
  schema_sentence="Schema migration v${old_schema} to v${new_schema}: full backfill expected (hours at prod scale); plan to run /monitor-rollout and merge during working hours."
fi

mr_title="chore(gkg): bump orbit-stg + orbit-prd image to ${VERSION}"
mr_description="Rolls out [Knowledge Graph ${VERSION}](${SOURCE_PROJECT_URL}/-/releases/${TAG}) to orbit-stg and orbit-prd.

${schema_sentence}

[Changelog v${deployed}...${TAG}](${SOURCE_PROJECT_URL}/-/compare/v${deployed}...${TAG})

Opened automatically by the [release tag pipeline](${CI_PIPELINE_URL:-}); see the release notes above for what ships."

if [[ "$DRY_RUN" == "true" ]]; then
  log "DRY_RUN: would push ${DEPLOY_BRANCH} with this diff:"
  git -C "$workdir/apps" --no-pager diff >&2
  log "DRY_RUN: would open MR '${mr_title}' with description:"
  log "$mr_description"
  log "DRY_RUN: would post the review request to Slack."
  exit 0
fi

git -C "$workdir/apps" config user.name "Orbit automation bot"
git -C "$workdir/apps" config user.email "orbit-automation-bot@noreply.gitlab.com"
git -C "$workdir/apps" checkout --quiet -b "$DEPLOY_BRANCH"
git -C "$workdir/apps" commit --quiet -am "$mr_title"
git -C "$workdir/apps" push --quiet origin "$DEPLOY_BRANCH"

mr_url="$(cd "$workdir/apps" && glab mr create \
  --source-branch "$DEPLOY_BRANCH" --target-branch main \
  --title "$mr_title" --description "$mr_description" --yes \
  | grep -o "https://[^ ]*/merge_requests/[0-9]*" | head -1)"
log "Opened deploy MR: ${mr_url}"

if [[ -n "${SLACK_WEBHOOK:-}" ]]; then
  jq -n --arg text ":rocket: GKG ${TAG} deploy MR is ready for review: ${mr_url} — ${schema_sentence}" '{text: $text}' \
    | curl -fsS -X POST -H 'Content-type: application/json' -d @- "$SLACK_WEBHOOK" >/dev/null
  log "Posted review request to Slack."
else
  log "SLACK_WEBHOOK not set; skipped the Slack notification."
fi
