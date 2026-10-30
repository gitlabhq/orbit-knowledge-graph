#!/usr/bin/env bash
#
# Opens the gl-infra/argocd/apps MR that bumps the gkg image tag for
# orbit-stg and orbit-prd to the version of the current tag pipeline, then
# posts a review request to Slack. Runs from the release-deploy-mr CI job.
#
# Idempotent: if the deploy branch already has an open MR the script logs it
# and exits, so a retried job never opens a duplicate. Set DRY_RUN=true to
# print the diff and MR body without pushing, creating the MR, or posting.
#
# After opening the MR it closes the bot's older open deploy MRs (lower
# version, same branch pattern) with a "superseded" note and deletes their
# branches. Every step is fail-soft so the sweep never fails the release job.

set -euo pipefail

TAG="${CI_COMMIT_TAG:?CI_COMMIT_TAG is required}"
TOKEN="${AUTOMATION_BOT_TOKEN:?AUTOMATION_BOT_TOKEN is required}"
SERVER_HOST="${CI_SERVER_HOST:-gitlab.com}"
DRY_RUN="${DRY_RUN:-true}"

VERSION="${TAG#v}"
APPS_PROJECT="gitlab-com/gl-infra/argocd/apps"
SOURCE_PROJECT_URL="https://${SERVER_HOST}/${CI_PROJECT_PATH:-gitlab-org/orbit/knowledge-graph}"
DEPLOY_BRANCH="gkg-release-${TAG}"

BOT_AUTHOR="orbit-automation-bot"
APPS_PROJECT_ID="${APPS_PROJECT//\//%2F}"

log() { printf '%s\n' "$*" >&2; }

# Prints "<iid> <branch>" for each open bot deploy MR whose version is lower
# than VERSION, skipping the MR with IID $1 (empty when none was created).
list_obsolete_mrs() {
  local new_iid="$1" iid branch
  glab api --paginate "projects/${APPS_PROJECT_ID}/merge_requests?state=opened&author_username=${BOT_AUTHOR}&per_page=100" 2>/dev/null \
    | jq -r '.[] | "\(.iid) \(.source_branch)"' \
    | while read -r iid branch; do
        [[ "$iid" != "$new_iid" && "$branch" =~ ^gkg-release-v[0-9]+\.[0-9]+\.[0-9]+$ ]] || continue
        [[ "${branch#gkg-release-v}" != "$VERSION" ]] || continue
        [[ "$(printf '%s\n%s\n' "${branch#gkg-release-v}" "$VERSION" | sort -V | head -1)" == "${branch#gkg-release-v}" ]] \
          && printf '%s %s\n' "$iid" "$branch"
      done || true
}

# Closes obsolete deploy MRs and prints the closed refs ("!1 !2") to stdout.
close_obsolete_mrs() {
  local new_iid="$1" iid branch closed=()
  while read -r iid branch; do
    [[ -n "$iid" ]] || continue
    if glab api -X POST "projects/${APPS_PROJECT_ID}/merge_requests/${iid}/notes" \
        -f body="Superseded by !${new_iid} (${VERSION}). Closing automatically." >/dev/null 2>&1 \
      && glab api -X PUT "projects/${APPS_PROJECT_ID}/merge_requests/${iid}" -f state_event=close >/dev/null 2>&1; then
      closed+=("!${iid}")
      glab api -X DELETE "projects/${APPS_PROJECT_ID}/repository/branches/${branch}" >/dev/null 2>&1 \
        || log "Could not delete branch ${branch}"
    else
      log "Could not close obsolete deploy MR !${iid}; skipping"
    fi
  done < <(list_obsolete_mrs "$new_iid")
  echo "${closed[*]:-}"
}

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
  sed -i "s/^  tag: \"${current//./\\.}\"/  tag: \"${VERSION}\"/" "$values"
  [[ "$(read_tag "$values")" == "$VERSION" ]] || { log "Failed to bump ${env} to ${VERSION}"; exit 1; }
done

# The deployed tag predates the shallow CI clone, so fetch it explicitly to
# read its schema version.
new_schema="$(awk '/^schema:/ { print $2 }' config/versions.yaml)"
old_schema=""
if git fetch --quiet origin "refs/tags/v${deployed}:refs/tags/v${deployed}" --no-tags --depth=1 2>/dev/null; then
  old_schema="$(git show "v${deployed}:config/versions.yaml" 2>/dev/null | awk '/^schema:/ { print $2 }' || true)"
fi
if [[ -z "$old_schema" ]]; then
  schema_sentence="Schema impact unknown: could not read the schema pin at v${deployed}; compare manually before merging."
elif [[ "$old_schema" == "$new_schema" ]]; then
  schema_sentence="No schema migration: both are schema v${new_schema}, plain image swap."
else
  schema_sentence="Schema migration v${old_schema} to v${new_schema}: full backfill expected (hours at prod scale); merge during working hours and monitor the rollout."
fi

mr_title="chore(gkg): bump orbit-stg + orbit-prd image to ${VERSION}"
mr_description="Rolls out [Orbit ${VERSION}](${SOURCE_PROJECT_URL}/-/releases/${TAG}) to orbit-stg and orbit-prd.

${schema_sentence}

[Changelog v${deployed}...${TAG}](${SOURCE_PROJECT_URL}/-/compare/v${deployed}...${TAG})

Opened automatically by the [release tag pipeline](${CI_PIPELINE_URL:-}); see the release notes above for what ships."

if [[ "$DRY_RUN" == "true" ]]; then
  log "DRY_RUN: would push ${DEPLOY_BRANCH} with this diff:"
  git -C "$workdir/apps" --no-pager diff >&2
  log "DRY_RUN: would open MR '${mr_title}' with description:"
  log "$mr_description"
  obsolete="$(list_obsolete_mrs "")"
  log "DRY_RUN: would close obsolete deploy MRs (iid branch): ${obsolete:-none}"
  if [[ -n "${SLACK_WEBHOOK:-}" ]]; then
    log "DRY_RUN: would post the review request to Slack (SLACK_WEBHOOK is set)."
  else
    log "DRY_RUN: SLACK_WEBHOOK is NOT visible to this pipeline; the live run would skip the notification."
  fi
  exit 0
fi

git -C "$workdir/apps" config user.name "Orbit automation bot"
git -C "$workdir/apps" config user.email "orbit-automation-bot@noreply.gitlab.com"
git -C "$workdir/apps" checkout --quiet -b "$DEPLOY_BRANCH"
git -C "$workdir/apps" commit --quiet -am "$mr_title"
git -C "$workdir/apps" push --quiet origin "$DEPLOY_BRANCH"

mr_output="$(cd "$workdir/apps" && glab mr create \
  --source-branch "$DEPLOY_BRANCH" --target-branch main \
  --title "$mr_title" --description "$mr_description" --yes)"
mr_url="$(grep -o "https://[^ ]*/merge_requests/[0-9]*" <<<"$mr_output" | head -1 || true)"
[[ -n "$mr_url" ]] || { log "Could not parse the MR URL from glab output: ${mr_output}"; exit 1; }
log "Opened deploy MR: ${mr_url}"

closed_refs="$(close_obsolete_mrs "${mr_url##*/}")"
slack_suffix=""
if [[ -n "$closed_refs" ]]; then
  closed_count="$(wc -w <<<"$closed_refs")"
  slack_suffix=" Closed ${closed_count} obsolete deploy MR(s): ${closed_refs}"
  log "Closed ${closed_count} obsolete deploy MR(s): ${closed_refs}"
fi

if [[ -n "${SLACK_WEBHOOK:-}" ]]; then
  jq -n --arg text ":rocket: GKG ${TAG} deploy MR is ready for review: ${mr_url} — ${schema_sentence}${slack_suffix}" '{text: $text}' \
    | curl -fsS -X POST -H 'Content-type: application/json' -d @- "$SLACK_WEBHOOK" >/dev/null
  log "Posted review request to Slack."
else
  log "SLACK_WEBHOOK not set; skipped the Slack notification."
fi
