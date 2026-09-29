#!/usr/bin/env bash

set -euo pipefail

SERVER_HOST="${CI_SERVER_HOST:-gitlab.com}"
DEFAULT_BRANCH="${CI_DEFAULT_BRANCH:-main}"
DRY_RUN="${DRY_RUN:-false}"
BRANCH="automation/doc-principles-sync"
ASSIGNEE="${DOC_PRINCIPLES_ASSIGNEE:-zpainter}"
LOCAL_DIR=".ai/principles/distilled"
SOURCE_PROJECT="gitlab-org%2Fgitlab"
SOURCE_PATH=".ai/principles/distilled"
SOURCE_REF="master"
API="https://${SERVER_HOST}/api/v4"
TITLE="docs: sync documentation principles from gitlab-org/gitlab"

log() { printf '==> %s\n' "$*" >&2; }

fetch() {
  curl -sf --max-time 30 --retry 4 --retry-all-errors --retry-connrefused --retry-max-time 120 "$@"
}

log "Listing ${SOURCE_PATH} in gitlab-org/gitlab@${SOURCE_REF}"
tree_json=$(fetch "${API}/projects/${SOURCE_PROJECT}/repository/tree?path=${SOURCE_PATH}&ref=${SOURCE_REF}&per_page=100") || {
  log "WARNING: could not list the upstream directory; skipping this run."
  exit 0
}

files=$(printf '%s' "$tree_json" | jq -r '
  .[] | select(.type == "blob") | .name | select(startswith("documentation") and endswith(".md"))
')

if [ -z "$files" ]; then
  log "WARNING: no documentation principle files found upstream; skipping this run."
  exit 0
fi

mkdir -p "$LOCAL_DIR"

# Compared file by file rather than through git, so the run is correct whether
# or not the target files are already tracked.
changed=()

for name in $files; do
  encoded="${SOURCE_PATH//\//%2F}%2F${name}"
  target="${LOCAL_DIR}/${name}"
  if ! fetch -o "${target}.tmp" "${API}/projects/${SOURCE_PROJECT}/repository/files/${encoded}/raw?ref=${SOURCE_REF}"; then
    log "WARNING: could not fetch ${name}; skipping this run."
    rm -f "${target}.tmp"
    exit 0
  fi
  if [ -f "$target" ] && cmp -s "${target}.tmp" "$target"; then
    rm -f "${target}.tmp"
    continue
  fi

  mv "${target}.tmp" "$target"
  changed+=("$name")
done

if [ ${#changed[@]} -eq 0 ]; then
  log "No upstream changes; nothing to do."
  exit 0
fi

log "Updated: ${changed[*]}"
git --no-pager diff --stat -- "$LOCAL_DIR" >&2 || true

if [ "$DRY_RUN" = "true" ]; then
  log "DRY_RUN=true — not pushing or opening an MR."
  exit 0
fi

PROJECT_ID="${CI_PROJECT_ID:?CI_PROJECT_ID is required}"
PROJECT_PATH="${CI_PROJECT_PATH:?CI_PROJECT_PATH is required}"
TOKEN="${AUTOMATION_BOT_TOKEN:?AUTOMATION_BOT_TOKEN is required}"

git config user.name "Orbit automation bot"
git config user.email "orbit-automation-bot@noreply.${SERVER_HOST}"

git checkout -B "$BRANCH"
git add "$LOCAL_DIR"
git commit -m "$TITLE"

# Named remote so git never echoes the token back into the job log.
git remote set-url origin "https://oauth2:${TOKEN}@${SERVER_HOST}/${PROJECT_PATH}.git"
git push --force origin "HEAD:${BRANCH}"

existing=$(glab api \
  "projects/${PROJECT_ID}/merge_requests?source_branch=${BRANCH}&target_branch=${DEFAULT_BRANCH}&state=opened" \
  | jq -r '.[0].web_url // empty')

if [ -n "$existing" ]; then
  log "Refreshed existing MR: $existing"
  exit 0
fi

# Quick actions silently ignore unknown usernames.
assign_line=""
if glab api "users?username=${ASSIGNEE}" \
  | jq -e 'length > 0' >/dev/null; then
  assign_line="/assign ${ASSIGNEE}"
else
  log "WARNING: assignee '${ASSIGNEE}' not found; opening the MR unassigned."
fi

# No apostrophes in this body: bash 3.2, which is what macOS ships, scans a
# command substitution for its closing paren before it handles the heredoc,
# so a lone quote in here fails `bash -n` on a contributor machine.
body="$(cat <<EOF
### What does this MR do and why?

The GitLab documentation principles changed upstream, and this repository keeps a copy of them so contributors and agents can read the standard without leaving the project. The scheduled sync job refetched them and opened this MR with the new text.

### Related Issues

None; recurring automated sync.

### Testing

The files are synced verbatim and are exempt from the prose linters here, so review is a read of the upstream diff. The rest of the pipeline runs on this MR as usual.

### Performance Analysis

- [x] This merge request does not introduce any performance regression.

${assign_line}
/label ~"group::context-systems" ~"Category:Orbit"
/label ~"type::maintenance"
/label ~documentation
EOF
)"

glab mr create \
  --source-branch "$BRANCH" \
  --target-branch "$DEFAULT_BRANCH" \
  --title "$TITLE" \
  --description "$body" \
  --yes >&2

log "Opened new doc-principles sync MR."
