#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

VERSIONS="$E2E_DIR/config/versions.yaml"
GITLAB_REPO="https://gitlab.com/gitlab-org/gitlab.git"
TABLES_PATH="db/siphon/tables"
PKG_PROJECT="${CI_PROJECT_ID:-gitlab-org%2Forbit%2Fknowledge-graph}"

command -v yq >/dev/null 2>&1 || { echo "yq (mikefarah v4+) required"; exit 1; }

GITLAB_REF="$(yq -r '.gitlab.ref' "$VERSIONS")"
[[ -z "$GITLAB_REF" || "$GITLAB_REF" == "null" ]] \
  && { echo "gitlab.ref missing in $VERSIONS"; exit 1; }

PKG_PATH="packages/generic/siphon-ssot-tables/${GITLAB_REF}/tables.tar.gz"
PKG_URL="https://gitlab.com/api/v4/projects/${PKG_PROJECT}/${PKG_PATH}"

if curl -sfIL -o /dev/null "$PKG_URL"; then
  log "artifact already published for $GITLAB_REF; nothing to do"
  exit 0
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
CLONE="$TMP/gitlab"
TARBALL="$TMP/tables.tar.gz"

log "sparse-fetching $TABLES_PATH from gitlab-org/gitlab @ $GITLAB_REF"
git init -q "$CLONE"
git -C "$CLONE" remote add origin "$GITLAB_REPO"
git -C "$CLONE" sparse-checkout set --no-cone "$TABLES_PATH"

fetched=false
for attempt in 1 2 3 4 5; do
  if git -C "$CLONE" fetch -q --depth 1 --filter=blob:none origin "$GITLAB_REF" \
    && git -C "$CLONE" checkout -q FETCH_HEAD; then
    fetched=true
    break
  fi
  [[ "$attempt" -lt 5 ]] || break
  log "fetch attempt $attempt failed; retrying in 30s"
  sleep 30
done
[[ "$fetched" == true ]] || { echo "failed to fetch $GITLAB_REF after 5 attempts"; exit 1; }

count="$(find "$CLONE/$TABLES_PATH" -maxdepth 1 \( -name '*.yml' -o -name '*.yaml' \) | wc -l | tr -d ' ')"
[[ "$count" -gt 0 ]] || { echo "no table files found at $TABLES_PATH in $GITLAB_REF"; exit 1; }
tar -czf "$TARBALL" -C "$CLONE/$TABLES_PATH" .
log "packed $count table files ($(du -h "$TARBALL" | cut -f1 | tr -d ' '))"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  log "DRY_RUN=true — not publishing $PKG_URL"
  exit 0
fi

log "publishing $PKG_URL"
if [[ -n "${CI_JOB_TOKEN:-}" ]]; then
  curl -sfL --header "JOB-TOKEN: ${CI_JOB_TOKEN}" --upload-file "$TARBALL" "$PKG_URL" >/dev/null
else
  command -v glab >/dev/null 2>&1 || { echo "glab required for local publish"; exit 1; }
  glab api --method PUT "projects/${PKG_PROJECT}/${PKG_PATH}" --input "$TARBALL" >/dev/null
fi

curl -sfIL -o /dev/null "$PKG_URL" || { echo "artifact not downloadable after publish"; exit 1; }
log "published siphon-ssot-tables @ $GITLAB_REF"
