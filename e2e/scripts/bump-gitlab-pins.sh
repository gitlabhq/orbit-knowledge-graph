#!/usr/bin/env bash
# Bump the pinned gitlab devel chart + gitlab-org/gitlab ref + CNG image
# digests to the latest master build available right now. Run manually when
# you want to catch up to current devel; the output is committed so every CI
# run uses the same snapshot (no registry lookups at deploy time).
#
# All pins live in e2e/config/versions.yaml under .gitlab.{chart,ref,images}.
# values/gitlab.yaml.gotmpl renders the digests as "master@<digest>" tags at
# helmfile sync time.
#
# Pins only the Rails-bearing CNG images (webservice, sidekiq, toolbox).
# The schema-version race lives in those images' dependency init check;
# other images (workhorse, shell, gitaly) don't touch schema_migrations.
#
# Usage:
#   e2e/scripts/bump-gitlab-pins.sh
#   git diff -- e2e/config/versions.yaml
#   git commit -am 'chore(e2e): bump gitlab pins'

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

VERSIONS="$E2E_DIR/config/versions.yaml"
REGISTRY="registry.gitlab.com"
REPO_BASE="gitlab-org/build/cng"

command -v yq >/dev/null 2>&1 || { echo "yq (mikefarah v4+) required"; exit 1; }

# --- Latest devel chart version ------------------------------------------
# The devel repo publishes a fresh chart every few hours tagged with the
# source pipeline ID. We pick the one with the newest `created` timestamp.
log "Finding latest devel chart version"
CHART_VERSION=$(curl -sSL --fail \
  "https://gitlab.com/api/v4/projects/3828396/packages/helm/devel/index.yaml" \
  | yq '.entries.gitlab | sort_by(.created) | reverse | .[0].version')
[[ -z "$CHART_VERSION" || "$CHART_VERSION" == "null" ]] \
  && { echo "Failed to resolve latest devel chart version"; exit 1; }
log "  chart: $CHART_VERSION"
yq -i ".gitlab.chart = \"$CHART_VERSION\"" "$VERSIONS"

# --- Pin gitlab-org/gitlab ref to current master HEAD --------------------
# Siphon table definitions come from db/siphon/tables/*.yml in gitlab-org/
# gitlab. Pinning to a specific SHA (not "master") makes sync-cdc-tables.sh
# fetch a stable snapshot that aligns with the pinned images.
log "Resolving gitlab-org/gitlab master -> commit SHA"
GITLAB_SHA=$(curl -sSL --fail \
  "https://gitlab.com/api/v4/projects/gitlab-org%2Fgitlab/repository/branches/master" \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["commit"]["id"])')
[[ -z "$GITLAB_SHA" ]] && { echo "Failed to resolve gitlab master SHA"; exit 1; }
log "  gitlab ref: $GITLAB_SHA"
yq -i ".gitlab.ref = \"$GITLAB_SHA\"" "$VERSIONS"

# --- Resolve :master manifest digests for CNG Rails images ---------------
fetch_digest() {
  local image="$1"
  local token
  token=$(curl -sSL --fail \
    "https://gitlab.com/jwt/auth?service=container_registry&scope=repository:${REPO_BASE}/${image}:pull" \
    | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])')
  curl -sSI --fail \
    -H "Authorization: Bearer $token" \
    -H "Accept: application/vnd.docker.distribution.manifest.list.v2+json" \
    -H "Accept: application/vnd.oci.image.index.v1+json" \
    -H "Accept: application/vnd.docker.distribution.manifest.v2+json" \
    "https://$REGISTRY/v2/${REPO_BASE}/${image}/manifests/master" \
    | awk 'BEGIN{IGNORECASE=1} /^docker-content-digest:/ {print $2}' \
    | tr -d '\r\n'
}

resolve() {
  local image="$1"
  local digest
  digest=$(fetch_digest "$image")
  if [[ -z "$digest" || "$digest" != sha256:* ]]; then
    echo "Failed to resolve :master digest for $image" >&2
    exit 1
  fi
  echo "==>   $image: $digest" >&2
  printf '%s' "$digest"
}

log "Resolving CNG image :master digests"
WEBSERVICE=$(resolve gitlab-webservice-ee)
SIDEKIQ=$(resolve gitlab-sidekiq-ee)
TOOLBOX=$(resolve gitlab-toolbox-ee)

yq -i "
  .gitlab.images.webservice.digest = \"$WEBSERVICE\" |
  .gitlab.images.sidekiq.digest    = \"$SIDEKIQ\" |
  .gitlab.images.toolbox.digest    = \"$TOOLBOX\"
" "$VERSIONS"

log "Updated $VERSIONS"
log ""
log "Review with:"
log "  git diff -- e2e/config/versions.yaml"
log "Then commit:"
log "  git commit -am 'chore(e2e): bump gitlab pins'"
