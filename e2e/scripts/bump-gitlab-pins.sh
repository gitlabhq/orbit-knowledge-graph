#!/usr/bin/env bash

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

VERSIONS="$E2E_DIR/config/versions.yaml"
REGISTRY="registry.gitlab.com"
REPO_BASE="gitlab-org/build/cng"

if [[ -n "$E2E_GITLAB_LANE" ]]; then
  CHART_INDEX="https://charts.gitlab.io/index.yaml"
  export CHART_APP_VERSION_RE="^v${E2E_GITLAB_LANE/-/\\.}\\."
  GITLAB_BRANCH="${E2E_GITLAB_LANE}-stable-ee"
  CNG_TAG="${E2E_GITLAB_LANE}-stable"
else
  CHART_INDEX="https://gitlab.com/api/v4/projects/3828396/packages/helm/devel/index.yaml"
  export CHART_APP_VERSION_RE=""
  GITLAB_BRANCH="master"
  CNG_TAG="master"
fi

command -v yq >/dev/null 2>&1 || { echo "yq (mikefarah v4+) required"; exit 1; }

IMAGE_REPOSITORY=$(yq -r "$GITLAB_PINS.repository // \"\"" "$VERSIONS")
if [[ -n "$IMAGE_REPOSITORY" ]]; then
  log "Skipping $GITLAB_PINS: the lane is pinned by hand to $IMAGE_REPOSITORY images, so its chart, ref, and digests stay as they are"
  exit 0
fi

log "Finding latest chart version in $CHART_INDEX"
CHART_VERSION=$(curl -sSL --fail "$CHART_INDEX" \
  | yq '.entries.gitlab | map(select(.appVersion | test(strenv(CHART_APP_VERSION_RE))))
      | sort_by(.created) | reverse | .[0].version')
[[ -z "$CHART_VERSION" || "$CHART_VERSION" == "null" ]] \
  && { echo "Failed to resolve latest chart version"; exit 1; }
log "  chart: $CHART_VERSION"
yq -i "$GITLAB_PINS.chart = \"$CHART_VERSION\"" "$VERSIONS"

log "Resolving gitlab-org/gitlab $GITLAB_BRANCH -> commit SHA"
GITLAB_SHA=$(curl -sSL --fail \
  "https://gitlab.com/api/v4/projects/gitlab-org%2Fgitlab/repository/branches/$GITLAB_BRANCH" \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["commit"]["id"])')
[[ -z "$GITLAB_SHA" ]] && { echo "Failed to resolve gitlab $GITLAB_BRANCH SHA"; exit 1; }
log "  gitlab ref: $GITLAB_SHA"
yq -i "$GITLAB_PINS.ref = \"$GITLAB_SHA\"" "$VERSIONS"

"$E2E_DIR/scripts/publish-siphon-tables.sh"

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
    "https://$REGISTRY/v2/${REPO_BASE}/${image}/manifests/${CNG_TAG}" \
    | awk 'BEGIN{IGNORECASE=1} /^docker-content-digest:/ {print $2}' \
    | tr -d '\r\n'
}

resolve() {
  local image="$1"
  local digest
  digest=$(fetch_digest "$image")
  if [[ -z "$digest" || "$digest" != sha256:* ]]; then
    echo "Failed to resolve :$CNG_TAG digest for $image" >&2
    exit 1
  fi
  echo "==>   $image: $digest" >&2
  printf '%s' "$digest"
}

log "Resolving CNG image :$CNG_TAG digests"
WEBSERVICE=$(resolve gitlab-webservice-ee)
WORKHORSE=$(resolve gitlab-workhorse-ee)
SIDEKIQ=$(resolve gitlab-sidekiq-ee)
TOOLBOX=$(resolve gitlab-toolbox-ee)

yq -i "
  $GITLAB_PINS.images.webservice.digest = \"$WEBSERVICE\" |
  $GITLAB_PINS.images.workhorse.digest  = \"$WORKHORSE\"  |
  $GITLAB_PINS.images.sidekiq.digest    = \"$SIDEKIQ\" |
  $GITLAB_PINS.images.toolbox.digest    = \"$TOOLBOX\"
" "$VERSIONS"

log "Updated $VERSIONS"
log ""
log "Review with:"
log "  git diff -- e2e/config/versions.yaml"
log "Then commit:"
log "  git commit -am 'chore(e2e): bump gitlab pins'"
