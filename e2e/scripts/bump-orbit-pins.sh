#!/usr/bin/env bash
# Bump the pinned gkg image tag in e2e/config/versions.yaml to the latest
# release. The chart pin stays manual: chart-line moves can change the values
# schema and need a reviewed values update (see !2062).

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

VERSIONS="$E2E_DIR/config/versions.yaml"
GKG_PROJECT="77960826"

command -v yq >/dev/null 2>&1 || { echo "yq (mikefarah v4+) required"; exit 1; }

log "Finding latest gkg release"
RELEASE_TAG=$(curl -sSL --fail \
  "https://gitlab.com/api/v4/projects/${GKG_PROJECT}/releases?per_page=1" \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d[0]["tag_name"] if d else "")')
[[ "$RELEASE_TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] \
  || { echo "Unexpected gkg release tag: '$RELEASE_TAG'"; exit 1; }
IMAGE_TAG="${RELEASE_TAG#v}"
log "  image tag: $IMAGE_TAG"

IMAGE_TAG="$IMAGE_TAG" yq -i '.gkg.image.tag = strenv(IMAGE_TAG)' "$VERSIONS"

log "Updated $VERSIONS"
log "Review with:"
log "  git diff -- e2e/config/versions.yaml"
