#!/usr/bin/env bash
# Bump the pinned gkg image tag to the latest release. The pin lives in
# e2e/config/versions.yaml under .gkg.image.tag; CI overrides it with an
# inline build via E2E_GKG_IMAGE/E2E_GKG_TAG, so this pin only drives local
# runs — but keeping it current means local e2e matches the deployed release.
#
# The gkg chart pin is deliberately NOT bumped here: chart-line moves can
# change the values schema and need a reviewed values update (see !2062).
#
# Usage:
#   e2e/scripts/bump-gkg-pins.sh
#   git diff -- e2e/config/versions.yaml

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
