#!/usr/bin/env bash
# Bump the pinned siphon helm chart + container image to the latest published
# versions. Run manually to catch up to current siphon, or from the scheduled
# e2e-pin-bump pipeline (scripts/ci/open-e2e-bump-mr.sh). The output is
# committed so every CI run uses the same snapshot.
#
# Both pins live in e2e/config/versions.yaml under .siphon.{chart,image.tag}:
#   - chart: helmfile.yaml.gotmpl pulls siphon/siphon from the `stable` channel.
#   - image.tag: values/siphon.yaml.gotmpl renders it as the deployed image tag.
#
# The CDC config embeds output from the image's `schema generate-values`;
# sync-cdc-tables.sh regenerates it at setup.sh time, so nothing beyond
# versions.yaml is committed here.
#
# All siphon sources are public, so no token is needed.
#
# Usage:
#   e2e/scripts/bump-siphon-pins.sh
#   git diff -- e2e/config/versions.yaml

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

VERSIONS="$E2E_DIR/config/versions.yaml"

# Helm `stable` channel project publishing the siphon chart, matching the
# repository URL in helmfile.yaml.gotmpl.
CHART_PROJECT="76780115"

command -v yq >/dev/null 2>&1 || { echo "yq (mikefarah v4+) required"; exit 1; }

log "Finding latest siphon stable chart version"
CHART_VERSION=$(curl -sSL --fail \
  "https://gitlab.com/api/v4/projects/${CHART_PROJECT}/packages/helm/stable/index.yaml" \
  | yq '.entries.siphon | sort_by(.created) | reverse | .[0].version')
[[ -z "$CHART_VERSION" || "$CHART_VERSION" == "null" ]] \
  && { echo "Failed to resolve latest siphon chart version"; exit 1; }
log "  chart: $CHART_VERSION"

# The image is versioned independently of the chart (chart appVersion is null);
# tags follow `0.0.<N>-beta`. Pick the numerically highest N across all pages.
IMAGE_REPO=$(yq -r '.siphon.image.repository' "$VERSIONS")
IMAGE_PATH=${IMAGE_REPO#registry.gitlab.com/}
IMAGE_PROJECT_ENC=$(printf '%s' "$IMAGE_PATH" | sed 's#/#%2F#g')

log "Resolving registry repository id for $IMAGE_PATH"
REPO_ID=$(curl -sSL --fail \
  "https://gitlab.com/api/v4/projects/${IMAGE_PROJECT_ENC}/registry/repositories?per_page=100" \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(next((r["id"] for r in d if r["path"]==sys.argv[1]), ""))' \
  "$IMAGE_PATH")
[[ -z "$REPO_ID" ]] && { echo "Failed to resolve registry repository id for $IMAGE_PATH"; exit 1; }

log "Listing image tags (repo $REPO_ID)"
TAGS_FILE=$(mktemp)
trap 'rm -f "$TAGS_FILE"' EXIT
page=1
while :; do
  count=$(curl -sSL --fail \
    "https://gitlab.com/api/v4/projects/${IMAGE_PROJECT_ENC}/registry/repositories/${REPO_ID}/tags?per_page=100&page=${page}" \
    | python3 -c '
import json,sys
d=json.load(sys.stdin)
with open(sys.argv[1],"a") as f:
    for t in d: f.write(t["name"]+"\n")
print(len(d))
' "$TAGS_FILE")
  [[ "$count" -eq 100 ]] || break
  page=$((page+1))
done

IMAGE_TAG=$(python3 -c '
import re,sys
best=None
for line in open(sys.argv[1]):
    m=re.match(r"^0\.0\.(\d+)-beta$", line.strip())
    if m and (best is None or int(m.group(1)) > best[0]):
        best=(int(m.group(1)), line.strip())
print(best[1] if best else "")
' "$TAGS_FILE")
[[ -z "$IMAGE_TAG" ]] && { echo "No 0.0.N-beta siphon image tags found"; exit 1; }
log "  image tag: $IMAGE_TAG"

CHART_VERSION="$CHART_VERSION" IMAGE_TAG="$IMAGE_TAG" \
  yq -i '.siphon.chart = strenv(CHART_VERSION) | .siphon.image.tag = strenv(IMAGE_TAG)' "$VERSIONS"

log "Updated $VERSIONS"
log "Review with:"
log "  git diff -- e2e/config/versions.yaml"
