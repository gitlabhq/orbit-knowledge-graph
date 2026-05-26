#!/usr/bin/env bash
# Bump a pinned Iglu schema version.
#
# Usage: ./scripts/iglu-bump.sh orbit_query 2-0-2
#        mise iglu:bump -- orbit_query 2-0-2
#
# 1. Pulls latest iglu subtree
# 2. Verifies the version exists in the subtree
# 3. Updates the .iglu-version pin file

set -euo pipefail

if [ $# -ne 2 ]; then
  echo "Usage: $0 <schema_name> <version>"
  echo "  e.g. $0 orbit_query 2-0-2"
  exit 1
fi

SCHEMA_NAME="$1"
VERSION="$2"
SUBTREE_PREFIX="vendor/iglu"
IGLU_REMOTE="https://gitlab.com/gitlab-org/iglu.git"
VERSION_FILE="config/schemas/${SCHEMA_NAME}.iglu-version"
SCHEMA_FILE="${SUBTREE_PREFIX}/public/schemas/com.gitlab/${SCHEMA_NAME}/jsonschema/${VERSION}"

echo "Pulling latest iglu subtree..."
git subtree pull --prefix="$SUBTREE_PREFIX" "$IGLU_REMOTE" master --squash -m "chore: pull iglu subtree for ${SCHEMA_NAME} ${VERSION}"

if [ ! -f "$SCHEMA_FILE" ]; then
  echo "ERROR: ${SCHEMA_FILE} not found after subtree pull."
  echo "The version ${VERSION} may not exist in the iglu repo yet."
  exit 1
fi

printf '%s' "$VERSION" > "$VERSION_FILE"
echo "Pinned ${SCHEMA_NAME} to ${VERSION}"
echo "  ${VERSION_FILE} -> ${VERSION}"
echo "  ${SCHEMA_FILE} exists"
echo ""
echo "Next: git add ${VERSION_FILE} && git commit"
