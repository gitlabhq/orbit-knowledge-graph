#!/usr/bin/env bash
# Bump a pinned Iglu schema version.
#
# Usage: mise iglu:bump -- orbit_query 2-0-2
#
# 1. Runs vendir sync to pull latest schemas
# 2. Verifies the version exists in the vendored directory
# 3. Updates the .version pin file

set -euo pipefail

if [ $# -ne 2 ]; then
  echo "Usage: $0 <schema_name> <version>"
  echo "  e.g. $0 orbit_query 2-0-2"
  exit 1
fi

SCHEMA_NAME="$1"
VERSION="$2"
VERSION_FILE="config/schemas/iglu/${SCHEMA_NAME}.version"
SCHEMA_FILE="vendor/iglu/public/schemas/com.gitlab/${SCHEMA_NAME}/jsonschema/${VERSION}"

echo "Syncing vendored Iglu schemas..."
vendir sync

if [ ! -f "$SCHEMA_FILE" ]; then
  echo "ERROR: ${SCHEMA_FILE} not found after vendir sync."
  echo "The version ${VERSION} may not exist in the iglu repo yet."
  exit 1
fi

printf '%s' "$VERSION" > "$VERSION_FILE"
echo "Pinned ${SCHEMA_NAME} to ${VERSION}"
echo "  ${VERSION_FILE} -> ${VERSION}"
echo "  ${SCHEMA_FILE} exists"
echo ""
echo "Next: git add ${VERSION_FILE} vendir.lock.yml vendor/ && git commit"
