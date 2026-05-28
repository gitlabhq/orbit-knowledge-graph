#!/usr/bin/env bash
# Bump a pinned Iglu schema version.
#
# Usage: mise iglu:bump -- orbit_query 2-0-2
#
# Fetches the named schema at the given version from the live Iglu registry,
# writes it to `config/schemas/iglu/<name>/<version>.json`, and updates the
# `<name>.version` pin file. Schemas are committed alongside their pin so the
# build script has a single source of truth without vendir or runtime network.

set -euo pipefail

if [ $# -ne 2 ]; then
  echo "Usage: $0 <schema_name> <version>"
  echo "  e.g. $0 orbit_query 2-0-2"
  exit 1
fi

SCHEMA_NAME="$1"
VERSION="$2"
IGLU_BASE="https://gitlab-org.gitlab.io/iglu/schemas/com.gitlab"
VERSION_DIR="config/schemas/iglu"
VERSION_FILE="${VERSION_DIR}/${SCHEMA_NAME}.version"
SCHEMA_DIR="${VERSION_DIR}/${SCHEMA_NAME}"
SCHEMA_FILE="${SCHEMA_DIR}/${VERSION}.json"

mkdir -p "$SCHEMA_DIR"

echo "Fetching ${SCHEMA_NAME}/${VERSION} from live Iglu..."
if ! curl -sfL "${IGLU_BASE}/${SCHEMA_NAME}/jsonschema/${VERSION}" -o "$SCHEMA_FILE"; then
  echo "ERROR: ${SCHEMA_NAME}/${VERSION} not found at ${IGLU_BASE}"
  rm -f "$SCHEMA_FILE"
  exit 1
fi

python3 -c "import json,sys; json.load(open('$SCHEMA_FILE'))" || {
  echo "ERROR: fetched ${SCHEMA_FILE} is not valid JSON"
  exit 1
}

printf '%s' "$VERSION" > "$VERSION_FILE"
echo "Pinned ${SCHEMA_NAME} to ${VERSION}"
echo "  ${VERSION_FILE} -> ${VERSION}"
echo "  ${SCHEMA_FILE} written"
echo ""
echo "Next: git add ${VERSION_FILE} ${SCHEMA_FILE} && git commit"
