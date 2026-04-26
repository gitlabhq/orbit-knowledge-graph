#!/usr/bin/env bash
# Fetch the OpenCode OpenAPI spec matching the version pinned in mise.toml.
# The spec is committed to the repo so codegen is reproducible offline.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
EVALS_DIR="$(dirname "$SCRIPT_DIR")"
SPEC_PATH="${EVALS_DIR}/opencode-openapi.json"

VERSION=$(mise ls opencode --json | jq -r '.[0].version')
if [ -z "$VERSION" ] || [ "$VERSION" = "null" ]; then
  echo "error: could not read opencode version from mise" >&2
  exit 1
fi

URL="https://raw.githubusercontent.com/anomalyco/opencode/v${VERSION}/packages/sdk/openapi.json"

echo "fetching opencode openapi spec v${VERSION}"
echo "  url: ${URL}"
echo "  out: ${SPEC_PATH}"

HTTP_CODE=$(curl -fsSL -w '%{http_code}' -o "$SPEC_PATH" "$URL")
if [ "$HTTP_CODE" != "200" ]; then
  echo "error: failed to fetch spec (HTTP ${HTTP_CODE})" >&2
  echo "  the tag v${VERSION} may not have packages/sdk/openapi.json" >&2
  rm -f "$SPEC_PATH"
  exit 1
fi

echo "ok: $(wc -c < "$SPEC_PATH" | tr -d ' ') bytes written"
