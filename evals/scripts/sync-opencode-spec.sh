#!/usr/bin/env bash
# Fetch the OpenAPI spec for the mise-pinned opencode version,
# then regenerate the Python SDK from it.
#
# The spec is fetched from the opencode GitHub repo at the matching
# version tag. This is the same spec used to generate the official
# JS SDK (packages/sdk/openapi.json).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
EVALS_DIR="$(dirname "$SCRIPT_DIR")"
SPEC_PATH="${EVALS_DIR}/opencode-openapi.json"
SDK_DIR="${EVALS_DIR}/opencode_sdk/opencode_sdk"

VERSION=$(mise ls opencode --json | jq -r '.[0].version')
if [ -z "$VERSION" ] || [ "$VERSION" = "null" ]; then
  echo "error: could not read opencode version from mise" >&2
  exit 1
fi

# --- fetch spec from GitHub at the pinned version tag ---
URL="https://raw.githubusercontent.com/anomalyco/opencode/v${VERSION}/packages/sdk/openapi.json"

echo "fetching opencode openapi spec v${VERSION}"
echo "  from: ${URL}"

HTTP_CODE=$(curl -fsSL -w '%{http_code}' -o "$SPEC_PATH" "$URL")
if [ "$HTTP_CODE" != "200" ]; then
  echo "error: failed to fetch spec (HTTP ${HTTP_CODE})" >&2
  echo "  tag v${VERSION} may not have packages/sdk/openapi.json" >&2
  rm -f "$SPEC_PATH"
  exit 1
fi

echo "ok: $(wc -c < "$SPEC_PATH" | tr -d ' ') bytes"

# --- regenerate SDK ---
echo "regenerating python client -> ${SDK_DIR}"
rm -rf "$SDK_DIR"
uvx openapi-python-client generate \
  --path "$SPEC_PATH" \
  --meta none \
  --output-path "$SDK_DIR" \
  --overwrite

echo "ok: $(find "$SDK_DIR" -name '*.py' | wc -l | tr -d ' ') python files generated"
