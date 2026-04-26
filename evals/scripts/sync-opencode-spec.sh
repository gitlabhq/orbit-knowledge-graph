#!/usr/bin/env bash
# Fetch the OpenCode OpenAPI spec matching the version pinned in mise.toml,
# then regenerate the Python SDK from it.
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

# --- fetch spec ---
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

# --- regenerate SDK ---
echo ""
echo "regenerating python client -> ${SDK_DIR}"
rm -rf "$SDK_DIR"
uvx openapi-python-client generate \
  --path "$SPEC_PATH" \
  --meta none \
  --output-path "$SDK_DIR" \
  --overwrite

echo "ok: $(find "$SDK_DIR" -name '*.py' | wc -l | tr -d ' ') python files generated"
