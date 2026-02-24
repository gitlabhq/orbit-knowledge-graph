#!/usr/bin/env bash
# Update the bundled Pajamas Design System SSoT references from the upstream repo.
# Run from the skill root directory: bash scripts/update-refs.sh
#
# Pulls both the docs (contents/) and design token definitions
# (packages/gitlab-ui/src/tokens/) from upstream.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SKILL_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DOCS_DIR="$SKILL_DIR/references/pajamas-docs"
TOKENS_DIR="$SKILL_DIR/references/design-tokens"
REMOTE="git@gitlab.com:gitlab-org/gitlab-services/design.gitlab.com.git"
TEMP_DIR=$(mktemp -d)

trap 'rm -rf "$TEMP_DIR"' EXIT

echo "Fetching latest Pajamas docs and tokens from upstream..."
git clone --depth 1 --filter=blob:none --sparse "$REMOTE" "$TEMP_DIR"
git -C "$TEMP_DIR" sparse-checkout set contents packages/gitlab-ui/src/tokens

UPSTREAM_TOKENS="$TEMP_DIR/packages/gitlab-ui/src/tokens"

if [ ! -d "$TEMP_DIR/contents" ]; then
  echo "Error: contents/ directory not found in the cloned repo."
  exit 1
fi

if [ ! -d "$UPSTREAM_TOKENS" ]; then
  echo "Warning: packages/gitlab-ui/src/tokens/ not found. Skipping token update."
else
  echo "Updating bundled token definitions..."
  rm -rf "$TOKENS_DIR/source"
  mkdir -p "$TOKENS_DIR/source"
  cp -R "$UPSTREAM_TOKENS/constant" "$TOKENS_DIR/source/"
  cp -R "$UPSTREAM_TOKENS/semantic" "$TOKENS_DIR/source/"
  cp -R "$UPSTREAM_TOKENS/contextual" "$TOKENS_DIR/source/"

  # Copy built CSS if available
  if [ -d "$UPSTREAM_TOKENS/build/css" ]; then
    cp "$UPSTREAM_TOKENS/build/css/tokens.css" "$TOKENS_DIR/tokens.css"
    cp "$UPSTREAM_TOKENS/build/css/tokens.dark.css" "$TOKENS_DIR/tokens.dark.css"
  else
    echo "Warning: Built CSS tokens not found. Run 'make tokens' in the upstream repo to generate them."
  fi
fi

echo "Updating bundled docs..."
rm -rf "$DOCS_DIR"
cp -R "$TEMP_DIR/contents" "$DOCS_DIR"

echo ""
echo "Done. Updated:"
echo "  Docs:   $DOCS_DIR"
echo "  Tokens: $TOKENS_DIR"
echo ""
echo "Next steps — rebuild generated indexes:"
echo "  python3 scripts/build-index.py"
echo "  python3 scripts/build-token-map.py"
