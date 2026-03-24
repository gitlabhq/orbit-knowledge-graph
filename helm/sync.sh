#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "Syncing vendored chart..."
vendir sync

echo "Applying patches..."

cp patches/templates/*.yaml gkg/templates/
echo "  Copied extra templates"

yq eval-all 'select(fileIndex == 0) * select(fileIndex == 1)' \
  gkg/values.yaml patches/values-extra.yaml > gkg/values.yaml.tmp
mv gkg/values.yaml.tmp gkg/values.yaml
echo "  Merged extra values"

if [ -f patches/chart-deps.yaml ]; then
  yq eval -i '.dependencies = (.dependencies // []) + load("patches/chart-deps.yaml")' gkg/Chart.yaml
  mkdir -p gkg/charts
  cp -r charts/nats gkg/charts/nats
  cp -r charts/siphon gkg/charts/siphon
  # Allow Helm-injected 'global' and subchart condition 'enabled' in siphon schema
  if [ -f gkg/charts/siphon/values.schema.json ]; then
    yq eval -i -o json '.properties.enabled = {"type": "boolean"} | .properties.global = {"type": "object"}' \
      gkg/charts/siphon/values.schema.json
  fi
  echo "  Added subchart dependencies"
fi

echo "Done."
