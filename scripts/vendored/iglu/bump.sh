#!/usr/bin/env bash
set -euo pipefail

# Fetch Iglu schema JSON files for every pin in vendored.iglu.pins.
#
# Called by `mise vendor -- iglu` which sets:
#   VENDOR_VERSIONS_FILE  — absolute path to config/versions.yaml
#   VENDOR_DIR            — absolute path to config/schemas/iglu
#   VENDOR_NAME           — "iglu"
#
# Can also be called directly; falls back to repo-relative paths.
#
# Workflow: edit a pin in versions.yaml, then run `mise vendor -- iglu`.

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)

VERSIONS_FILE="${VENDOR_VERSIONS_FILE:-$REPO_ROOT/config/versions.yaml}"
VENDOR_DIR="${VENDOR_DIR:-$REPO_ROOT/$(yq '.vendored.iglu.vendor_dir' "$VERSIONS_FILE")}"
IGLU_BASE="https://gitlab-org.gitlab.io/iglu/schemas/com.gitlab"

for name in $(yq '.vendored.iglu.pins | keys | .[]' "$VERSIONS_FILE"); do
    version=$(yq ".vendored.iglu.pins.$name" "$VERSIONS_FILE")
    schema_dir="$VENDOR_DIR/$name"
    schema_file="$schema_dir/$version.json"

    mkdir -p "$schema_dir"

    echo "Fetching $name/$version from live Iglu..."
    if ! curl -sfL "$IGLU_BASE/$name/jsonschema/$version" -o "$schema_file"; then
        echo "ERROR: $name/$version not found at $IGLU_BASE" >&2
        rm -f "$schema_file"
        exit 1
    fi

    python3 -c "import json,sys; json.load(open('$schema_file'))" || {
        echo "ERROR: fetched $schema_file is not valid JSON" >&2
        exit 1
    }

    echo "  $schema_file written"
done

echo "All Iglu schemas fetched."
