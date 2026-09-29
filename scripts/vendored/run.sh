#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
VERSIONS_FILE="$REPO_ROOT/config/versions.yaml"

if [[ ! -f "$VERSIONS_FILE" ]]; then
    echo "Cannot find $VERSIONS_FILE — run this script from the repo root or scripts/vendored/" >&2
    exit 1
fi

run_one() {
    local name="$1"

    if [[ ! "$name" =~ ^[a-z0-9_-]+$ ]]; then
        echo "Invalid vendored dependency name: $name (must match [a-z0-9_-]+)" >&2
        exit 1
    fi

    local vendor_dir_rel script version
    vendor_dir_rel=$(yq ".vendored.$name.vendor_dir" "$VERSIONS_FILE")
    script=$(yq ".vendored.$name.vendor_script" "$VERSIONS_FILE")
    version=$(yq ".vendored.$name.version // \"\"" "$VERSIONS_FILE")

    if [[ "$vendor_dir_rel" == "null" ]]; then
        echo "No vendor_dir for vendored.$name" >&2
        exit 1
    fi
    if [[ "$script" == "null" ]]; then
        echo "No vendor_script for vendored.$name" >&2
        exit 1
    fi
    if [[ ! -x "$REPO_ROOT/$script" ]]; then
        echo "$script is not executable" >&2
        exit 1
    fi

    export VENDOR_NAME="$name"
    export VENDOR_VERSIONS_FILE="$VERSIONS_FILE"
    export VENDOR_DIR="$REPO_ROOT/$vendor_dir_rel"
    export VENDOR_VERSION="$version"

    bash "$REPO_ROOT/$script"

    if [[ ! -d "$VENDOR_DIR" ]] || [[ -z "$(ls -A "$VENDOR_DIR")" ]]; then
        echo "Postcondition failed: $VENDOR_DIR is empty after vendor_script" >&2
        exit 1
    fi
    yq '.' "$VERSIONS_FILE" > /dev/null || {
        echo "Postcondition failed: versions.yaml is not valid YAML after vendor_script" >&2
        exit 1
    }
}

run_all() {
    for name in $(yq '.vendored | keys | .[]' "$VERSIONS_FILE"); do
        local script
        script=$(yq ".vendored.$name.vendor_script" "$VERSIONS_FILE")
        [[ "$script" == "null" ]] && continue
        echo "=== Vendor $name ==="
        run_one "$name"
    done
}

MODE="${1:?usage: scripts/vendored/run.sh vendor <name|--all>}"
TARGET="${2:?usage: scripts/vendored/run.sh vendor <name|--all>}"

if [[ "$MODE" != "vendor" ]]; then
    echo "Unknown mode: $MODE (expected vendor). Use mise check:vendored -- $TARGET for checks." >&2
    exit 1
fi

if [[ "$TARGET" == "--all" ]]; then
    run_all
else
    run_one "$TARGET"
fi
