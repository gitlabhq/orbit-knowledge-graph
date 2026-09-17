#!/usr/bin/env bash
set -euo pipefail

# Generic vendored-dependency runner. Resolves a vendored entry from
# config/versions.yaml, exports the contract env vars, and invokes
# the entry's vendor_script or check_script with pre/postcondition
# validation.
#
# Usage:
#   scripts/vendored/run.sh vendor <name>
#   scripts/vendored/run.sh check  <name>
#   scripts/vendored/run.sh vendor --all
#   scripts/vendored/run.sh check  --all

# run.sh always lives at scripts/vendored/run.sh in the repo.
REPO_ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
VERSIONS_FILE="$REPO_ROOT/config/versions.yaml"

if [[ ! -f "$VERSIONS_FILE" ]]; then
    echo "Cannot find $VERSIONS_FILE — run this script from the repo root or scripts/vendored/" >&2
    exit 1
fi

resolve_mode() {
    case "$1" in
        vendor) SCRIPT_KEY="vendor_script" ;;
        check)  SCRIPT_KEY="check_script" ;;
        *)      echo "Unknown mode: $1 (expected vendor or check)" >&2; exit 1 ;;
    esac
}

run_one() {
    local name="$1"

    if [[ ! "$name" =~ ^[a-z0-9_-]+$ ]]; then
        echo "Invalid vendored dependency name: $name (must match [a-z0-9_-]+)" >&2
        exit 1
    fi

    local vendor_dir_rel script version
    vendor_dir_rel=$(yq ".vendored.$name.vendor_dir" "$VERSIONS_FILE")
    script=$(yq ".vendored.$name.$SCRIPT_KEY" "$VERSIONS_FILE")
    version=$(yq ".vendored.$name.version // \"\"" "$VERSIONS_FILE")

    if [[ "$vendor_dir_rel" == "null" ]]; then
        echo "No vendor_dir for vendored.$name" >&2
        exit 1
    fi
    if [[ "$script" == "null" ]]; then
        echo "No $SCRIPT_KEY for vendored.$name" >&2
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

    local pre_sha
    pre_sha=$(sha256sum "$VERSIONS_FILE" | awk '{print $1}')

    bash "$REPO_ROOT/$script"

    if [[ "$MODE" == "vendor" ]]; then
        if [[ ! -d "$VENDOR_DIR" ]] || [[ -z "$(ls -A "$VENDOR_DIR")" ]]; then
            echo "Postcondition failed: $VENDOR_DIR is empty after vendor_script" >&2
            exit 1
        fi
        yq '.' "$VERSIONS_FILE" > /dev/null || {
            echo "Postcondition failed: versions.yaml is not valid YAML after vendor_script" >&2
            exit 1
        }
    fi

    if [[ "$MODE" == "check" ]]; then
        local post_sha
        post_sha=$(sha256sum "$VERSIONS_FILE" | awk '{print $1}')
        if [[ "$pre_sha" != "$post_sha" ]]; then
            echo "Postcondition failed: check_script modified $VERSIONS_FILE (must be read-only)" >&2
            exit 1
        fi
    fi
}

run_all() {
    for name in $(yq '.vendored | keys | .[]' "$VERSIONS_FILE"); do
        local script
        script=$(yq ".vendored.$name.$SCRIPT_KEY" "$VERSIONS_FILE")
        [[ "$script" == "null" ]] && continue
        echo "=== ${MODE^} $name ==="
        run_one "$name"
    done
}

MODE="${1:?usage: scripts/vendored/run.sh <vendor|check> <name|--all>}"
TARGET="${2:?usage: scripts/vendored/run.sh <vendor|check> <name|--all>}"

resolve_mode "$MODE"

if [[ "$TARGET" == "--all" ]]; then
    run_all
else
    run_one "$TARGET"
fi
