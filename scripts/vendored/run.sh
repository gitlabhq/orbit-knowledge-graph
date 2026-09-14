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

MODE="${1:?usage: scripts/vendored/run.sh <vendor|check> <name>}"
NAME="${2:?usage: scripts/vendored/run.sh <vendor|check> <name>}"

REPO_ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
VERSIONS_FILE="$REPO_ROOT/config/versions.yaml"

case "$MODE" in
    vendor) SCRIPT_KEY="vendor_script" ;;
    check)  SCRIPT_KEY="check_script" ;;
    *)      echo "Unknown mode: $MODE (expected vendor or check)" >&2; exit 1 ;;
esac

VENDOR_DIR_REL=$(yq ".vendored.$NAME.vendor_dir" "$VERSIONS_FILE")
SCRIPT=$(yq ".vendored.$NAME.$SCRIPT_KEY" "$VERSIONS_FILE")
VERSION=$(yq ".vendored.$NAME.version // \"\"" "$VERSIONS_FILE")

if [[ "$SCRIPT" == "null" ]]; then
    echo "No $SCRIPT_KEY for vendored.$NAME" >&2
    exit 1
fi
if [[ ! -x "$REPO_ROOT/$SCRIPT" ]]; then
    echo "$SCRIPT is not executable" >&2
    exit 1
fi

export VENDOR_NAME="$NAME"
export VENDOR_VERSIONS_FILE="$VERSIONS_FILE"
export VENDOR_DIR="$REPO_ROOT/$VENDOR_DIR_REL"
export VENDOR_VERSION="$VERSION"

PRE_SHA=$(shasum -a 256 "$VERSIONS_FILE" | awk '{print $1}')

bash "$REPO_ROOT/$SCRIPT"

if [[ "$MODE" == "vendor" ]]; then
    if [[ ! -d "$VENDOR_DIR" ]] || [[ -z "$(ls -A "$VENDOR_DIR")" ]]; then
        echo "Postcondition failed: $VENDOR_DIR is empty after vendor_script" >&2
        exit 1
    fi
    yq '.' "$VERSIONS_FILE" > /dev/null 2>&1 || {
        echo "Postcondition failed: versions.yaml is not valid YAML after vendor_script" >&2
        exit 1
    }
fi

if [[ "$MODE" == "check" ]]; then
    POST_SHA=$(shasum -a 256 "$VERSIONS_FILE" | awk '{print $1}')
    if [[ "$PRE_SHA" != "$POST_SHA" ]]; then
        echo "Postcondition failed: check_script modified $VERSIONS_FILE (must be read-only)" >&2
        exit 1
    fi
fi
