#!/usr/bin/env bash
set -euo pipefail

# Run vendor_script or check_script for every vendored entry that has one.
#
# Usage:
#   scripts/vendored/run-all.sh vendor
#   scripts/vendored/run-all.sh check

MODE="${1:?usage: scripts/vendored/run-all.sh <vendor|check>}"

REPO_ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
VERSIONS_FILE="$REPO_ROOT/config/versions.yaml"

case "$MODE" in
    vendor) SCRIPT_KEY="vendor_script" ;;
    check)  SCRIPT_KEY="check_script" ;;
    *)      echo "Unknown mode: $MODE (expected vendor or check)" >&2; exit 1 ;;
esac

for NAME in $(yq '.vendored | keys | .[]' "$VERSIONS_FILE"); do
    SCRIPT=$(yq ".vendored.$NAME.$SCRIPT_KEY" "$VERSIONS_FILE")
    [[ "$SCRIPT" == "null" ]] && continue
    echo "=== ${MODE^} $NAME ==="
    "$REPO_ROOT/scripts/vendored/run.sh" "$MODE" "$NAME"
done
