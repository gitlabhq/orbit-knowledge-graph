#!/usr/bin/env bash
set -euo pipefail

BASE_REF="${1:-origin/main}"
source "$(dirname "$0")/ci-skip-utils.sh"

if ci_skip_requested "migration-ledger-check"; then
    echo "✅ [skip migration-ledger-check] — skipping."
    exit 0
fi

cargo xtask migration-ledger check --base "$BASE_REF"
