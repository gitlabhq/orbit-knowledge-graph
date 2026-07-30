#!/usr/bin/env bash
# Verify the migration ledger and fingerprint snapshot are in sync with the
# ontology, and that a bumped fingerprint carries a scope-covering ledger entry.
# The build-time check in gkg-server enforces drift + ledger shape without a
# base ref; this adds the git-diff under-declaration guard on merge requests.
set -euo pipefail

BASE_REF="${1:-origin/main}"
source "$(dirname "$0")/ci-skip-utils.sh"

if ci_skip_requested "migration-ledger-check"; then
    echo "✅ [skip migration-ledger-check] — skipping."
    exit 0
fi

cargo xtask migration-ledger check --base "$BASE_REF"
