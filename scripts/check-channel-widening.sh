#!/usr/bin/env bash
# ADR 013 §9 widening review gate. Fails when an MR widens the resolved
# channel set of any entity's `channel_allowlist` unless the
# `channel-widening-approved` label is present in `$MR_LABELS`.
#
# The compile-time linter in gkg-server's build.rs enforces presence and
# validity (fail-closed empty allowlists panic the build); this shell
# wrapper adds the git-diff comparison against the base ref, mirroring
# how check-migration-ledger.sh layers on top of the build-time drift check.
set -euo pipefail

BASE_REF="${1:-origin/main}"
source "$(dirname "$0")/ci-skip-utils.sh"

if ci_skip_requested "channel-widening-check"; then
    echo "✅ [skip channel-widening-check] — skipping."
    exit 0
fi

cargo xtask channel-widening check --base "$BASE_REF"
