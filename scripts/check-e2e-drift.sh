#!/bin/sh
set -e

# Detect potential e2e drift when helm-dev/gkg/ changes.
#
# Default mode: finds the last MR on origin/main that touched e2e files and
# checks if helm-dev/gkg/ has changed since then. This answers: "has infra
# drifted since we last synced e2e?"
#
# Branch mode: compares current branch against a base ref (e.g. origin/main).
# This answers: "does this branch introduce drift?"
#
# Outputs the full diff for the agent to analyze.
#
# Usage:
#   scripts/check-e2e-drift.sh              # since last e2e commit
#   scripts/check-e2e-drift.sh <base-ref>   # since merge-base with ref

# E2E files that should track helm-dev/gkg/ changes
E2E_PATHS="e2e/helm-values.yaml config/e2e.yaml crates/xtask/src/e2e/ e2e/templates/"

if [ -n "$1" ]; then
    BASE_COMMIT=$(git merge-base HEAD "$1" 2>/dev/null) || {
        echo "ERROR: Could not find merge base between HEAD and $1"
        echo "Make sure the ref is fetched: git fetch origin"
        exit 1
    }
    MODE_DESC="merge-base with $1"
else
    # Find the last MR on origin/main that touched e2e files.
    # shellcheck disable=SC2086
    BASE_COMMIT=$(git log origin/main -1 --format='%H' -- $E2E_PATHS 2>/dev/null || true)
    if [ -z "$BASE_COMMIT" ]; then
        echo "ERROR: No commits on origin/main found touching e2e files."
        echo "Try providing a base ref: scripts/check-e2e-drift.sh origin/main"
        exit 1
    fi
    MODE_DESC="last MR touching e2e on origin/main"
fi

SHORT_BASE=$(echo "$BASE_COMMIT" | cut -c1-10)
echo "E2E drift check ($MODE_DESC: $SHORT_BASE)"

# ── 1. Check for helm-dev changes since baseline ─────────────────────────────

HELM_CHANGED=$(git diff --name-only "$BASE_COMMIT" HEAD -- helm-dev/gkg/ 2>/dev/null | grep -v 'Chart.lock' || true)

if [ -z "$HELM_CHANGED" ]; then
    echo "No helm-dev/gkg/ changes since baseline — no drift."
    exit 0
fi

echo ""
echo "Changed files:"
echo "$HELM_CHANGED"

echo ""
echo "Commits:"
git log --oneline "$BASE_COMMIT"..HEAD -- helm-dev/gkg/

# ── 2. Full diff ─────────────────────────────────────────────────────────────

echo ""
echo "Full diff:"
git diff "$BASE_COMMIT" HEAD -- helm-dev/gkg/

# ── 3. E2E files to review ──────────────────────────────────────────────────

echo ""
echo "E2E files to review:"
echo "  e2e/helm-values.yaml                  <- mirrors helm-dev/gkg/values*.yaml"
echo "  config/e2e.yaml                       <- ports, secrets, labels, endpoints"
echo "  crates/xtask/src/e2e/pipeline/gkg.rs  <- helm --set overrides, secrets"
echo "  crates/xtask/src/e2e/utils.rs          <- create_k8s_secrets()"
echo "  e2e/templates/                        <- job templates"
exit 1
