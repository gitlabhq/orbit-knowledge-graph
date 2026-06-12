#!/usr/bin/env bash
# Verify config/vendored/system_note_metadata.actions matches the Rails
# SystemNoteMetadata::ICON_TYPES at the commit SHA pinned in that file.
#
# Fetches the Rails source from gitlab.com; requires network access.
# Skippable via [skip system-note-actions-check] in the MR description,
# MR title, or a commit message, or by setting SKIP_SYSTEM_NOTE_ACTIONS_CHECK=1.
set -euo pipefail

ACTIONS_FILE="config/vendored/system_note_metadata.actions"
RAILS_PATH="app/models/system_note_metadata.rb"
GITLAB_PROJECT="gitlab-org/gitlab"

source "$(dirname "$0")/ci-skip-utils.sh"

if ci_skip_requested "system-note-actions-check"; then
    echo "[skip system-note-actions-check] found — skipping."
    exit 0
fi

pinned_sha=$(grep -m1 '^# Pinned:' "$ACTIONS_FILE" | awk '{print $3}')

if [[ -z "$pinned_sha" ]]; then
    echo "Could not find '# Pinned:' line in $ACTIONS_FILE"
    exit 1
fi

echo "Checking $ACTIONS_FILE against ${GITLAB_PROJECT} @ ${pinned_sha:0:12}..."

raw_url="https://gitlab.com/${GITLAB_PROJECT}/-/raw/${pinned_sha}/${RAILS_PATH}"

# Retry transient failures with backoff. Plain --retry skips DNS/connection-
# refused/timeouts, hence --retry-all-errors + --retry-connrefused.
if ! rails_src=$(curl -sf --max-time 30 \
        --retry 4 --retry-all-errors --retry-connrefused --retry-max-time 120 \
        "$raw_url"); then
    # Transient fetch failure must not fail unrelated MRs; real drift still hard-fails below.
    echo "WARNING: could not fetch $raw_url after retries (non-fatal)"
    echo "         Network unavailable, rate-limited, or commit SHA no longer accessible."
    echo "         If this persists, verify the '# Pinned:' SHA in $ACTIONS_FILE is reachable."
    exit 0
fi

upstream_actions=$(printf '%s' "$rails_src" | python3 -c "
import sys, re
src = sys.stdin.read()
m = re.search(r'ICON_TYPES\s*=\s*%[wi]\[([^\]]*)\]', src, re.DOTALL)
if not m:
    print('ERROR: ICON_TYPES not found in Rails source', file=sys.stderr)
    sys.exit(1)
for token in m.group(1).split():
    if not token.startswith('#'):
        print(token)
") || {
    echo "Failed to parse ICON_TYPES from upstream Rails source"
    exit 1
}

if [[ -z "$upstream_actions" ]]; then
    echo "Parsed an empty ICON_TYPES, check the %w[]/%i[] regex against the Rails source"
    exit 1
fi

local_sorted=$(grep -v '^#' "$ACTIONS_FILE" | grep -v '^[[:space:]]*$' | sort)
upstream_sorted=$(echo "$upstream_actions" | sort)

if [[ "$local_sorted" == "$upstream_sorted" ]]; then
    count=$(echo "$upstream_sorted" | wc -l | tr -d ' ')
    echo "$ACTIONS_FILE matches upstream ($count actions) at ${pinned_sha:0:12}"
else
    echo "DRIFT: $ACTIONS_FILE does not match Rails ICON_TYPES at $pinned_sha"
    echo ""
    echo "Diff (< local  > upstream):"
    diff <(echo "$local_sorted") <(echo "$upstream_sorted") || true
    echo ""
    echo "To fix: update $ACTIONS_FILE to match the upstream list and"
    echo "        update the '# Pinned:' line to the new commit SHA."
    exit 1
fi
