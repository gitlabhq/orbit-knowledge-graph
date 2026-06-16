#!/usr/bin/env bash
# Verify config/vendored/system_note_metadata.actions matches the Rails
# SystemNoteMetadata::ICON_TYPES at the commit SHA pinned in that file.
#
# The vendored list is the runtime union the Rails model exposes:
# CE `ICON_TYPES` plus EE `EE_ICON_TYPES` (icon_types is overridden in EE as
# `super + EE_ICON_TYPES`), so this check fetches both files and compares
# against their union.
#
# Fetches the Rails source from gitlab.com; requires network access.
# Skippable via [skip system-note-actions-check] in the MR description,
# MR title, or a commit message, or by setting SKIP_SYSTEM_NOTE_ACTIONS_CHECK=1.
set -euo pipefail

ACTIONS_FILE="config/vendored/system_note_metadata.actions"
CE_RAILS_PATH="app/models/system_note_metadata.rb"
EE_RAILS_PATH="ee/app/models/ee/system_note_metadata.rb"
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

# Fetch one Rails source file at the pinned SHA. Retries transient failures with
# backoff. Plain --retry skips DNS/connection-refused/timeouts, hence
# --retry-all-errors + --retry-connrefused. A transient fetch failure must not
# fail unrelated MRs, so a fetch miss is reported via a sentinel and treated as
# non-fatal by the caller; real drift still hard-fails below.
fetch_rails_src() {
    local path="$1"
    local raw_url="https://gitlab.com/${GITLAB_PROJECT}/-/raw/${pinned_sha}/${path}"
    if ! curl -sf --max-time 30 \
            --retry 4 --retry-all-errors --retry-connrefused --retry-max-time 120 \
            "$raw_url"; then
        echo "WARNING: could not fetch $raw_url after retries (non-fatal)" >&2
        echo "         Network unavailable, rate-limited, or commit SHA no longer accessible." >&2
        echo "         If this persists, verify the '# Pinned:' SHA in $ACTIONS_FILE is reachable." >&2
        return 1
    fi
}

if ! ce_src=$(fetch_rails_src "$CE_RAILS_PATH"); then
    exit 0
fi
if ! ee_src=$(fetch_rails_src "$EE_RAILS_PATH"); then
    exit 0
fi

# CE exposes ICON_TYPES; EE overrides icon_types as `super + EE_ICON_TYPES`, so
# the vendored list is the union of both constants.
upstream_actions=$(printf '%s\n%s' "$ce_src" "$ee_src" | python3 -c "
import sys, re
src = sys.stdin.read()
seen = set()
found_any = False
# \b prevents the CE 'ICON_TYPES' pattern from also matching 'EE_ICON_TYPES'.
for const in (r'\bICON_TYPES', r'\bEE_ICON_TYPES'):
    m = re.search(const + r'\s*=\s*%[wi]\[([^\]]*)\]', src, re.DOTALL)
    if not m:
        continue
    found_any = True
    for token in m.group(1).split():
        if token.startswith('#'):
            continue
        if token not in seen:
            seen.add(token)
            print(token)
if not found_any:
    print('ERROR: neither ICON_TYPES nor EE_ICON_TYPES found in Rails source', file=sys.stderr)
    sys.exit(1)
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
