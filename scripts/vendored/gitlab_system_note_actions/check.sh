#!/usr/bin/env bash
set -euo pipefail

VERSIONS_FILE="${VENDOR_VERSIONS_FILE:?Set VENDOR_VERSIONS_FILE or call via scripts/vendored/run.sh}"
VENDOR_DIR="${VENDOR_DIR:?Set VENDOR_DIR or call via scripts/vendored/run.sh}"
pinned_sha="${VENDOR_VERSION:?Set VENDOR_VERSION or call via scripts/vendored/run.sh}"

ACTIONS_FILE="$VENDOR_DIR/system_note_metadata.actions"
CE_RAILS_PATH="app/models/system_note_metadata.rb"
EE_RAILS_PATH="ee/app/models/ee/system_note_metadata.rb"
GITLAB_PROJECT="gitlab-org/gitlab"

if python "$(dirname "$VERSIONS_FILE")/../ci/skip_check.py" system-note-actions-check; then
    echo "[skip system-note-actions-check] found — skipping."
    exit 0
fi

if [[ -z "$pinned_sha" || "$pinned_sha" == "null" ]]; then
    echo "Could not find gitlab_system_note_actions version in config/versions.yaml" >&2
    exit 1
fi

echo "Checking $ACTIONS_FILE against ${GITLAB_PROJECT} @ ${pinned_sha:0:12}..."

fetch_rails_src() {
    local path="$1"
    local raw_url="https://gitlab.com/${GITLAB_PROJECT}/-/raw/${pinned_sha}/${path}"
    if ! curl -sf --max-time 30 \
            --retry 4 --retry-all-errors --retry-connrefused --retry-max-time 120 \
            "$raw_url"; then
        echo "WARNING: could not fetch $raw_url after retries (non-fatal)" >&2
        echo "         Network unavailable, rate-limited, or commit SHA no longer accessible." >&2
        return 1
    fi
}

if ! ce_src=$(fetch_rails_src "$CE_RAILS_PATH"); then
    exit 0
fi
if ! ee_src=$(fetch_rails_src "$EE_RAILS_PATH"); then
    exit 0
fi

upstream_actions=$(printf '%s\n%s' "$ce_src" "$ee_src" | python3 -c "
import sys, re
src = sys.stdin.read()
seen = set()
found_any = False
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
    echo "        bump vendored.gitlab_system_note_actions.version in config/versions.yaml."
    exit 1
fi
