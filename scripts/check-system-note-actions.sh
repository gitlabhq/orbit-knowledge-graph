#!/usr/bin/env bash
# Verify that the vendored copy of Rails `SystemNoteMetadata::ICON_TYPES`
# (crates/indexer/src/modules/sdlc/handler/system_notes/vendored/icon_types.rs)
# still matches the upstream source-of-truth in
# `gitlab-org/gitlab/app/models/system_note_metadata.rb`.
#
# Modelled on `scripts/check-goon-format-version.sh` and the broader
# vendored-constant + CI drift-check pattern from ADR 012 (GOON format).
#
# Failure modes:
#
#   - Upstream gains a new ICON_TYPES value not present in the vendored
#     list: the action would be silently dropped by the runtime
#     `log_and_drop` path, producing a `gkg.indexer.sdlc.system_notes.\
#     unknown_action_total{action="…"}` counter increment in production.
#     The drift check fails so the contributor adds the new action and
#     decides whether the parser dispatcher needs an arm for it.
#
#   - Upstream removes an ICON_TYPES value still present in the vendored
#     list: lower priority — the vendored entry becomes dead code but does
#     not cause incorrect emission. We still fail to keep the lists in
#     sync.
#
# Skip mechanism (matches GOON / RAW patterns):
#   - `[skip system-note-actions-check]` in MR description, OR
#   - `SKIP_SYSTEM_NOTE_ACTIONS_CHECK=1` env var.
#
# Offline behaviour: the upstream-fetch step uses `curl --fail || exit 0`,
# so a contributor with no network (or a CI runner with egress restrictions)
# gets a silent no-op rather than a failed commit. The CI job runs on
# GitLab.com runners that always have egress to gitlab.com, so production
# drift detection is not affected.

set -euo pipefail

UPSTREAM_URL="https://gitlab.com/gitlab-org/gitlab/-/raw/master/app/models/system_note_metadata.rb"
VENDORED="crates/indexer/src/modules/sdlc/handler/system_notes/vendored/icon_types.rs"

skip_requested() {
    [[ "${SKIP_SYSTEM_NOTE_ACTIONS_CHECK:-}" == "1" ]] && return 0
    local mr_desc
    mr_desc="${CI_MERGE_REQUEST_DESCRIPTION:-}"
    [[ "$mr_desc" == *"[skip system-note-actions-check]"* ]]
}

if skip_requested; then
    echo "✅ [skip system-note-actions-check] found — skipping drift check."
    exit 0
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

if ! curl --silent --show-error --fail --max-time 10 "$UPSTREAM_URL" \
    > "$tmpdir/upstream.rb"; then
    echo "⚠️  Could not fetch upstream system_note_metadata.rb from $UPSTREAM_URL."
    echo "   This is a non-fatal warning in CI (network access may be restricted)."
    echo "   The drift check is also enforced in lefthook pre-commit; if you can"
    echo "   reach gitlab.com locally, run \`bash scripts/check-system-note-actions.sh\`."
    exit 0
fi

# Extract the array literal between `ICON_TYPES = %w[` and `].freeze`.
# Use awk for portability (sed -nE on macOS vs GNU has surprises).
awk '
    /ICON_TYPES *= *%w\[/ { capture = 1; next }
    capture && /\]\.freeze/ { exit }
    capture { print }
' "$tmpdir/upstream.rb" \
    | tr -s '[:space:]' '\n' \
    | grep -v '^$' \
    | sort -u \
    > "$tmpdir/upstream.list"

# Extract quoted strings out of the vendored `ICON_TYPES` const. Anchored
# on the const name so we only pick up the all-actions list, not the
# `HANDLED_*` subsets below it.
awk '
    /pub const ICON_TYPES: *&\[&str\] *= *\&\[/ { capture = 1; next }
    capture && /^\];/ { exit }
    capture { print }
' "$VENDORED" \
    | grep -oE '"[^"]+"' \
    | tr -d '"' \
    | sort -u \
    > "$tmpdir/vendored.list"

if diff -u "$tmpdir/vendored.list" "$tmpdir/upstream.list" > "$tmpdir/diff"; then
    echo "✅ ICON_TYPES vendored copy matches upstream."
    exit 0
fi

echo "❌ Vendored ICON_TYPES is out of sync with upstream Rails."
echo ""
echo "   Diff (- vendored, + upstream):"
echo ""
cat "$tmpdir/diff"
echo ""
echo "   Fix: update $VENDORED to match upstream, then re-pin the SHA in the"
echo "   module-level docstring."
exit 1
