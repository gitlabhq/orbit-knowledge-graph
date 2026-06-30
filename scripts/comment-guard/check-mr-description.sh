#!/usr/bin/env bash
# MR-description headline gate.
#
# Scores the headline section ("What does this MR do and why?") of the MR
# description using score_description.py (co-located; research-tuned: word_cap=100,
# span_cap=3, bare_idents<=3 with the v2 acronym/ref-aware regex).
#
# Sources the description from the predefined CI variable
# CI_MERGE_REQUEST_DESCRIPTION (set on merge_request_event pipelines) — no
# API token or glab needed. Follows the same pattern as check-mr-title.sh.
#
# Exit codes: non-zero when the description FAILS the scorer; zero on PASS or
# when there is nothing to check (not an MR pipeline, empty description).
# Blocking-ness is controlled by CI `allow_failure: true` (one-line change to
# promote), not inside this script.
set -uo pipefail

skip() {
    echo "ℹ️  mr-description lint: $1 — skipping (no-op, exit 0)."
    exit 0
}

# Not a merge_request pipeline — nothing to check.
[ -n "${CI_MERGE_REQUEST_IID:-}" ] || skip "not a merge request pipeline"

DESC="${CI_MERGE_REQUEST_DESCRIPTION:-}"
[ -n "$DESC" ] || skip "MR !${CI_MERGE_REQUEST_IID} has an empty description"

# Truncation guard: CI_MERGE_REQUEST_DESCRIPTION is capped at 2700 chars.
# The headline section is at the top and almost always fits, but if the text
# was truncated and the headline boundary markers (next ### heading or
# <details> block) were cut off, the scorer would score non-headline content.
# Detect this: truncated + no section boundary after the headline heading.
if [ "${CI_MERGE_REQUEST_DESCRIPTION_IS_TRUNCATED:-}" = "true" ]; then
    has_boundary=""
    # Check for <details (Agent context block) or a second ### heading.
    if printf '%s' "$DESC" | grep -q '<details'; then
        has_boundary=1
    elif [ "$(printf '%s\n' "$DESC" | grep -cE '^### ')" -ge 2 ]; then
        has_boundary=1
    fi
    if [ -z "$has_boundary" ]; then
        echo "ℹ️  mr-description lint: description was truncated at 2700 chars and the"
        echo "   headline section boundary is missing — cannot score reliably."
        echo "   (The headline is likely longer than the truncation window.)"
        exit 0
    fi
fi

TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT
printf '%s' "$DESC" > "$TMP"

echo "MR !${CI_MERGE_REQUEST_IID} description headline check:"
# Capture the scorer's exit code; command substitution does not trigger
# pipefail, so $? is the scorer's real exit.
scorer_out="$(python3 "$(dirname "$0")/score_description.py" "$TMP")"
scorer_rc=$?
printf '%s\n' "$scorer_out" | sed 's/^/  /'
echo ""
echo "⚠️  Limits: <=100 words, <=3 inline-code spans, <=3 bare identifiers in the"
echo "   headline section. Long-form mechanics belong in the Agent context"
echo "   <details> block."

exit "$scorer_rc"
