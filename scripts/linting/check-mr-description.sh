#!/usr/bin/env bash
set -uo pipefail

skip() {
    echo "ℹ️  mr-description lint: $1 — skipping (no-op, exit 0)."
    exit 0
}

[ -n "${CI_MERGE_REQUEST_IID:-}" ] || skip "not a merge request pipeline"

DESC="${CI_MERGE_REQUEST_DESCRIPTION:-}"
[ -n "$DESC" ] || skip "MR !${CI_MERGE_REQUEST_IID} has an empty description"

if [ "${CI_MERGE_REQUEST_DESCRIPTION_IS_TRUNCATED:-}" = "true" ]; then
    has_boundary=""
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
scorer_out="$(python3 "$(dirname "$0")/score_description.py" "$TMP")"
scorer_rc=$?
printf '%s\n' "$scorer_out" | sed 's/^/  /'
echo ""
echo "⚠️  Limits: <=100 words, <=3 inline-code spans, <=3 bare identifiers in the"
echo "   headline section. Long-form mechanics belong in the Agent context"
echo "   <details> block."

exit "$scorer_rc"
