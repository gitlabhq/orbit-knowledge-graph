#!/usr/bin/env bash
# Warning-mode narration-comment lint for Rust sources.
#
# Runs scripts/narration_score.py over the given .rs files (or, with no args,
# the whole crates/ tree) and prints any flagged narration comments. A comment
# is narration when it restates the next line / labels a block with no "why"
# content (see AGENTS.md "Code quality"). This is the primary, higher-precision
# lint; lint/ast-grep/narration-comments.yml is a lower-precision fallback.
#
# WARNING-MODE: this always exits 0. It reports flags for awareness; it does
# not block the commit or pipeline. (Rollout plan #2933: promote to blocking
# once the new-commit false-positive rate is acceptable.)
#
# Usage:
#   scripts/check-narration.sh                 # scan crates/
#   scripts/check-narration.sh a.rs b.rs ...   # scan specific files (lefthook)
set -uo pipefail

SCORER="$(dirname "$0")/narration_score.py"

if [ "$#" -gt 0 ]; then
    files=("$@")
else
    # Default scan target: the whole Rust source tree.
    mapfile -t files < <(find crates -name '*.rs' -type f | sort)
fi

total=0
flagged_files=0
for f in "${files[@]}"; do
    [ -f "$f" ] || continue
    case "$f" in
        *.rs) ;;
        *) continue ;;
    esac
    out="$(python3 "$SCORER" "$f" 2>/dev/null)"
    if [ -n "$out" ]; then
        echo "$out"
        n=$(printf '%s\n' "$out" | grep -c $'\t' || true)
        total=$((total + n))
        flagged_files=$((flagged_files + 1))
    fi
done

if [ "$total" -gt 0 ]; then
    echo ""
    echo "⚠️  narration lint: $total flagged comment(s) across $flagged_files file(s) (warning-mode, non-blocking)."
    echo "   A comment must say *why* (a constraint, gotcha, ADR/issue link), never *what*."
    echo "   See AGENTS.md \"Code quality\". Rewrite or delete; this does not fail the build."
else
    echo "✅ narration lint: no narration comments flagged."
fi

# Warning-mode: never fail.
exit 0
