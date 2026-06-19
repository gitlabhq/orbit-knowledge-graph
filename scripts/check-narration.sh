#!/usr/bin/env bash
# Narration-comment lint for Rust sources.
#
# Runs scripts/narration_score.py over the given .rs files (or, with no args,
# the whole crates/ tree) and prints any flagged narration comments. A comment
# is narration when it restates the next line / labels a block with no "why"
# content (see AGENTS.md "Code quality"). This is the primary, higher-precision
# lint; lint/ast-grep/narration-comments.yml is a lower-precision fallback.
#
# Exit codes: non-zero when narration flags are found OR when the scorer
# itself errors; zero only on a genuinely clean run. Blocking-ness is
# controlled externally — CI `allow_failure: true` (one-line change to
# promote) and lefthook's per-job config — not inside this script.
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
scorer_errors=0
for f in "${files[@]}"; do
    [ -f "$f" ] || continue
    case "$f" in
        *.rs) ;;
        *) continue ;;
    esac
    # Capture the scorer's exit status: 0 = clean, 1 = flags found (output
    # contains the flagged lines), >=2 = usage/crash error. Command
    # substitution does not trigger pipefail, so $? is the scorer's real exit.
    out="$(python3 "$SCORER" "$f")"
    rc=$?
    if [ "$rc" -ge 2 ]; then
        scorer_errors=$((scorer_errors + 1))
        continue
    fi
    if [ -n "$out" ]; then
        echo "$out"
        n=$(printf '%s\n' "$out" | grep -c $'\t' || true)
        total=$((total + n))
        flagged_files=$((flagged_files + 1))
    fi
done

if [ "$total" -gt 0 ]; then
    echo ""
    echo "⚠️  narration lint: $total flagged comment(s) across $flagged_files file(s)."
    echo "   A comment must say *why* (a constraint, gotcha, ADR/issue link), never *what*."
    echo "   See AGENTS.md \"Code quality\". Rewrite or delete."
elif [ "$scorer_errors" -eq 0 ]; then
    echo "✅ narration lint: no narration comments flagged."
fi

if [ "$scorer_errors" -gt 0 ]; then
    echo ""
    echo "⚠️  narration scorer error: the scorer failed on $scorer_errors file(s)."
    echo "   The lint did not run cleanly — do not read this as 'no narration'."
    echo "   Check that $SCORER exists and python3 is available."
fi

# Exit non-zero when narration flags are found or the scorer itself errored.
# Blocking-ness is controlled by CI allow_failure / lefthook job config.
if [ "$scorer_errors" -gt 0 ] || [ "$total" -gt 0 ]; then
    exit 1
fi
