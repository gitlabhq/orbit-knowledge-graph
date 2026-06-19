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
# Modes:
#   Whole-tree / explicit files (lefthook, main-branch CI):
#     scripts/check-narration.sh                 # scan crates/
#     scripts/check-narration.sh a.rs b.rs ...   # scan specific files
#
#   MR-diff-scoped (merge_request CI pipelines):
#     scripts/check-narration.sh --diff-base <sha>
#     Scans only .rs files changed since <sha> and reports only flags on
#     added/modified lines, so pre-existing legacy narration is not noise.
#     Errors out (exit 2) if the base SHA is unreachable — never silently
#     falls back to a whole-tree scan that would report a false "clean".
set -uo pipefail

SCORER="$(dirname "$0")/narration_score.py"
DIFF_BASE=""

# Parse --diff-base flag (must be first arg if present).
if [ "${1:-}" = "--diff-base" ]; then
    if [ "$#" -lt 2 ] || [ -z "${2:-}" ]; then
        echo "error: --diff-base requires a SHA argument" >&2
        exit 2
    fi
    DIFF_BASE="$2"
    shift 2
fi

# ── Diff-scoped mode ──────────────────────────────────────────────────
# When --diff-base is set, only report flags on lines the MR added/changed.
# The scorer runs over whole files (it needs the next-code-line context for
# token_overlap), but we post-filter its output to the MR's changed hunks.
if [ -n "$DIFF_BASE" ]; then
    # Ensure the base SHA is reachable (CI shallow clones may not have it).
    if ! git cat-file -e "${DIFF_BASE}^{commit}" 2>/dev/null; then
        git fetch origin "$DIFF_BASE" --depth=1 2>/dev/null || true
        if ! git cat-file -e "${DIFF_BASE}^{commit}" 2>/dev/null; then
            echo ""
            echo "⚠️  narration scorer error: diff-base $DIFF_BASE is unreachable."
            echo "   Cannot scope to MR changes — the lint did not run."
            exit 2
        fi
    fi

    # Changed .rs files (exclude pure deletions — no lines to flag).
    mapfile -t files < <(git diff --name-only --diff-filter=d "${DIFF_BASE}...HEAD" -- '*.rs' | sort)
    if [ "${#files[@]}" -eq 0 ]; then
        echo "✅ narration lint: no Rust files changed in this MR."
        exit 0
    fi

    # Build a set of added/modified line numbers per file from hunk headers.
    # Output: one "file:line" per added line, consumed as a lookup set below.
    ADDED_LINES_FILE="$(mktemp)"
    trap 'rm -f "$ADDED_LINES_FILE"' EXIT
    git diff --unified=0 "${DIFF_BASE}...HEAD" -- '*.rs' \
        | python3 -c '
import sys, re
# Parse unified diff: extract file path from +++ and added-line ranges from @@.
current_file = None
for line in sys.stdin:
    m = re.match(r"^\+\+\+ b/(.+)$", line)
    if m:
        current_file = m.group(1)
        continue
    m = re.match(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@", line)
    if m and current_file:
        start = int(m.group(1))
        count = int(m.group(2)) if m.group(2) is not None else 1
        if count == 0:
            continue
        for ln in range(start, start + count):
            print(f"{current_file}:{ln}")
' > "$ADDED_LINES_FILE"

    echo "narration lint (MR-diff-scoped, base ${DIFF_BASE:0:12}):"
    echo "  scanning ${#files[@]} changed .rs file(s)..."
    echo ""
fi

# ── Scoring loop ──────────────────────────────────────────────────────
if [ -z "$DIFF_BASE" ]; then
    if [ "$#" -gt 0 ]; then
        files=("$@")
    else
        # Default scan target: the whole Rust source tree.
        mapfile -t files < <(find crates -name '*.rs' -type f | sort)
    fi
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
    # In diff-scoped mode, suppress the scorer's per-file stderr header
    # ("# file: N narration comment(s)") — it shows the whole-file count
    # which would contradict the filtered diff-scoped count.
    if [ -n "$DIFF_BASE" ]; then
        out="$(python3 "$SCORER" "$f" 2>/dev/null)"
    else
        out="$(python3 "$SCORER" "$f")"
    fi
    rc=$?
    if [ "$rc" -ge 2 ]; then
        scorer_errors=$((scorer_errors + 1))
        continue
    fi
    if [ -n "$out" ]; then
        if [ -n "$DIFF_BASE" ]; then
            # Post-filter: only keep flags on lines the MR added/changed.
            # Scorer output format: "file:line<TAB>detector<TAB>text"
            filtered=""
            while IFS= read -r flag_line; do
                # Extract "file:line" from the flag (everything before first tab).
                file_and_line="${flag_line%%	*}"
                if grep -qFx "$file_and_line" "$ADDED_LINES_FILE"; then
                    filtered="${filtered:+${filtered}
}${flag_line}"
                fi
            done <<< "$out"
            out="$filtered"
        fi
        if [ -n "$out" ]; then
            echo "$out"
            n=$(printf '%s\n' "$out" | grep -c $'\t' || true)
            total=$((total + n))
            flagged_files=$((flagged_files + 1))
        fi
    fi
done

if [ "$total" -gt 0 ]; then
    echo ""
    if [ -n "$DIFF_BASE" ]; then
        echo "⚠️  narration lint: $total new flagged comment(s) across $flagged_files file(s) in this MR."
    else
        echo "⚠️  narration lint: $total flagged comment(s) across $flagged_files file(s)."
    fi
    echo "   A comment must say *why* (a constraint, gotcha, ADR/issue link), never *what*."
    echo "   See AGENTS.md \"Code quality\". Rewrite or delete."
elif [ "$scorer_errors" -eq 0 ]; then
    if [ -n "$DIFF_BASE" ]; then
        echo "✅ narration lint: no new narration comments in this MR."
    else
        echo "✅ narration lint: no narration comments flagged."
    fi
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
