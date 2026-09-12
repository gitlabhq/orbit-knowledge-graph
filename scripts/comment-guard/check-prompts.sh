#!/usr/bin/env bash
# Prompt prose lint. Runs prompt_score.py (co-located) over the agent-facing
# text files: config/prompts/**/*.yml, config/setup/setup.yaml, and
# skills/**/*.md except the generated query_language.md.
#
# Modes:
#   check-prompts.sh                       # whole tree
#   check-prompts.sh FILE...               # explicit files (lefthook staged_files)
#   check-prompts.sh --diff-base <sha>     # files changed since <sha> (MR pipelines)
#
# Exit codes: 1 when any unit fails a gate, 2 when the diff base is unreachable,
# 0 on a clean run. Blocking-ness lives in CI allow_failure and lefthook config.
set -uo pipefail

SCORER="$(dirname "$0")/prompt_score.py"
PATHS=(config/prompts config/setup/setup.yaml skills)

is_prompt_file() {
    case "$1" in
        skills/orbit/references/query_language.md) return 1 ;;
        config/prompts/*.yml|config/prompts/*/*.yml|config/prompts/*/*/*.yml|config/setup/setup.yaml|skills/*.md|skills/*/*.md|skills/*/*/*.md) return 0 ;;
        *) return 1 ;;
    esac
}

if [ "${1:-}" = "--diff-base" ]; then
    base="${2:-}"
    [ -n "$base" ] || { echo "error: --diff-base requires a SHA argument" >&2; exit 2; }
    if ! git cat-file -e "${base}^{commit}" 2>/dev/null; then
        git fetch origin "$base" --depth=1 2>/dev/null || true
        git cat-file -e "${base}^{commit}" 2>/dev/null || { echo "⚠️  prompt lint: diff-base $base is unreachable; the lint did not run."; exit 2; }
    fi
    mapfile -t candidates < <(git diff --name-only --diff-filter=d "${base}...HEAD" -- "${PATHS[@]}" | sort)
    scope="changed in this MR"
elif [ "$#" -gt 0 ]; then
    candidates=("$@")
    scope="staged"
else
    mapfile -t candidates < <(find "${PATHS[@]}" -type f \( -name '*.yml' -o -name '*.yaml' -o -name '*.md' \) | sort)
    scope="whole tree"
fi

files=()
for f in "${candidates[@]}"; do
    [ -f "$f" ] && is_prompt_file "$f" && files+=("$f")
done
if [ "${#files[@]}" -eq 0 ]; then
    echo "✅ prompt lint: no prompt files ${scope}."
    exit 0
fi

out="$(python3 "$SCORER" "${files[@]}")"
rc=$?
if [ "$rc" -ge 2 ]; then
    echo "⚠️  prompt lint: the scorer failed. Do not read this as clean."
    exit "$rc"
fi
if [ -n "$out" ]; then
    echo "prompt lint (${scope}, ${#files[@]} file(s)):"
    echo "$out"
    echo ""
    echo "⚠️  Gates: no sentence over 25 words, average under 20, grade under 10,"
    echo "   no em or en dashes, no tell words. Rewrite the unit; do not widen the gate."
    exit 1
fi
echo "✅ prompt lint: ${#files[@]} file(s) ${scope} pass."
