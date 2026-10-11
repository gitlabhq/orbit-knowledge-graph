#!/bin/sh
# Fails when a file under config/prompts/ changes without a version: change.
# Compares the working tree against $1 (default: MR diff base, then
# origin/main). Semver format itself is validated at build time by orbit-prompts.
set -eu

base="${1:-${CI_MERGE_REQUEST_DIFF_BASE_SHA:-origin/main}}"
status=0

for f in $(git diff --name-only "$base" -- config/prompts/); do
    case "$f" in *.yml) ;; *) continue ;; esac
    [ -f "$f" ] || continue
    old="$(git show "$base:$f" 2>/dev/null | grep '^version:' || true)"
    new="$(grep '^version:' "$f" || true)"
    if [ -z "$new" ]; then
        echo "ERROR: $f has no top-level version: field"
        status=1
    elif [ "$old" = "$new" ]; then
        echo "ERROR: $f changed without a version bump ($new)"
        status=1
    else
        echo "OK: $f (${old:-new} -> $new)"
    fi
done

exit $status
