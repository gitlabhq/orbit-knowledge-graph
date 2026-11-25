#!/bin/sh
set -e

if [ -z "$CI_MERGE_REQUEST_IID" ]; then
  echo "Not a merge request. Skipping."
  exit 0
fi

TITLE=$(printf '%s\n' "$CI_MERGE_REQUEST_TITLE" | sed 's/^\(\[Draft\]\|(Draft)\|Draft:\)[[:space:]]*//')

echo "Checking MR title against conventional commit format: $TITLE"

if ! printf '%s\n' "$TITLE" | bun x commitlint; then
  echo "Merge request title does not follow conventional commit format."
  echo "Please update the title to follow the pattern: type(scope): description"
  echo "Examples: 'feat(api): add new endpoint', 'fix: resolve login issue'"
  exit 1
fi

echo "MR title follows conventional commit format"
exit 0
