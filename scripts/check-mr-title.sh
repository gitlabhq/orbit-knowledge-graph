#!/bin/sh
set -e

# Skip if not a merge request
if [ -z "$CI_MERGE_REQUEST_IID" ]; then
  echo "Not a merge request. Skipping."
  exit 0
fi

echo "Checking MR title against conventional commit format: $CI_MERGE_REQUEST_TITLE"

# Strip "Draft: " prefix (case-insensitive) if present
TITLE_TO_CHECK="$CI_MERGE_REQUEST_TITLE"
TITLE_TO_CHECK=$(echo "$TITLE_TO_CHECK" | sed -E 's/^[Dd][Rr][Aa][Ff][Tt]:[[:space:]]*//')

echo "Title after stripping Draft prefix: $TITLE_TO_CHECK"

# Check the MR title using commitlint
if ! echo "$TITLE_TO_CHECK" | npx commitlint; then
  echo "Merge request title does not follow conventional commit format."
  echo "Please update the title to follow the pattern: type(scope): description"
  echo "Examples: 'feat(api): add new endpoint', 'fix: resolve login issue'"
  exit 1
fi

echo "MR title follows conventional commit format"
exit 0
