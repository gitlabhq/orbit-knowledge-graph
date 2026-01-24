#!/bin/sh
set -e

# Skip if not a merge request
if [ -z "$CI_MERGE_REQUEST_IID" ]; then
  echo "Not a merge request. Skipping."
  exit 0
fi

echo "Checking MR title against conventional commit format: $CI_MERGE_REQUEST_TITLE"

# Check the MR title directly using commitlint
if ! echo "$CI_MERGE_REQUEST_TITLE" | npx commitlint; then
  echo "Merge request title does not follow conventional commit format."
  echo "Please update the title to follow the pattern: type(scope): description"
  echo "Examples: 'feat(api): add new endpoint', 'fix: resolve login issue'"
  exit 1
fi

echo "MR title follows conventional commit format"
exit 0
