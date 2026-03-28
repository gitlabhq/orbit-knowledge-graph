#!/bin/sh
set -e

# Skip if not a merge request
if [ -z "$CI_MERGE_REQUEST_IID" ]; then
  echo "Not a merge request. Skipping."
  exit 0
fi

# Strip GitLab draft prefixes: "Draft:", "[Draft]", "(Draft)"
# Use multiple -e expressions for POSIX compatibility (busybox sed lacks \| alternation)
TITLE=$(printf '%s\n' "$CI_MERGE_REQUEST_TITLE" \
  | sed -e 's/^\[Draft\][[:space:]]*//' \
        -e 's/^(Draft)[[:space:]]*//' \
        -e 's/^Draft:[[:space:]]*//')

echo "Checking MR title against conventional commit format: $TITLE"

# Check the MR title directly using commitlint
if ! printf '%s\n' "$TITLE" | commitlint; then
  echo "Merge request title does not follow conventional commit format."
  echo "Please update the title to follow the pattern: type(scope): description"
  echo "Examples: 'feat(api): add new endpoint', 'fix: resolve login issue'"
  exit 1
fi

echo "MR title follows conventional commit format"
exit 0
