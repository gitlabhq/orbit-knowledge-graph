#!/bin/sh
set -e

# Skip if not a merge request
if [ -z "$CI_MERGE_REQUEST_IID" ]; then
  echo "Not a merge request. Skipping."
  exit 0
fi

TITLE="$CI_MERGE_REQUEST_TITLE"

# Reject Draft MRs — they must be marked ready before the title check passes.
# Checking for all GitLab draft prefix variants: "Draft:", "[Draft]", "(Draft)"
case "$TITLE" in
  "Draft: "*|"[Draft] "*|"(Draft) "*|"Draft:"*|"[Draft]"*|"(Draft)"*)
    echo "This MR is still a Draft. Remove the Draft status before merging."
    echo "MR title: $TITLE"
    exit 1
    ;;
esac

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
