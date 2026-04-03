#!/bin/sh
set -e

if [ -z "$CI_MERGE_REQUEST_IID" ]; then
  echo "Not a merge request. Skipping."
  exit 0
fi

DESCRIPTION_FILE="${CI_MERGE_REQUEST_DESCRIPTION_FILE:-}"

if [ -z "$DESCRIPTION_FILE" ] || [ ! -f "$DESCRIPTION_FILE" ]; then
  echo "CI_MERGE_REQUEST_DESCRIPTION_FILE is unavailable."
  echo "Unable to validate whether this MR is linked to an issue."
  exit 1
fi

echo "Checking MR description for an issue-closing or issue-linking reference"

# Accept common GitLab issue-linking patterns against project-local or full URLs.
# Examples:
# - Closes #123
# - Related to #123
# - Closes https://gitlab.com/group/project/-/issues/123
# - Partially resolves https://gitlab.com/group/project/-/issues/123
PATTERN='(Closes|Closes:|Closes issue|Closes issues|Fixes|Fixes:|Fixes issue|Fixes issues|Resolves|Resolves:|Related to|Relates to|Partially resolves)[[:space:]]+((#[0-9]+)|(https://gitlab\.com/[^[:space:]]+/-/issues/[0-9]+))'

if ! grep -Eiq "$PATTERN" "$DESCRIPTION_FILE"; then
  echo "Merge request must be linked to an issue in the description."
  echo "Add a line such as:"
  echo "  Closes #123"
  echo "  Related to #123"
  echo "  Closes https://gitlab.com/<group>/<project>/-/issues/123"
  exit 1
fi

echo "MR description includes an issue reference"
exit 0
