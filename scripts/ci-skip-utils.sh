#!/usr/bin/env bash
# Shared helper for [skip <check-name>] detection in CI.
#
# Usage (source from a check script):
#   BASE_REF="${1:-origin/main}"
#   source "$(dirname "$0")/ci-skip-utils.sh"
#   if ci_skip_requested "schema-version-check"; then
#       echo "skipping"; exit 0
#   fi
#
# Checks four places (in order):
#   1. SKIP_<UPPER_NAME> env var (set to "1")
#   2. CI_MERGE_REQUEST_DESCRIPTION (set at pipeline creation)
#   3. Commit messages in the MR range (fallback when description is stale)

ci_skip_requested() {
    local check_name="$1"
    local tag="[skip ${check_name}]"

    # Env var override: SKIP_SCHEMA_VERSION_CHECK=1 etc.
    local env_var
    env_var="SKIP_$(echo "$check_name" | tr '[:lower:]-' '[:upper:]_')"
    [[ "${!env_var:-}" == "1" ]] && return 0

    # MR description (set at pipeline creation, may be stale).
    [[ "${CI_MERGE_REQUEST_DESCRIPTION:-}" == *"${tag}"* ]] && return 0

    # MR title.
    [[ "${CI_MERGE_REQUEST_TITLE:-}" == *"${tag}"* ]] && return 0

    # Commit messages in the MR range. This is the reliable fallback
    # when the description was edited after the pipeline started.
    if [[ -n "${BASE_REF:-}" ]]; then
        git log "${BASE_REF}"...HEAD --format=%B 2>/dev/null | grep -qF "${tag}" && return 0
    fi

    return 1
}
