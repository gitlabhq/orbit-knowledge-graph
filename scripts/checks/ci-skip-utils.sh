#!/usr/bin/env bash

ci_skip_requested() {
    local check_name="$1"
    local tag="[skip ${check_name}]"

    local env_var
    env_var="SKIP_$(echo "$check_name" | tr '[:lower:]-' '[:upper:]_')"
    [[ "${!env_var:-}" == "1" ]] && return 0

    [[ "${CI_MERGE_REQUEST_DESCRIPTION:-}" == *"${tag}"* ]] && return 0

    [[ "${CI_MERGE_REQUEST_TITLE:-}" == *"${tag}"* ]] && return 0

    # Commit messages in the MR range. This is the reliable fallback
    # when the description was edited after the pipeline started.
    if [[ -n "${BASE_REF:-}" ]]; then
        if [[ -n "${CI_MERGE_REQUEST_DIFF_BASE_SHA:-}" ]]; then
            git fetch origin "$CI_MERGE_REQUEST_DIFF_BASE_SHA" --depth=1 2>/dev/null || true
        fi
        # Buffer git log output before grepping to avoid SIGPIPE.
        # grep -q closes the pipe on first match, which SIGPIPEs the
        # producer (exit 141). Under set -o pipefail the pipeline takes
        # that non-zero exit and the && never fires. Buffering first
        # avoids the race entirely.
        local msgs
        msgs="$(git log "${BASE_REF}"...HEAD --format=%B 2>/dev/null)" || true
        echo "$msgs" | grep -qF "${tag}" && return 0
    fi

    return 1
}
