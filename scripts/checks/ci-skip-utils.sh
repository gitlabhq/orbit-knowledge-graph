#!/usr/bin/env bash

ci_skip_requested() {
    local check_name="$1"
    local tag="[skip ${check_name}]"

    local env_var
    env_var="SKIP_$(echo "$check_name" | tr '[:lower:]-' '[:upper:]_')"
    [[ "${!env_var:-}" == "1" ]] && return 0

    [[ "${CI_MERGE_REQUEST_DESCRIPTION:-}" == *"${tag}"* ]] && return 0

    [[ "${CI_MERGE_REQUEST_TITLE:-}" == *"${tag}"* ]] && return 0

    if [[ -n "${BASE_REF:-}" ]]; then
        if [[ -n "${CI_MERGE_REQUEST_DIFF_BASE_SHA:-}" ]]; then
            git fetch origin "$CI_MERGE_REQUEST_DIFF_BASE_SHA" --depth=1 2>/dev/null || true
        fi
        local msgs
        msgs="$(git log "${BASE_REF}"...HEAD --format=%B 2>/dev/null)" || true
        echo "$msgs" | grep -qF "${tag}" && return 0
    fi

    return 1
}
