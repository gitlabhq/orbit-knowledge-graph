#!/usr/bin/env bash
# Verify that config/RAW_OUTPUT_FORMAT_VERSION (and GOON_OUTPUT_FORMAT_VERSION
# when it exists) is bumped when response-format-affecting files change.
# Also verifies the $id in query_response.json matches the version's major.
# Used in CI on merge requests and as a lefthook pre-commit hook.
set -euo pipefail

BASE_REF="${1:-origin/main}"

valid_semver() {
    [[ "$1" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]
}

semver_gt() {
    # returns 0 if $1 > $2 (strictly greater)
    local a="$1" b="$2"
    [[ "$a" != "$b" ]] || return 1
    # sort -V treats 1.10.0 > 1.9.0 correctly
    [[ "$(printf '%s\n%s\n' "$a" "$b" | sort -V | tail -1)" == "$a" ]]
}

raw_files_changed() {
    git diff --name-only "$BASE_REF"...HEAD \
        | grep -qE '^(crates/query-engine/formatters/src/(graph|lib)\.rs|crates/gkg-server/schemas/query_response\.json)$'
}

goon_files_changed() {
    git diff --name-only "$BASE_REF"...HEAD \
        | grep -qE '^(crates/query-engine/formatters/src/goon\.rs|crates/gkg-server/schemas/goon_format\.md)$'
}

read_version_at_ref() {
    # $1 = ref (empty for working tree), $2 = path
    local ref="$1" path="$2"
    if [[ -z "$ref" ]]; then
        tr -d '[:space:]' < "$path" 2>/dev/null || true
    else
        git show "$ref:$path" 2>/dev/null | tr -d '[:space:]' || true
    fi
}

semver_bumped() {
    local file="$1"
    local old new
    old="$(read_version_at_ref "$BASE_REF" "$file")"
    new="$(read_version_at_ref "" "$file")"
    # New file introduction counts as a bump.
    if [[ -z "$old" && -n "$new" ]]; then
        valid_semver "$new"
        return
    fi
    [[ -n "$new" ]] && valid_semver "$new" && semver_gt "$new" "$old"
}

schema_id_matches_major() {
    local version_file="$1" schema_file="$2"
    local version major id_suffix
    version="$(read_version_at_ref "" "$version_file")"
    [[ -n "$version" ]] || return 0
    major="${version%%.*}"
    # Extract the /vN segment from $id. grep -o gives us the full match.
    id_suffix="$(grep -oE '/v[0-9]+"' "$schema_file" | head -1 | tr -d '/v"')"
    if [[ -z "$id_suffix" ]]; then
        echo "FAIL $schema_file has no /vN \$id suffix to match against RAW_OUTPUT_FORMAT_VERSION major."
        return 1
    fi
    if [[ "$id_suffix" != "$major" ]]; then
        echo "FAIL $schema_file \$id has /v$id_suffix but RAW_OUTPUT_FORMAT_VERSION major is $major."
        return 1
    fi
}

skip_requested() {
    [[ "${SKIP_RESPONSE_SCHEMA_VERSION_CHECK:-}" == "1" ]] && return 0
    local mr_desc
    mr_desc="${CI_MERGE_REQUEST_DESCRIPTION:-}"
    [[ "$mr_desc" == *"[skip response-schema-version-check]"* ]]
}

if skip_requested; then
    echo "WARN response-schema-version-check SKIPPED via [skip response-schema-version-check] or SKIP_RESPONSE_SCHEMA_VERSION_CHECK=1."
    echo "WARN Reviewer: confirm this is intentional."
    exit 0
fi

exit_code=0

# Always validate RAW_OUTPUT_FORMAT_VERSION itself is well-formed.
if [[ -f config/RAW_OUTPUT_FORMAT_VERSION ]]; then
    raw_ver="$(read_version_at_ref "" config/RAW_OUTPUT_FORMAT_VERSION)"
    if ! valid_semver "$raw_ver"; then
        echo "FAIL config/RAW_OUTPUT_FORMAT_VERSION ('$raw_ver') is not valid semver."
        exit_code=1
    fi
fi

# Always validate $id/major sync regardless of whether files changed --
# a direct edit to the schema $id without touching the version file would
# otherwise slip through.
if ! schema_id_matches_major config/RAW_OUTPUT_FORMAT_VERSION \
    crates/gkg-server/schemas/query_response.json; then
    exit_code=1
fi

# Check RAW format changes require a semver bump.
if raw_files_changed; then
    if semver_bumped config/RAW_OUTPUT_FORMAT_VERSION; then
        echo "OK RAW format files changed and config/RAW_OUTPUT_FORMAT_VERSION was bumped (semver-greater)."
    else
        echo "FAIL RAW format files changed but config/RAW_OUTPUT_FORMAT_VERSION was not bumped (or was downgraded/invalid)."
        echo ""
        echo "Any MR that modifies:"
        echo "  - crates/query-engine/formatters/src/graph.rs"
        echo "  - crates/query-engine/formatters/src/lib.rs"
        echo "  - crates/gkg-server/schemas/query_response.json"
        echo "must also bump config/RAW_OUTPUT_FORMAT_VERSION to a strictly greater semver."
        echo ""
        echo "If the change does not affect the response shape (e.g. refactoring,"
        echo "comments, test-only changes), add [skip response-schema-version-check]"
        echo "to the MR description, or set SKIP_RESPONSE_SCHEMA_VERSION_CHECK=1."
        exit_code=1
    fi
else
    echo "OK No RAW format files changed -- no RAW_OUTPUT_FORMAT_VERSION bump required."
fi

# Check GOON format changes (no-op until config/GOON_OUTPUT_FORMAT_VERSION exists).
if [[ -f config/GOON_OUTPUT_FORMAT_VERSION ]]; then
    goon_ver="$(read_version_at_ref "" config/GOON_OUTPUT_FORMAT_VERSION)"
    if ! valid_semver "$goon_ver"; then
        echo "FAIL config/GOON_OUTPUT_FORMAT_VERSION ('$goon_ver') is not valid semver."
        exit_code=1
    fi

    if goon_files_changed; then
        if semver_bumped config/GOON_OUTPUT_FORMAT_VERSION; then
            echo "OK GOON format files changed and config/GOON_OUTPUT_FORMAT_VERSION was bumped (semver-greater)."
        else
            echo "FAIL GOON format files changed but config/GOON_OUTPUT_FORMAT_VERSION was not bumped (or was downgraded/invalid)."
            exit_code=1
        fi
    else
        echo "OK No GOON format files changed -- no GOON_OUTPUT_FORMAT_VERSION bump required."
    fi
fi

exit $exit_code
