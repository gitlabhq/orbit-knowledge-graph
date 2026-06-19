#!/usr/bin/env bash
# Warning-mode MR-description headline gate.
#
# The MR description lives in the GitLab API, not the repo, so unlike most
# checks this CANNOT be a build.rs/lint — it is the one check that legitimately
# must run in CI (network-only data), per the AGENTS.md "prefer build-time
# validation" rationale.
#
# It resolves the MR for the current branch, fetches its `description` field,
# and scores the headline section with scripts/score_description.py
# (research-tuned: word_cap=100, span_cap=3, bare_idents<=3 with the v2
# acronym/ref-aware regex). It prints a verdict and any warnings.
#
# WARNING-MODE: this always exits 0. It also no-ops cleanly (exit 0) when the
# branch has no MR yet — push-before-MR is the common case.
#
# TODO(#2933, promote-to-blocking): this is NOT a config toggle. score_description.py
# signals PASS/FAIL via printed text only and always exits 0; making this gate
# blocking requires a deliberate code change in BOTH the scorer (emit a non-zero
# exit on FAIL) AND this wrapper (propagate that exit instead of `exit 0`).
#
# MR resolution order:
#   1. CI_MERGE_REQUEST_IID         (GitLab predefined, merge_request pipelines)
#   2. source_branch lookup via the API (CI fallback / local runs)
set -uo pipefail

PROJECT="${CI_PROJECT_ID:-${CI_PROJECT_PATH:-}}"

skip() {
    echo "ℹ️  mr-description lint: $1 — skipping (no-op, exit 0)."
    exit 0
}

# glab is the simplest authenticated client both locally and in CI. In CI it
# reads GITLAB_TOKEN from the environment; a read-API token must be provided as
# a CI variable for the API call to succeed (CI_JOB_TOKEN cannot read MR
# descriptions). If auth is missing, fetch_description returns empty and the
# check no-ops cleanly — it never blocks.
have_glab() { command -v glab >/dev/null 2>&1; }

fetch_description() {
    # Args: <iid>. Echoes the raw description on success.
    local iid="$1"
    if have_glab; then
        glab api "projects/${PROJECT//\//%2F}/merge_requests/${iid}" 2>/dev/null \
            | python3 -c 'import sys,json; print(json.load(sys.stdin).get("description") or "")'
    else
        skip "glab not available to fetch the description"
    fi
}

resolve_iid() {
    # Prefer the predefined var (only set on merge_request_event pipelines).
    if [ -n "${CI_MERGE_REQUEST_IID:-}" ]; then
        echo "$CI_MERGE_REQUEST_IID"
        return 0
    fi
    # Fallback: look the MR up by source branch.
    local branch="${CI_COMMIT_REF_NAME:-$(git rev-parse --abbrev-ref HEAD 2>/dev/null)}"
    [ -n "$branch" ] || return 1
    [ -n "$PROJECT" ] || return 1
    have_glab || return 1
    glab api "projects/${PROJECT//\//%2F}/merge_requests?source_branch=${branch}&state=opened" 2>/dev/null \
        | python3 -c 'import sys,json
mrs=json.load(sys.stdin)
print(mrs[0]["iid"] if mrs else "")'
}

[ -n "$PROJECT" ] || skip "no project id (CI_PROJECT_ID/CI_PROJECT_PATH unset)"

IID="$(resolve_iid || true)"
[ -n "${IID:-}" ] || skip "no open MR for this branch"

DESC="$(fetch_description "$IID")"
if [ -z "$DESC" ]; then
    skip "MR !$IID has an empty description"
fi

TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT
printf '%s' "$DESC" > "$TMP"

echo "MR !$IID description headline check (warning-mode, non-blocking):"
python3 "$(dirname "$0")/score_description.py" "$TMP" | sed 's/^/  /'
echo ""
echo "⚠️  Limits: <=100 words, <=3 inline-code spans, <=3 bare identifiers in the"
echo "   headline section. Long-form mechanics belong in the Agent context"
echo "   <details> block. This is warning-mode; it does not fail the pipeline."

# Warning-mode: never fail.
exit 0
