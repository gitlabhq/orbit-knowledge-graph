#!/usr/bin/env bash
set -euo pipefail

VERSIONS_FILE="${VENDOR_VERSIONS_FILE:?Set VENDOR_VERSIONS_FILE or call via scripts/vendored/run.sh}"
VENDOR_DIR="${VENDOR_DIR:?Set VENDOR_DIR or call via scripts/vendored/run.sh}"
LATEST_URL="https://gitlab.com/gitlab-org/gitlab/-/jobs/artifacts/master/raw/tmp/tests/graphql/gitlab_schema.graphql?job=graphql-schema-dump"

job_url=$(curl -sf --retry 4 --retry-all-errors -o /dev/null -w '%{redirect_url}' "$LATEST_URL")
job_id=$(sed -nE 's#^https://gitlab.com/gitlab-org/gitlab/-/jobs/([0-9]+)/artifacts/.*#\1#p' <<<"$job_url")
if [[ -z "$job_id" ]]; then
    echo "ERROR: no graphql-schema-dump job behind $LATEST_URL" >&2
    exit 1
fi

download=$(mktemp)
trap 'rm -f "$download"' EXIT
curl -sf --retry 4 --retry-all-errors --max-filesize 67108864 "$job_url" -o "$download"
if ! grep -q '^type Query ' "$download"; then
    echo "ERROR: job $job_id did not return a GraphQL schema with a Query type" >&2
    exit 1
fi

mkdir -p "$VENDOR_DIR"
mv "$download" "$VENDOR_DIR/gitlab.graphql"
JOB_ID="$job_id" perl -0pi -e 's/(\n  \Q$ENV{VENDOR_NAME}\E:\n(?:    .*\n)*?    version: )"[^"]*"/$1"$ENV{JOB_ID}"/' "$VERSIONS_FILE"
echo "Vendored the GitLab GraphQL schema from job $job_id."
