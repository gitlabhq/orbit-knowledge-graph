#!/usr/bin/env bash
set -euo pipefail

VERSIONS_FILE="${VENDOR_VERSIONS_FILE:?Set VENDOR_VERSIONS_FILE or call via scripts/vendored/run.sh}"
VENDOR_DIR="${VENDOR_DIR:?Set VENDOR_DIR or call via scripts/vendored/run.sh}"
LATEST_URL="https://gitlab.com/gitlab-org/gitlab/-/jobs/artifacts/master/raw/tmp/tests/graphql/gitlab_schema.graphql?job=graphql-schema-dump"

job_url=$(curl -sf -o /dev/null -w '%{redirect_url}' "$LATEST_URL")
job_id=$(sed -nE 's#^https://gitlab.com/gitlab-org/gitlab/-/jobs/([0-9]+)/artifacts/.*#\1#p' <<<"$job_url")
if [[ -z "$job_id" ]]; then
    echo "ERROR: no graphql-schema-dump job behind $LATEST_URL" >&2
    exit 1
fi

mkdir -p "$VENDOR_DIR"
curl -sf "$job_url" -o "$VENDOR_DIR/gitlab.graphql"
JOB_ID="$job_id" yq -i ".vendored.${VENDOR_NAME}.version = strenv(JOB_ID)" "$VERSIONS_FILE"
echo "Vendored the GitLab GraphQL schema from job $job_id."
