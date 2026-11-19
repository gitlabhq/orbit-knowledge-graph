#!/usr/bin/env bash
set -euo pipefail

IMAGE="${1:?usage: fetch-orbit-cli.sh <image>}"
JOB_NAME=cli-integration-test
API="${CI_API_V4_URL}/projects/${CI_PROJECT_ID}"
AUTH="JOB-TOKEN: ${CI_JOB_TOKEN}"
DEADLINE=$((SECONDS + ${E2E_CLI_WAIT_SECONDS:-900}))

job_state() {
  local page=1 body found
  while :; do
    body=$(curl -sfL --retry 3 --retry-all-errors --header "$AUTH" "$API/pipelines/${CI_PIPELINE_ID}/jobs?per_page=100&page=$page") || return 1
    found=$(python3 -c 'import json,sys
jobs=json.load(sys.stdin)
print(next((str(j["id"]) + " " + j["status"] for j in jobs if j["name"]==sys.argv[1]), "" if jobs else "end"))' "$JOB_NAME" <<<"$body")
    case "$found" in
      end) return 1 ;;
      "") page=$((page + 1)) ;;
      *) echo "$found"; return 0 ;;
    esac
  done
}

while :; do
  read -r JOB_ID STATUS < <(job_state || echo "- missing")
  case "$STATUS" in
    success) break ;;
    created|pending|running|waiting_for_resource|preparing|scheduled) ;;
    *) echo "$JOB_NAME is $STATUS in pipeline ${CI_PIPELINE_ID}"; exit 1 ;;
  esac
  ((SECONDS < DEADLINE)) || { echo "$JOB_NAME did not finish in time"; exit 1; }
  sleep 15
done

WORK=$(mktemp -d)
curl -sfL --retry 3 --retry-all-errors --header "$AUTH" -o "$WORK/artifacts.zip" "$API/jobs/$JOB_ID/artifacts"
python3 -m zipfile -e "$WORK/artifacts.zip" "$WORK"
[[ -f "$WORK/orbit-cli-e2e/orbit" ]] || { echo "job $JOB_ID has no orbit-cli-e2e/orbit"; exit 1; }
chmod +x "$WORK/orbit-cli-e2e/orbit"
docker build -t "$IMAGE" -f e2e/Dockerfile.cli "$WORK/orbit-cli-e2e"
docker push "$IMAGE"
