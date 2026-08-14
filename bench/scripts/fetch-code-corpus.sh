#!/usr/bin/env bash
# Fetch repository archives from gitlab.com and store locally or in GCS.
# Input: TSV file with project_id \t full_path (from dump-project-list.sh)
# Usage:
#   bash bench/scripts/fetch-code-corpus.sh projects.tsv
#   FETCH_MAX=10 BUCKET=/tmp/corpus bash bench/scripts/fetch-code-corpus.sh projects.tsv
set -euo pipefail

: "${GITLAB_TOKEN:=$(glab config get token --host gitlab.com 2>/dev/null)}"
: "${GITLAB_TOKEN:?must be set (PAT or glab oauth token)}"
: "${BUCKET:=gs://gkg-code-corpus}"
: "${JOBS:=8}"
: "${FETCH_MAX:=0}"

if [[ "${GITLAB_TOKEN}" == glpat-* ]]; then
  AUTH_HEADER="PRIVATE-TOKEN: ${GITLAB_TOKEN}"
else
  AUTH_HEADER="Authorization: Bearer ${GITLAB_TOKEN}"
fi

INPUT="${1:?usage: $0 <projects.tsv>}"
WORK="${TMPDIR:-/tmp}/code-corpus-work"
mkdir -p "${WORK}"
[[ "${BUCKET}" != gs://* ]] && mkdir -p "${BUCKET}"

TOTAL=$(wc -l < "${INPUT}" | tr -d ' ')
if [[ "${FETCH_MAX}" -gt 0 ]]; then
  echo "[corpus] ${TOTAL} projects in list, fetching first ${FETCH_MAX}"
else
  FETCH_MAX="${TOTAL}"
  echo "[corpus] ${TOTAL} projects to fetch"
fi

OK=0; SKIP=0; FAIL=0

while IFS=$'\t' read -r id path; do
  [[ $((OK + SKIP + FAIL)) -ge ${FETCH_MAX} ]] && break

  dest="${WORK}/${id}.tar.gz"
  target="${BUCKET}/${id}.tar.gz"

  # Skip if already exists
  if [[ "${BUCKET}" == gs://* ]]; then
    gsutil -q stat "${target}" 2>/dev/null && { SKIP=$((SKIP+1)); continue; }
  else
    [[ -f "${target}" ]] && { SKIP=$((SKIP+1)); continue; }
  fi

  code=$(curl -sf -w '%{http_code}' -o "${dest}" \
    -H "${AUTH_HEADER}" \
    "https://gitlab.com/api/v4/projects/${id}/repository/archive.tar.gz" 2>/dev/null) || code="000"

  case "${code}" in
    200)
      if [[ -s "${dest}" ]]; then
        if [[ "${BUCKET}" == gs://* ]]; then
          gsutil -q cp "${dest}" "${target}" && rm -f "${dest}"
        else
          mv "${dest}" "${target}"
        fi
        OK=$((OK+1))
        echo "  OK ${path} (${id})"
      else
        rm -f "${dest}"; SKIP=$((SKIP+1))
      fi
      ;;
    404|403)
      rm -f "${dest}"; SKIP=$((SKIP+1))
      ;;
    429)
      rm -f "${dest}"; FAIL=$((FAIL+1))
      echo "  RATE-LIMITED ${path} (${id}), sleeping 30s"
      sleep 30
      ;;
    *)
      rm -f "${dest}"; FAIL=$((FAIL+1))
      echo "  FAIL ${path} (${id}): HTTP ${code}"
      ;;
  esac
done < "${INPUT}"

echo "[corpus] Done: ${OK} fetched, ${SKIP} skipped, ${FAIL} failed. Archives in ${BUCKET}"
