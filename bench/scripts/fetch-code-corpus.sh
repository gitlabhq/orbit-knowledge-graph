#!/usr/bin/env bash
# Fetch repository archives from gitlab.com and store locally or in GCS.
# Input: TSV file with project_id \t full_path (from dump-project-list.sh)
# Usage:
#   bash bench/scripts/fetch-code-corpus.sh projects.tsv
#   FETCH_MAX=10 BUCKET=/tmp/corpus bash bench/scripts/fetch-code-corpus.sh projects.tsv
#   JOBS=16 bash bench/scripts/fetch-code-corpus.sh projects.tsv
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

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
export WORK="${TMPDIR:-/tmp}/code-corpus-work.$$"
export BUCKET
mkdir -p "${WORK}"
[[ "${BUCKET}" != gs://* ]] && mkdir -p "${BUCKET}"
echo "${AUTH_HEADER}" > "${WORK}/.auth_header"
chmod 600 "${WORK}/.auth_header"
trap 'rm -rf "${WORK}"' EXIT
export AUTH_HEADER_FILE="${WORK}/.auth_header"

TOTAL=$(wc -l < "${INPUT}" | tr -d ' ')

# Pre-fetch existing archives list to avoid per-project GCS lookups.
if [[ "${BUCKET}" == gs://* ]]; then
  echo "[corpus] Listing existing archives in ${BUCKET}..."
  gcloud storage ls "${BUCKET}/*.tar.gz" 2>/dev/null \
    | sed "s|${BUCKET}/||; s|\.tar\.gz$||" \
    | sort > "${WORK}/existing.txt"
  EXISTING=$(wc -l < "${WORK}/existing.txt" | tr -d ' ')
  echo "[corpus] ${EXISTING} archives already in bucket"
else
  touch "${WORK}/existing.txt"
fi
export EXISTING_FILE="${WORK}/existing.txt"

if [[ "${FETCH_MAX}" -gt 0 ]]; then
  echo "[corpus] ${TOTAL} projects in list, fetching first ${FETCH_MAX}, ${JOBS} parallel"
  head -n "${FETCH_MAX}" "${INPUT}" > "${WORK}/input.tsv"
else
  echo "[corpus] ${TOTAL} projects to fetch, ${JOBS} parallel"
  cp "${INPUT}" "${WORK}/input.tsv"
fi

FETCH_SCRIPT="${BENCH_DIR}/scripts/fetch-one-archive.sh"

# Convert TSV to space-separated for xargs, run fetch-one-archive.sh per line.
awk -F'\t' '{print $1, $2}' "${WORK}/input.tsv" \
  | xargs -P "${JOBS}" -n 2 \
    env WORK="${WORK}" BUCKET="${BUCKET}" AUTH_HEADER_FILE="${AUTH_HEADER_FILE}" EXISTING_FILE="${EXISTING_FILE}" \
    bash "${FETCH_SCRIPT}"

echo "[corpus] Done. Archives in ${BUCKET}"
