#!/usr/bin/env bash
# Fetch repository archives from gitlab.com via git archive --remote (SSH).
# Streams directly to GCS without touching local disk.
# Input: TSV file with project_id \t full_path (from dump-project-list.sh)
# Usage:
#   bash bench/scripts/fetch-code-corpus.sh projects.tsv
#   FETCH_MAX=10 bash bench/scripts/fetch-code-corpus.sh projects.tsv
#   JOBS=16 bash bench/scripts/fetch-code-corpus.sh projects.tsv
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

: "${BUCKET:=gs://gkg-code-corpus}"
: "${JOBS:=8}"
: "${FETCH_MAX:=0}"

INPUT="${1:-}"
WORK="${TMPDIR:-/tmp}/code-corpus-work.$$"
mkdir -p "${WORK}"
trap 'rm -rf "${WORK}"' EXIT
[[ "${BUCKET}" != gs://* ]] && mkdir -p "${BUCKET}"

# Resolve project list: explicit file > GCS > error
if [[ -z "${INPUT}" ]]; then
  if [[ "${BUCKET}" == gs://* ]]; then
    echo "[corpus] No input file specified, checking ${BUCKET}/projects.tsv"
    INPUT="${WORK}/projects.tsv"
    gcloud storage cp "${BUCKET}/projects.tsv" "${INPUT}" --quiet 2>/dev/null \
      || { echo "[corpus] ERROR: no projects.tsv in ${BUCKET}. Run dump-project-list.sh first." >&2; exit 1; }
  else
    echo "Usage: $0 [projects.tsv]" >&2; exit 1
  fi
fi

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

if [[ "${FETCH_MAX}" -gt 0 ]]; then
  echo "[corpus] ${TOTAL} projects in list, fetching first ${FETCH_MAX}, ${JOBS} parallel"
  head -n "${FETCH_MAX}" "${INPUT}" > "${WORK}/input.tsv"
else
  echo "[corpus] ${TOTAL} projects to fetch, ${JOBS} parallel"
  cp "${INPUT}" "${WORK}/input.tsv"
fi

FETCH_SCRIPT="${BENCH_DIR}/scripts/fetch-one-archive.sh"

awk -F'\t' '{print $1, $2}' "${WORK}/input.tsv" \
  | xargs -P "${JOBS}" -n 2 \
    env BUCKET="${BUCKET}" EXISTING_FILE="${WORK}/existing.txt" ARCHIVE_TIMEOUT="${ARCHIVE_TIMEOUT:-120}" \
    bash "${FETCH_SCRIPT}"

echo "[corpus] Done. Archives in ${BUCKET}"
