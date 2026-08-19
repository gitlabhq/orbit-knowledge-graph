#!/usr/bin/env bash
# Fetch repository archives from gitlab.com via git archive --remote (SSH).
# Streams directly to GCS without touching local disk.
#
# Usage:
#   bash bench/scripts/fetch-code-corpus.sh [projects.tsv]
#   FETCH_MAX=10 bash bench/scripts/fetch-code-corpus.sh
#   JOBS=16 ARCHIVE_TIMEOUT=300 bash bench/scripts/fetch-code-corpus.sh
#   RETRY=failed bash bench/scripts/fetch-code-corpus.sh   # retry only failures
#   RETRY=all bash bench/scripts/fetch-code-corpus.sh      # re-fetch everything
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

: "${BUCKET:=gs://gkg-code-corpus}"
: "${JOBS:=8}"
: "${FETCH_MAX:=0}"
: "${ARCHIVE_TIMEOUT:=120}"
: "${RETRY:=}"
: "${LOG_FILE:=/tmp/corpus-fetch.log}"

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

# --- Retry modes ---
# RETRY=fail         re-fetch only FAIL entries from a previous log
# RETRY=skip         re-fetch only SKIP entries from a previous log
# RETRY=fail,skip    both (any order, comma-separated)
# RETRY=all          ignore existing archives, re-fetch everything
# (unset)            skip existing archives (default)
if [[ "${RETRY}" == "all" ]]; then
  echo "[corpus] RETRY=all: re-fetching all projects (ignoring existing)"
  touch "${WORK}/existing.txt"
elif [[ -n "${RETRY}" ]]; then
  if [[ ! -f "${LOG_FILE}" ]]; then
    echo "[corpus] ERROR: RETRY requires a previous log at ${LOG_FILE}" >&2; exit 1
  fi
  GREP_PATTERN=""
  IFS=',' read -ra MODES <<< "${RETRY}"
  for mode in "${MODES[@]}"; do
    case "${mode}" in
      fail|failed) GREP_PATTERN="${GREP_PATTERN:+${GREP_PATTERN}|}FAIL" ;;
      skip|skipped) GREP_PATTERN="${GREP_PATTERN:+${GREP_PATTERN}|}SKIP" ;;
      *) echo "[corpus] ERROR: unknown retry mode '${mode}' (use fail, skip, all)" >&2; exit 1 ;;
    esac
  done
  grep -E "${GREP_PATTERN}" "${LOG_FILE}" | grep -oE '\([0-9]+\)' | tr -d '()' | sort -u > "${WORK}/retry_ids.txt"
  RETRY_COUNT=$(wc -l < "${WORK}/retry_ids.txt" | tr -d ' ')
  echo "[corpus] Retrying ${RETRY_COUNT} projects (${RETRY}) from ${LOG_FILE}"
  awk -F'\t' 'NR==FNR{ids[$1];next} ($1 in ids)' "${WORK}/retry_ids.txt" "${INPUT}" > "${WORK}/filtered.tsv"
  INPUT="${WORK}/filtered.tsv"
  TOTAL="${RETRY_COUNT}"
  touch "${WORK}/existing.txt"
else
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
fi

if [[ "${FETCH_MAX}" -gt 0 ]] && [[ "${RETRY}" != "failed" ]]; then
  echo "[corpus] ${TOTAL} projects in list, fetching first ${FETCH_MAX}, ${JOBS} parallel, timeout=${ARCHIVE_TIMEOUT}s"
  head -n "${FETCH_MAX}" "${INPUT}" > "${WORK}/input.tsv"
else
  echo "[corpus] ${TOTAL} projects to fetch, ${JOBS} parallel, timeout=${ARCHIVE_TIMEOUT}s"
  cp "${INPUT}" "${WORK}/input.tsv"
fi

FETCH_SCRIPT="${BENCH_DIR}/scripts/fetch-one-archive.sh"

awk -F'\t' '{print $1, $2}' "${WORK}/input.tsv" \
  | xargs -P "${JOBS}" -n 2 \
    env BUCKET="${BUCKET}" EXISTING_FILE="${WORK}/existing.txt" ARCHIVE_TIMEOUT="${ARCHIVE_TIMEOUT}" \
    bash "${FETCH_SCRIPT}"

echo "[corpus] Done. Archives in ${BUCKET}"
