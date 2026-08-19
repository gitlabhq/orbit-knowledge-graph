#!/usr/bin/env bash
# Re-download all existing archives with the correct --prefix.
# Usage: JOBS=8 bash bench/scripts/reprefix-archives.sh [projects.tsv]
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

: "${BUCKET:=gs://gkg-code-corpus}"
: "${JOBS:=8}"
: "${ARCHIVE_TIMEOUT:=120}"

INPUT="${1:-}"
WORK="${TMPDIR:-/tmp}/reprefix-work.$$"
mkdir -p "${WORK}"
trap 'rm -rf "${WORK}"' EXIT

if [[ -z "${INPUT}" ]]; then
  echo "[reprefix] Downloading project list from ${BUCKET}/projects.tsv"
  INPUT="${WORK}/projects.tsv"
  gcloud storage cp "${BUCKET}/projects.tsv" "${INPUT}" --quiet 2>/dev/null \
    || { echo "[reprefix] ERROR: no projects.tsv in ${BUCKET}" >&2; exit 1; }
fi

# Get list of existing archive IDs
echo "[reprefix] Listing existing archives..."
gcloud storage ls "${BUCKET}/*.tar.gz" 2>/dev/null \
  | sed "s|${BUCKET}/||; s|\.tar\.gz$||" \
  | sort > "${WORK}/existing.txt"
TOTAL=$(wc -l < "${WORK}/existing.txt" | tr -d ' ')
echo "[reprefix] ${TOTAL} archives to re-download with --prefix"

# Filter input to only existing IDs
awk -F'\t' 'NR==FNR{ids[$1];next} ($1 in ids)' "${WORK}/existing.txt" "${INPUT}" > "${WORK}/to-fix.tsv"

export GIT_SSH_COMMAND="ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new"

reprefix_one() {
  local id="$1" path="$2"
  local remote="git@gitlab.com:${path}.git"
  local target="${BUCKET}/${id}.tar.gz"
  if timeout "${ARCHIVE_TIMEOUT}" git archive --remote="${remote}" --prefix="${id}/" HEAD 2>/dev/null \
    | gzip | gcloud storage cp - "${target}" --quiet 2>/dev/null; then
    echo "  OK ${path} (${id})"
  else
    echo "  FAIL ${path} (${id})" >&2
  fi
}

export -f reprefix_one
export BUCKET ARCHIVE_TIMEOUT

awk -F'\t' '{print $1, $2}' "${WORK}/to-fix.tsv" \
  | xargs -P "${JOBS}" -n 2 bash -c 'reprefix_one "$@"' _

echo "[reprefix] Done."
