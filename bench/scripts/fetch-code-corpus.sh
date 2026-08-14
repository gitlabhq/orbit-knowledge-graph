#!/usr/bin/env bash
# Fetch repository archives from gitlab.com and upload to GCS.
# Input: TSV file with project_id \t full_path (from dump-project-list.sh)
# Usage: GITLAB_TOKEN=glpat-... bash bench/scripts/fetch-code-corpus.sh projects.tsv
set -euo pipefail

: "${GITLAB_TOKEN:?must be set (PAT with read_api scope)}"
: "${BUCKET:=gs://gkg-code-corpus}"
: "${JOBS:=8}"

INPUT="${1:?usage: $0 <projects.tsv>}"
WORK="${TMPDIR:-/tmp}/code-corpus"
mkdir -p "${WORK}"

TOTAL=$(wc -l < "${INPUT}" | tr -d ' ')
echo "[corpus] ${TOTAL} projects to fetch, ${JOBS} parallel jobs"

fetch_one() {
  local id="$1" path="$2"
  local dest="${WORK}/${id}.tar.gz"
  local gcs="${BUCKET}/${id}.tar.gz"

  # Skip if already in GCS
  if gsutil -q stat "${gcs}" 2>/dev/null; then
    return 0
  fi

  local url="https://gitlab.com/api/v4/projects/${id}/repository/archive.tar.gz"
  local attempt
  for attempt in 1 2 3; do
    local code
    code=$(curl -sf -w '%{http_code}' -o "${dest}" \
      -H "PRIVATE-TOKEN: ${GITLAB_TOKEN}" \
      "${url}" 2>/dev/null) || code="000"

    case "${code}" in
      200) break ;;
      404|403)
        # Archived, deleted, or private
        rm -f "${dest}"
        return 0
        ;;
      429)
        # Rate limited
        sleep $((attempt * 10))
        ;;
      *)
        sleep $((attempt * 2))
        ;;
    esac
  done

  if [[ ! -f "${dest}" ]] || [[ ! -s "${dest}" ]]; then
    echo "  SKIP ${path} (${id}): empty or failed"
    rm -f "${dest}"
    return 0
  fi

  gsutil -q cp "${dest}" "${gcs}" && rm -f "${dest}"
  echo "  OK ${path} (${id})"
}

export -f fetch_one
export WORK BUCKET GITLAB_TOKEN

# GNU parallel or xargs fallback
if command -v parallel &>/dev/null; then
  cat "${INPUT}" | parallel -j "${JOBS}" --colsep '\t' fetch_one {1} {2}
else
  cat "${INPUT}" | xargs -P "${JOBS}" -I{} bash -c '
    id=$(echo "{}" | cut -f1)
    path=$(echo "{}" | cut -f2)
    fetch_one "${id}" "${path}"
  '
fi

echo "[corpus] Done. Archives in ${BUCKET}"
