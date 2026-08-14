#!/usr/bin/env bash
# Fetch a single project archive. Called by fetch-code-corpus.sh.
set -euo pipefail

id="$1" path="$2"
dest="${WORK}/${id}.tar.gz"
target="${BUCKET}/${id}.tar.gz"
AUTH_HEADER=$(cat "${AUTH_HEADER_FILE}")

# Skip if already exists
if [[ "${BUCKET}" == gs://* ]]; then
  gcloud storage ls "${target}" &>/dev/null && exit 0
else
  [[ -f "${target}" ]] && exit 0
fi

code=$(curl -s -w '%{http_code}' -o "${dest}" \
  -H "${AUTH_HEADER}" \
  "https://gitlab.com/api/v4/projects/${id}/repository/archive.tar.gz" 2>/dev/null) || code="000"

case "${code}" in
  200)
    if [[ -s "${dest}" ]]; then
      if [[ "${BUCKET}" == gs://* ]]; then
        if gcloud storage cp "${dest}" "${target}" --quiet 2>/dev/null; then
          rm -f "${dest}"
          echo "  OK ${path} (${id})"
        else
          rm -f "${dest}"
          echo "  FAIL ${path} (${id}): gcs upload" >&2
        fi
      else
        mv "${dest}" "${target}"
        echo "  OK ${path} (${id})"
      fi
    else
      rm -f "${dest}"
    fi
    ;;
  404|403) rm -f "${dest}" ;;
  429)
    rm -f "${dest}"
    echo "  RATE-LIMITED ${path} (${id})"
    sleep 30
    ;;
  *)
    rm -f "${dest}"
    echo "  FAIL ${path} (${id}): HTTP ${code}"
    ;;
esac
