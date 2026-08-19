#!/usr/bin/env bash
# Fetch a single project archive via git archive --remote (SSH).
# Pipes directly to GCS or local file. Zero local disk for GCS path.
# Called by fetch-code-corpus.sh.
set -euo pipefail

id="$1" path="$2"
target="${BUCKET}/${id}.tar.gz"

# Skip if already exists (checked against pre-fetched list)
if [[ "${BUCKET}" == gs://* ]]; then
  grep -qx "${id}" "${EXISTING_FILE}" && exit 0
else
  [[ -f "${target}" ]] && exit 0
fi

remote="git@gitlab.com:${path}.git"

if [[ "${BUCKET}" == gs://* ]]; then
  # Stream directly to GCS, no local disk
  if git archive --remote="${remote}" HEAD 2>/dev/null | gzip | gcloud storage cp - "${target}" --quiet 2>/dev/null; then
    echo "  OK ${path} (${id})"
  else
    echo "  SKIP ${path} (${id})" >&2
  fi
else
  mkdir -p "${BUCKET}"
  if git archive --remote="${remote}" HEAD 2>/dev/null | gzip > "${target}"; then
    if [[ -s "${target}" ]]; then
      echo "  OK ${path} (${id})"
    else
      rm -f "${target}"
    fi
  else
    rm -f "${target}"
    echo "  SKIP ${path} (${id})" >&2
  fi
fi
