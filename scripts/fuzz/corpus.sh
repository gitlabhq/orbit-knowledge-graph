#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
tests="$root/crates/fuzz/tests"
seeds="$root/crates/fuzz/seeds.yaml"
gcs="https://storage.googleapis.com"
usage="usage: corpus.sh pull|push|crashes [target...] | check <archive...>"
action="${1:?$usage}"
shift
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

if (($# == 0)); then
  shopt -s nullglob
  for dir in "$tests"/fuzz_*/; do
    set -- "$@" "$(basename "$dir")"
  done
fi

token="${FUZZ_CORPUS_TOKEN:-}"
if [ -z "$token" ] && [ -n "${FUZZ_CORPUS_JWT:-}" ] && [ -n "${FUZZ_CORPUS_WIF_PROVIDER:-}" ] &&
  [ "${CI_MERGE_REQUEST_SOURCE_PROJECT_ID:-${CI_PROJECT_ID:-}}" = "${CI_PROJECT_ID:-}" ]; then
  token="$(jq -n --arg audience "$FUZZ_CORPUS_WIF_PROVIDER" --arg jwt "$FUZZ_CORPUS_JWT" '{
      audience: $audience,
      subjectToken: $jwt,
      subjectTokenType: "urn:ietf:params:oauth:token-type:jwt",
      grantType: "urn:ietf:params:oauth:grant-type:token-exchange",
      requestedTokenType: "urn:ietf:params:oauth:token-type:access_token",
      scope: "https://www.googleapis.com/auth/devstorage.read_write"
    }' | curl -fsS -H 'Content-Type: application/json' -d @- https://sts.googleapis.com/v1/token | jq -er .access_token)"
fi

gcs_curl() {
  curl -sS -H "Authorization: Bearer $token" "$@"
}

check() {
  local names types
  names="$(tar -tzf "$1")"
  types="$(tar -tvzf "$1" | cut -c1)"
  if grep -Eqv '^\./([A-Za-z0-9_-][A-Za-z0-9_.-]*)?$' <<< "$names" || grep -qv '[-d]' <<< "$types"; then
    echo "error: $1 has entries that are not plain files" >&2
    exit 1
  fi
}

pull() {
  local dir="$tests/$1" archive="$work/$1.tar.gz" status seed n=0
  rm -rf "$dir/corpus"
  mkdir -p "$dir/corpus"
  yq -o=json -I=0 ".$1 // [] | .[].input" "$seeds" > "$work/$1.seeds"
  while IFS= read -r seed; do
    jq -j . <<< "$seed" > "$dir/corpus/seed-$n"
    n=$((n + 1))
  done < "$work/$1.seeds"
  if [ -n "$token" ]; then
    status="$(gcs_curl -o "$archive" -w '%{http_code}' "$gcs/${FUZZ_CORPUS_BUCKET:?FUZZ_CORPUS_BUCKET must be set with a token}/$1/corpus.tar.gz")"
    case "$status" in
      200)
        check "$archive"
        tar -xzf "$archive" --no-same-owner -C "$dir/corpus"
        ;;
      404) ;;
      *)
        echo "error: the $1 corpus download returned HTTP $status" >&2
        exit 1
        ;;
    esac
  fi
  echo "$1: $(find "$dir/corpus" -type f | wc -l | tr -d ' ') corpus inputs"
}

push() {
  if [ -z "$token" ]; then
    echo "error: push needs FUZZ_CORPUS_TOKEN or a CI ID token" >&2
    exit 1
  fi
  tar -czf "$work/$1.tar.gz" -C "$tests/$1/corpus" .
  gcs_curl -f -X PUT -H 'Content-Type: application/gzip' --data-binary "@$work/$1.tar.gz" \
    "$gcs/${FUZZ_CORPUS_BUCKET:?FUZZ_CORPUS_BUCKET must be set with a token}/$1/corpus.tar.gz" > /dev/null
  echo "$1: pushed $(tar -tzf "$work/$1.tar.gz" | grep -vc '/$') corpus inputs"
}

crashes() {
  local dir="$tests/$1" run="${CI_PIPELINE_ID:-local}-${CI_JOB_ID:-$$}" file count=0
  if [ -z "$token" ]; then
    echo "$1: crashes and fuzz.log kept in $dir"
    return
  fi
  for file in "$dir"/crashes/* "$dir/fuzz.log"; do
    [ -f "$file" ] || continue
    gcs_curl -f -X PUT -H 'x-goog-if-generation-match: 0' --data-binary "@$file" \
      "$gcs/${FUZZ_CRASHES_BUCKET:?FUZZ_CRASHES_BUCKET must be set with a token}/$1/$run/$(basename "$file")" > /dev/null
    rm "$file"
    count=$((count + 1))
  done
  echo "$1: uploaded $count files to the crash bucket under $1/$run"
}

case "$action" in
  pull | push | crashes | check)
    for target in "$@"; do
      "$action" "$target"
    done
    ;;
  *)
    echo "$usage" >&2
    exit 1
    ;;
esac
