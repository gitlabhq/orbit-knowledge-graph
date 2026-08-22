#!/usr/bin/env bash
# Fetch the code-indexing memory-profiling corpus.
#
# The profiler's archive server serves `<project_id>.tar.gz` from the corpus
# directory, exactly as the Rails internal archive endpoint would, so each
# entry here is a pinned upstream tarball named after a synthetic project id.

set -euo pipefail

DIR="${MEMPROFILE_CORPUS_DIR:-.memprofile/corpus}"
mkdir -p "$DIR"

fetch() {
  local id="$1" url="$2" name="$3"
  if [[ -s "$DIR/$id.tar.gz" ]]; then
    echo "have  $id  $name ($(du -h "$DIR/$id.tar.gz" | cut -f1))"
    return
  fi
  echo "fetch $id  $name"
  curl -sSL --retry 3 --max-time 3600 -o "$DIR/$id.tar.gz.part" "$url"
  mv "$DIR/$id.tar.gz.part" "$DIR/$id.tar.gz"
}

fetch 100001 \
  "https://codeload.github.com/elastic/elasticsearch/tar.gz/refs/tags/v9.0.0" \
  "elastic/elasticsearch v9.0.0 (java)"

fetch 278964 \
  "https://gitlab.com/gitlab-org/gitlab/-/archive/v18.9.1-ee/gitlab-v18.9.1-ee.tar.gz" \
  "gitlab-org/gitlab v18.9.1-ee (ruby + typescript)"

fetch 100003 \
  "https://codeload.github.com/torvalds/linux/tar.gz/refs/tags/v6.12" \
  "torvalds/linux v6.12 (c)"

fetch 100004 \
  "https://codeload.github.com/microsoft/vscode/tar.gz/refs/tags/1.99.3" \
  "microsoft/vscode 1.99.3 (typescript)"

fetch 100005 \
  "https://codeload.github.com/django/django/tar.gz/refs/tags/5.2" \
  "django/django 5.2 (python)"

fetch 100006 \
  "https://codeload.github.com/protocolbuffers/protobuf/tar.gz/refs/tags/v30.2" \
  "protocolbuffers/protobuf v30.2 (c++)"

# A dhat pass over the full Java tree does not finish in a useful time, so the
# reduced corpus below carries the same shape at a fraction of the block count.
if [[ ! -s "$DIR/100002.tar.gz" ]]; then
  echo "build 100002  elasticsearch server/src/main/java (java, reduced for dhat)"
  work="$(mktemp -d)"
  tar xzf "$DIR/100001.tar.gz" -C "$work" elasticsearch-9.0.0/server/src/main/java
  mkdir -p "$work/esjava"
  mv "$work/elasticsearch-9.0.0/server/src/main/java" "$work/esjava/java"
  COPYFILE_DISABLE=1 tar czf "$DIR/100002.tar.gz" -C "$work" esjava
  rm -rf "$work"
else
  echo "have  100002  elasticsearch server/src/main/java ($(du -h "$DIR/100002.tar.gz" | cut -f1))"
fi

ls -la "$DIR"
