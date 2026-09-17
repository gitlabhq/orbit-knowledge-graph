#!/bin/sh

# Publishes a multi-arch index from the digests the build jobs reported and
# signs it before any final tag exists, so a registry tag moved between build
# and publish is never included and never signed.

set -eu

usage() {
  echo "Usage: $0 <image:tag> [<image:tag>...] -- <image:tag@sha256:digest>..." >&2
  exit 1
}

targets=""
while [ "$#" -gt 0 ] && [ "$1" != "--" ]; do
  targets="${targets} $1"
  shift
done
[ -n "$targets" ] && [ "$#" -ge 2 ] || usage
shift
sources="$*"

first_target="${targets# }"
first_target="${first_target%% *}"
image_name="${first_target%:*}"
first_tag="${first_target##*:}"
staging="${image_name}:${first_tag}-candidate"
workdir=$(mktemp -d)
script_dir=$(dirname "$0")

digest_from_metadata() {
  digest=$(sed -n 's/.*"containerimage\.digest": *"\([^"]*\)".*/\1/p' "$1" | head -n1)
  if [ -z "$digest" ]; then
    digest=$(grep -o '"digest": *"sha256:[0-9a-f]*"' "$1" | head -n1 | grep -o 'sha256:[0-9a-f]*')
  fi
  [ -n "$digest" ] || { echo "No digest in ${1}:" >&2; cat "$1" >&2; exit 1; }
  echo "$digest"
}

source_digest_refs=""
for src in $sources; do
  case "$src" in
    *@sha256:*) source_digest_refs="${source_digest_refs} ${image_name}@${src##*@}" ;;
    *) echo "Source ${src} carries no digest." >&2; exit 1 ;;
  esac
done

echo "Creating ${staging} from${source_digest_refs}"
docker buildx imagetools create --metadata-file "${workdir}/create.json" -t "$staging" $source_digest_refs
index_digest=$(digest_from_metadata "${workdir}/create.json")
echo "Index digest ${index_digest}"

"${script_dir}/sign-image.sh" "${image_name}:${first_tag}@${index_digest}" $sources

tag_args=""
for target in $targets; do
  tag_args="${tag_args} -t ${target}"
done
echo "Promoting ${index_digest} to${targets}"
docker buildx imagetools create --prefer-index=false --metadata-file "${workdir}/promote.json" $tag_args "${image_name}@${index_digest}"
promoted_digest=$(digest_from_metadata "${workdir}/promote.json")
if [ "$promoted_digest" != "$index_digest" ]; then
  echo "Promoted digest ${promoted_digest} differs from the signed digest ${index_digest}." >&2
  exit 1
fi
