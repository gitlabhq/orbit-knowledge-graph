#!/bin/sh

# Publishes a multi-arch index from the digests the build jobs stored and
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
case "$first_tag" in
  */*|"$first_target")
    echo "Target ${first_target} has no tag." >&2
    exit 1
    ;;
esac
staging="${image_name}:${first_tag}-candidate"
workdir=$(mktemp -d)
script_dir=$(dirname "$0")

index_digest_from_metadata() {
  digest=$(tr -d ' \n\t' < "$1" | grep -oE '"containerimage\.descriptor":\{[^}]*\}' | grep -oE '"digest":"sha256:[0-9a-f]{64}"' | head -n1 | grep -oE 'sha256:[0-9a-f]{64}')
  if [ -z "$digest" ]; then
    echo "No index digest in ${1}:" >&2
    cat "$1" >&2
    exit 1
  fi
  echo "$digest"
}

source_digest_refs=""
for src in $sources; do
  case "$src" in
    "${image_name}:"*) ;;
    *)
      echo "Source ${src} is not in ${image_name}." >&2
      exit 1
      ;;
  esac
  if ! printf '%s' "$src" | grep -Eq '@sha256:[0-9a-f]{64}$'; then
    echo "Source ${src} carries no full digest." >&2
    exit 1
  fi
  source_digest_refs="${source_digest_refs} ${image_name}@${src##*@}"
done

echo "Creating ${staging} from${source_digest_refs}"
docker buildx imagetools create --metadata-file "${workdir}/create.json" -t "$staging" $source_digest_refs
index_digest=$(index_digest_from_metadata "${workdir}/create.json")
echo "Index digest ${index_digest}"

"${script_dir}/sign-image.sh" "${image_name}:${first_tag}@${index_digest}" $sources

tag_args=""
for target in $targets; do
  tag_args="${tag_args} -t ${target}"
done
echo "Promoting ${index_digest} to${targets}"
docker buildx imagetools create --metadata-file "${workdir}/promote.json" $tag_args "${image_name}@${index_digest}"
promoted_digest=$(index_digest_from_metadata "${workdir}/promote.json")
if [ "$promoted_digest" != "$index_digest" ]; then
  echo "Promoted digest ${promoted_digest} differs from the signed digest ${index_digest}." >&2
  exit 1
fi
