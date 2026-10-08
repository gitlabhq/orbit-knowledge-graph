#!/usr/bin/env bash
# Point the cacheable parts of CARGO_HOME at directories inside the project
# dir, because GitLab CI can only cache paths under $CI_PROJECT_DIR.
#
# CARGO_HOME itself stays put: it also holds the mold linker config and the
# toolchain binaries, which must not be cached. On a cache miss the baked
# contents of the image seed the cache directories, so a miss is harmless.
#
# Keep the cached paths in sync with `.cargo-home-cache` in .gitlab-ci.yml
# (git/db, registry/cache, registry/index; git/checkouts is rebuilt by cargo).
set -euo pipefail

cargo_home="${CARGO_HOME:-/opt/cargo}"
project_dir="${CI_PROJECT_DIR:-$PWD}"
cache_root="$project_dir/.cargo-cache"

redirect() {
  local relative_path="$1"
  local target="$cache_root/$relative_path"
  local link="$cargo_home/$relative_path"

  [ "$(readlink "$link" || true)" = "$target" ] && return 0

  mkdir -p "$(dirname "$target")" "$(dirname "$link")"
  if [ ! -d "$target" ]; then
    if [ -d "$link" ]; then
      mv "$link" "$target"
    else
      mkdir -p "$target"
    fi
  fi
  rm -rf "$link"
  ln -s "$target" "$link"
}

# The cache is shared between MR pipelines and cargo trusts what it finds, so
# keep only crate archives whose sha256 matches Cargo.lock. Anything else is
# deleted and cargo downloads it again.
drop_unverified_crates() {
  local lockfile="$project_dir/Cargo.lock"
  [ -f "$lockfile" ] || return 0

  local expected
  expected="$(mktemp)"
  awk -F'"' '/^name =/ {name = $2} /^version =/ {version = $2}
             /^checksum =/ {print $2, name "-" version ".crate"}' "$lockfile" > "$expected"

  find "$cache_root/registry/cache" -name '*.crate' -print0 \
    | xargs -0 -r sha256sum \
    | awk 'NR == FNR {wanted[$2] = $1; next}
           {n = split($2, parts, "/"); if (wanted[parts[n]] != $1) print $2}' "$expected" - \
    | xargs -r rm -f
  rm -f "$expected"
}

redirect git
redirect registry/cache
redirect registry/index
drop_unverified_crates
