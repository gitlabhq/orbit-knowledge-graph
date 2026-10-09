#!/usr/bin/env bash
# GitLab only caches paths under $CI_PROJECT_DIR, so symlink the downloadable
# parts of CARGO_HOME (git/db, registry/cache, registry/index) into
# $CI_PROJECT_DIR/.cargo-cache. CARGO_HOME itself is not moved because it also
# holds the toolchain and the mold linker config. On a cache miss the image's
# baked copies seed the directories, so a miss costs nothing extra.
#
# Trust: only unit-test writes the cache, from MR and main pipelines, and the
# test jobs of both read it. Anyone who can open an MR can already run code in
# those jobs, so the cache grants nothing new. Release and image jobs neither
# read nor write it. Even so, restored content is not trusted blindly: crate
# archives are checked against Cargo.lock and symlinks are removed. git/db and
# registry/index are not verified.
#
# Keep the paths in sync with `.cargo-home-cache` in .gitlab-ci.yml.
set -euo pipefail

# Set by .no-cargo-home-cache for jobs that restore no cache.
[ -z "${SKIP_CARGO_HOME_CACHE:-}" ] || exit 0

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

# Delete crate archives whose sha256 differs from Cargo.lock; cargo refetches them.
drop_unverified_crates() {
  local lockfile="$project_dir/Cargo.lock"
  [ -f "$lockfile" ] || return 0

  local expected
  expected="$(mktemp)"
  awk -F'"' '/^name =/ {name = $2} /^version =/ {version = $2}
             /^checksum =/ {print $2, name "-" version ".crate"}' "$lockfile" > "$expected"

  find "$cache_root/registry/cache" -type f -name '*.crate' -print0 \
    | xargs -0 -r sha256sum \
    | awk 'NR == FNR {wanted[$2] = $1; next}
           {n = split($2, parts, "/"); if (wanted[parts[n]] != $1) print $2}' "$expected" - \
    | tr '\n' '\0' | xargs -0 -r rm -f
  rm -f "$expected"
}

# Cargo follows symlinks, which would bypass the checksum check above. Cargo
# creates none in these directories.
[ -L "$cache_root" ] && rm -f "$cache_root"
if [ -d "$cache_root" ]; then
  find "$cache_root" -mindepth 1 ! -type d ! -type f -delete
fi

redirect git
redirect registry/cache
redirect registry/index
drop_unverified_crates
