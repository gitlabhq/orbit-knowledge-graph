#!/usr/bin/env bash
# Point the cacheable parts of CARGO_HOME at directories inside the project
# dir, because GitLab CI can only cache paths under $CI_PROJECT_DIR.
#
# CARGO_HOME itself stays put: it also holds the mold linker config and the
# toolchain binaries, which must not be cached. On a cache miss the baked
# contents of the image seed the cache directories, so a miss is harmless.
#
# Trust model: MR pipelines targeting main share the protected cache with main
# and release pipelines, so poisoning reaches MR to main. That stays inside the
# existing boundary because those MR pipelines already get protected variables.
# Hence the symlink and checksum checks below; git/db and registry/index are
# not content-verified.
#
# Keep the cached paths in sync with `.cargo-home-cache` in .gitlab-ci.yml
# (git/db, registry/cache, registry/index; git/checkouts is rebuilt by cargo).
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

  find "$cache_root/registry/cache" -type f -name '*.crate' -print0 \
    | xargs -0 -r sha256sum \
    | awk 'NR == FNR {wanted[$2] = $1; next}
           {n = split($2, parts, "/"); if (wanted[parts[n]] != $1) print $2}' "$expected" - \
    | tr '\n' '\0' | xargs -0 -r rm -f
  rm -f "$expected"
}

# Cargo follows symlinks, so a symlink restored from the cache could point
# crates at files the checksum check never sees. Cargo creates none here.
[ -L "$cache_root" ] && rm -f "$cache_root"
if [ -d "$cache_root" ]; then
  find "$cache_root" -mindepth 1 ! -type d ! -type f -delete
fi

redirect git
redirect registry/cache
redirect registry/index
drop_unverified_crates
