#!/usr/bin/env bash
# GitLab only caches paths under $CI_PROJECT_DIR, so symlink the downloadable
# parts of CARGO_HOME (git, registry/cache, registry/index) into
# $CI_PROJECT_DIR/.cargo-cache. CARGO_HOME itself is not moved because it also
# holds the rustup proxies and the mold linker config. On a cache miss the
# image's baked copies seed the directories, so a miss costs nothing extra.
#
# Trust: only main pipelines write the cache. MR pipelines only read it, from
# the -protected key when a Maintainer started the pipeline and the
# -non_protected key otherwise. Release CLI builds skip it. Cargo does not
# re-check a cached .crate, so archives that don't match Cargo.lock are
# deleted, and so is every symlink. git/db and registry/index are not verified.
#
# Keep the cached paths in sync with `.cargo-home-cache` in .gitlab-ci.yml
# (git/checkouts is rebuilt by cargo).
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

# `find -type f` in drop_unverified_crates skips symlinks, so a symlinked
# .crate or directory would never be hashed while cargo still follows it.
# Cargo creates none in these directories.
[ -L "$cache_root" ] && rm -f "$cache_root"
if [ -d "$cache_root" ]; then
  find "$cache_root" -mindepth 1 ! -type d ! -type f -delete
fi

redirect git
redirect registry/cache
redirect registry/index
drop_unverified_crates
