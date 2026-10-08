#!/usr/bin/env bash
# Point the cacheable parts of CARGO_HOME at directories inside the project
# dir, because GitLab CI can only cache paths under $CI_PROJECT_DIR.
#
# CARGO_HOME itself stays put: it also holds the mold linker config and the
# toolchain binaries, which must not be cached. On a cache miss the baked
# contents of the image seed the cache directories, so a miss is harmless.
set -euo pipefail

cargo_home="${CARGO_HOME:-/opt/cargo}"
cache_root="${CI_PROJECT_DIR:-$PWD}/.cargo-cache"

redirect() {
  local relative_path="$1"
  local target="$cache_root/$relative_path"
  local link="$cargo_home/$relative_path"

  [ -L "$link" ] && return 0

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

redirect git
redirect registry/cache
redirect registry/index
