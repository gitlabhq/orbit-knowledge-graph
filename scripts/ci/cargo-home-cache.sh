#!/usr/bin/env bash
set -euo pipefail

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

[ -L "$cache_root" ] && rm -f "$cache_root"
if [ -d "$cache_root" ]; then
  find "$cache_root" -mindepth 1 ! -type d ! -type f -delete
fi

redirect git
redirect registry/cache
redirect registry/index
drop_unverified_crates
