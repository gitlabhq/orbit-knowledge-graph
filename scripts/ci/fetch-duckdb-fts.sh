#!/usr/bin/env bash
# Fetch the official DuckDB fts extension artifact for a Rust target and
# export ORBIT_BUNDLED_FTS / ORBIT_BUNDLED_FTS_VERSION for the cargo build
# (consumed by crates/duckdb-client/build.rs, which embeds the artifact).
#
# Usage:
#   source scripts/ci/fetch-duckdb-fts.sh <rust-target>   # no-op for musl targets
#   scripts/ci/fetch-duckdb-fts.sh --print-checksums      # regenerate the manifest
#
# The DuckDB version is derived from the duckdb crate version in Cargo.lock
# (1.10505.0 encodes DuckDB 1.5.5), so the embedded artifact cannot drift
# from the linked engine. extensions.duckdb.org serves over plain HTTP with
# no published checksums, so downloads are verified against the manifest
# pinned in scripts/ci/duckdb-fts-checksums.sha256.
#
# Static musl binaries cannot dlopen extensions, so musl targets are skipped
# and keep the runtime INSTALL fallback (which fails the same way today).

_fts_repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
_fts_manifest="$_fts_repo_root/scripts/ci/duckdb-fts-checksums.sha256"
_fts_platforms="linux_amd64 linux_arm64 osx_amd64 osx_arm64 windows_amd64"

_fts_sha256() {
    if command -v sha256sum >/dev/null; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

_fts_duckdb_version() {
    local crate enc
    crate=$(awk -F'"' '/^name = "duckdb"$/ { getline; print $2 }' "$_fts_repo_root/Cargo.lock")
    enc=${crate#1.}
    enc=${enc%%.*}
    if [ "${#enc}" -ne 5 ]; then
        echo "cannot derive a DuckDB version from duckdb crate version '$crate'" >&2
        return 1
    fi
    echo "v${enc:0:1}.$((10#${enc:1:2})).$((10#${enc:3:2}))"
}

_fts_platform_for_target() {
    case "$1" in
        aarch64-apple-darwin)      echo osx_arm64 ;;
        x86_64-apple-darwin)       echo osx_amd64 ;;
        x86_64-unknown-linux-gnu)  echo linux_amd64 ;;
        aarch64-unknown-linux-gnu) echo linux_arm64 ;;
        x86_64-pc-windows-gnullvm) echo windows_amd64 ;;
        *-musl) ;;
        *)
            echo "no DuckDB extension platform mapping for target: $1" >&2
            return 1
            ;;
    esac
}

_fts_download() {
    local rel=$1 dest=$2
    mkdir -p "$(dirname "$dest")"
    curl -fsSL -o "$dest" "http://extensions.duckdb.org/$rel"
}

_fts_fetch() {
    local target=$1 version platform rel dest expected actual
    version=$(_fts_duckdb_version) || return 1
    platform=$(_fts_platform_for_target "$target") || return 1
    if [ -z "$platform" ]; then
        echo "skipping bundled fts for $target (static binaries cannot dlopen extensions)"
        return 0
    fi

    rel="$version/$platform/fts.duckdb_extension.gz"
    dest="$_fts_repo_root/target/duckdb-fts/$rel"
    expected=$(awk -v rel="$rel" '$2 == rel { print $1 }' "$_fts_manifest")
    if [ -z "$expected" ]; then
        echo "no pinned checksum for $rel in $_fts_manifest;" \
             "regenerate with: scripts/ci/fetch-duckdb-fts.sh --print-checksums" >&2
        return 1
    fi

    if [ ! -f "$dest" ] || [ "$(_fts_sha256 "$dest")" != "$expected" ]; then
        _fts_download "$rel" "$dest"
    fi
    actual=$(_fts_sha256 "$dest")
    if [ "$actual" != "$expected" ]; then
        echo "checksum mismatch for $rel: expected $expected, got $actual" >&2
        return 1
    fi

    export ORBIT_BUNDLED_FTS="$dest"
    export ORBIT_BUNDLED_FTS_VERSION="$version"
    echo "bundling fts $version/$platform into the build"
}

_fts_print_checksums() {
    local version platform rel dest
    version=$(_fts_duckdb_version) || return 1
    cat <<'HEADER'
# SHA-256 of the official DuckDB fts extension artifacts embedded into
# release binaries by scripts/ci/fetch-duckdb-fts.sh.
#
# extensions.duckdb.org serves over plain HTTP and publishes no out-of-band
# checksums, so these are recorded at review time and pinned. When the duckdb
# crate is bumped (or upstream republishes an artifact and a release build
# fails checksum verification), regenerate with:
#
#   scripts/ci/fetch-duckdb-fts.sh --print-checksums > scripts/ci/duckdb-fts-checksums.sha256
#
# and review the diff before merging.
HEADER
    for platform in $_fts_platforms; do
        rel="$version/$platform/fts.duckdb_extension.gz"
        dest=$(mktemp)
        _fts_download "$rel" "$dest" >&2 || return 1
        printf '%s  %s\n' "$(_fts_sha256 "$dest")" "$rel"
        rm -f "$dest"
    done
}

if [ "${1:-}" = "--print-checksums" ]; then
    _fts_print_checksums
elif [ -n "${1:-}" ]; then
    _fts_fetch "$1"
else
    echo "usage: source $0 <rust-target> | $0 --print-checksums" >&2
    false
fi
