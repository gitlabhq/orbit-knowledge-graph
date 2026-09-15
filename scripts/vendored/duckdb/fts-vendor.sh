#!/usr/bin/env bash
set -euo pipefail

# Vendor DuckDB extension artifacts: regenerate source archives for
# extensions with source_revision pins, and re-pin per-platform binary
# checksums for extensions with binaries.
#
# Called by `mise vendor -- duckdb` which sets:
#   VENDOR_VERSIONS_FILE  — absolute path to config/versions.yaml
#   VENDOR_DIR            — absolute path to the vendor directory
#   VENDOR_VERSION        — duckdb version (e.g. v1.5.5)
#   VENDOR_NAME           — "duckdb"
#
# Can also be called directly; falls back to repo-relative paths.

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)

VERSIONS_FILE="${VENDOR_VERSIONS_FILE:-$REPO_ROOT/config/versions.yaml}"
DUCKDB_VERSION="${VENDOR_VERSION:-$(yq '.vendored.duckdb.version' "$VERSIONS_FILE")}"
VENDOR_DIR="${VENDOR_DIR:-$REPO_ROOT/$(yq '.vendored.duckdb.vendor_dir' "$VERSIONS_FILE")}"

WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

vendor_source_archive() {
    local ext_name="$1"
    local revision
    revision=$(yq ".vendored.duckdb.extensions.$ext_name.source_revision" "$VERSIONS_FILE")
    [[ "$revision" == "null" ]] && return

    echo "=== Vendoring $ext_name source archive ==="

    local archive="$VENDOR_DIR/duckdb-${ext_name}-sources.tar.gz"
    local source_root="$WORK_DIR/stage/duckdb-${ext_name}-sources"

    export LC_ALL=C
    if [[ ! -d "$WORK_DIR/duckdb" ]]; then
        git -c advice.detachedHead=false clone --quiet --depth 1 --branch "$DUCKDB_VERSION" \
            --filter=blob:none --sparse https://github.com/duckdb/duckdb.git "$WORK_DIR/duckdb"
        git -C "$WORK_DIR/duckdb" sparse-checkout set third_party/snowball
    fi

    local ext_repo="$WORK_DIR/duckdb-${ext_name}"
    git init --quiet "$ext_repo"
    git -C "$ext_repo" remote add origin "https://github.com/duckdb/duckdb-${ext_name}.git"
    git -C "$ext_repo" fetch --quiet --depth 1 origin "$revision"
    git -C "$ext_repo" checkout --quiet --detach FETCH_HEAD

    mkdir -p "$source_root/fts/include"
    cp -R "$WORK_DIR/duckdb/third_party/snowball" "$source_root/snowball"
    rm -f "$source_root/snowball/CMakeLists.txt"
    cp "$ext_repo/extension/fts/"{fts_extension.cpp,fts_indexing.cpp,indexing.sql} \
        "$source_root/fts/"
    cp "$ext_repo/extension/fts/include/"{fts_extension.hpp,fts_indexing.hpp} \
        "$source_root/fts/include/"
    cp "$ext_repo/LICENSE" "$source_root/fts/"
    find "$source_root" -type d -exec chmod 0755 {} +
    find "$source_root" -type f -exec chmod 0644 {} +
    mkdir -p "$(dirname "$archive")"
    tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner --format=ustar \
        -C "$WORK_DIR/stage" "duckdb-${ext_name}-sources" | gzip -n > "$archive"

    local sha256
    sha256=$(sha256sum "$archive" | awk '{ print $1 }')
    yq -i ".vendored.duckdb.extensions.$ext_name.source_archive_sha256 = \"$sha256\"" "$VERSIONS_FILE"
    echo "  $archive: $sha256"
}

repin_binaries() {
    local ext_name="$1"
    local has_binaries
    has_binaries=$(yq ".vendored.duckdb.extensions.$ext_name.binaries" "$VERSIONS_FILE")
    [[ "$has_binaries" == "null" ]] && return

    echo "=== Re-pinning $ext_name binary checksums ==="

    for platform in $(yq ".vendored.duckdb.extensions.$ext_name.binaries | keys | .[]" "$VERSIONS_FILE"); do
        local url="http://extensions.duckdb.org/${DUCKDB_VERSION}/${platform}/${ext_name}.duckdb_extension.gz"
        local gz="$WORK_DIR/${ext_name}.${platform}.duckdb_extension.gz"

        if ! curl -sf --max-time 60 -o "$gz" "$url"; then
            echo "  WARNING: failed to download $url (skipping)" >&2
            continue
        fi

        local sha256
        sha256=$(sha256sum "$gz" | awk '{ print $1 }')
        yq -i ".vendored.duckdb.extensions.$ext_name.binaries.$platform = \"$sha256\"" "$VERSIONS_FILE"
        echo "  $platform: $sha256"
    done
}

for ext_name in $(yq '.vendored.duckdb.extensions | keys | .[]' "$VERSIONS_FILE"); do
    vendor_source_archive "$ext_name"
    repin_binaries "$ext_name"
done

echo "Done."
