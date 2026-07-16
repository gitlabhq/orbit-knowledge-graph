#!/usr/bin/env bash
set -euo pipefail

# Publish the local CLI to npm as @gitlab/orbit plus per-platform binary
# packages, staged from the checked-in templates under npm/. Authentication
# uses npm trusted publishing (OIDC); the CI job provides NPM_ID_TOKEN and no
# npm token is involved. Platform packages ship the signed release archives;
# Linux uses the static musl builds so one package covers glibc and musl
# distributions.
#
# Required env (provided by GitLab CI on tag pipelines):
#   CI_COMMIT_TAG

: "${CI_COMMIT_TAG:?CI_COMMIT_TAG is required}"

VERSION="${CI_COMMIT_TAG#v}"
PLATFORMS=(darwin-arm64 darwin-x64 linux-arm64 linux-x64 win32-x64)
STAGING=$(mktemp -d)

archive_for() {
  case "$1" in
    darwin-arm64) echo "orbit-local-darwin-aarch64.tar.gz" ;;
    darwin-x64)   echo "orbit-local-darwin-x86_64.tar.gz" ;;
    linux-arm64)  echo "orbit-local-linux-musl-aarch64.tar.gz" ;;
    linux-x64)    echo "orbit-local-linux-musl-x86_64.tar.gz" ;;
    win32-x64)    echo "orbit-local-windows-x86_64.zip" ;;
  esac
}

binary_for() {
  case "$1" in
    win32-x64) echo "orbit.exe" ;;
    *)         echo "orbit" ;;
  esac
}

already_published() {
  npm view "$1@$VERSION" version >/dev/null 2>&1
}

publish_package() {
  local pkg_dir="$1"
  local name
  name=$(cd "$pkg_dir" && npm pkg get name | tr -d '"')
  if already_published "$name"; then
    echo "$name@$VERSION already published; skipping"
    return 0
  fi
  echo "Publishing $name@$VERSION"
  (cd "$pkg_dir" && npm publish)
}

for platform in "${PLATFORMS[@]}"; do
  archive=$(archive_for "$platform")
  if [ ! -f "$archive" ]; then
    echo "missing build artifact: $archive" >&2
    exit 1
  fi
done

for platform in "${PLATFORMS[@]}"; do
  archive=$(archive_for "$platform")
  binary=$(binary_for "$platform")
  pkg_dir="$STAGING/orbit-$platform"

  cp -R "npm/orbit-$platform" "$pkg_dir"
  case "$archive" in
    *.zip)    unzip -q "$archive" "$binary" -d "$pkg_dir" ;;
    *.tar.gz) tar -xzf "$archive" -C "$pkg_dir" "$binary" ;;
  esac
  if [ ! -f "$pkg_dir/$binary" ]; then
    echo "archive $archive did not contain $binary" >&2
    exit 1
  fi
  chmod +x "$pkg_dir/$binary"
  cp LICENSE.md "$pkg_dir/"
  (cd "$pkg_dir" && npm pkg set version="$VERSION")

  publish_package "$pkg_dir"
done

pkg_dir="$STAGING/orbit"
cp -R npm/orbit "$pkg_dir"
cp LICENSE.md "$pkg_dir/"
(
  cd "$pkg_dir"
  npm pkg set version="$VERSION"
  for platform in "${PLATFORMS[@]}"; do
    npm pkg set "optionalDependencies.@gitlab/orbit-$platform=$VERSION"
  done
)

publish_package "$pkg_dir"

echo "Done."
