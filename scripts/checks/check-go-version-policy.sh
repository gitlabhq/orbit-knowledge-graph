#!/usr/bin/env bash
# Check the Go modules in this repo and the Go images CI uses for them against
# the Go versions GitLab ships:
# https://docs.gitlab.com/development/go_guide/go_upgrade/#supporting-multiple-go-versions
#
# For every go.mod:
#   - the go directive patch version must be .0
#   - the go directive minor and any toolchain directive minor must not exceed
#     the oldest minor CNG ships on the last three released N-N-stable branches
# For every golang:X.Y image in CI_FILE:
#   - the minor must not exceed that same minor
#   - the minor must be at least every module's go minor (golang images set
#     GOTOOLCHAIN=local, so an older image cannot build the module)
# Only a scalar `image: golang:X.Y` line in CI_FILE is detected; images set
# through variables, anchors or other files are not checked.
# All violations are reported before the script exits non-zero. Failures to
# fetch or parse CNG data also exit non-zero.
#
# Offline testing overrides (each one skips the matching lookup):
#   SHIPPED_GO_VERSIONS  CNG GO_VERSION of the last three released stable
#                        branches, e.g. "1.26.10 1.27.3 1.27.3"
#   GO_MODS              space-separated go.mod paths (default: all in the repo)
#   CI_FILE              CI config to scan for golang images (default .gitlab-ci.yml)
set -euo pipefail
shopt -s inherit_errexit

CI_FILE="${CI_FILE:-.gitlab-ci.yml}"
CNG_API="https://gitlab.com/api/v4/projects/gitlab-org%2Fbuild%2FCNG"

die() {
  echo "Error: $*" >&2
  exit 1
}

fetch() {
  curl --silent --show-error --fail --retry 3 --location "$1" || die "failed to download $1"
}

minor_of() {
  echo "$1" | cut -d. -f1,2
}

# True when minor $1 is greater than minor $2.
minor_gt() {
  [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | tail -n 1)" = "$1" ] && [ "$1" != "$2" ]
}

list_cng_stable_branches() {
  local page=1 body names
  while :; do
    body=$(fetch "${CNG_API}/repository/branches?regex=%5E%5B0-9%5D%2B-%5B0-9%5D%2B-stable%24&per_page=100&page=${page}") || exit 1
    names=$(echo "${body}" | grep -oE '"name":"[0-9]+-[0-9]+-stable"' | cut -d'"' -f4 || true)
    [ -n "${names}" ] || break
    echo "${names}"
    page=$((page + 1))
  done
}

# CNG cuts a stable branch a few days before the release, and the policy
# excludes the active milestone, so the newest branch is dropped until CNG has
# tagged its vX.Y.0.
fetch_shipped_go_versions() {
  local branches newest_tag tag_status branch stable_yml stable_version versions=""
  branches=$(list_cng_stable_branches | sort -uV | tail -n 4)
  [ "$(echo "${branches}" | grep -c .)" -eq 4 ] || die "could not list four CNG N-N-stable branches"
  newest_tag="v$(echo "${branches}" | tail -n 1 | sed -E 's/-stable$//; s/-/./').0"
  tag_status=$(curl --silent --output /dev/null --write-out '%{http_code}' --retry 3 "${CNG_API}/repository/tags/${newest_tag}" || true)
  case "${tag_status}" in
    200) branches=$(echo "${branches}" | tail -n 3) ;;
    404) branches=$(echo "${branches}" | head -n 3) ;;
    *) die "failed to look up CNG tag ${newest_tag} (HTTP ${tag_status})" ;;
  esac
  [ "$(echo "${branches}" | grep -c .)" -eq 3 ] || die "could not select three CNG N-N-stable branches"
  for branch in ${branches}; do
    stable_yml=$(fetch "${CNG_API}/repository/files/ci_files%2Fvariables.yml/raw?ref=${branch}")
    stable_version=$(echo "${stable_yml}" | grep -E "^\s*GO_VERSION:" | awk '{print $2}' | tr -d '"' | head -n 1 || true)
    [ -n "${stable_version}" ] || die "no GO_VERSION in CNG ${branch} ci_files/variables.yml"
    [[ "${stable_version}" =~ ^[0-9]+\.[0-9]+(\.[0-9]+)?$ ]] || die "unexpected GO_VERSION '${stable_version}' in CNG ${branch} ci_files/variables.yml"
    echo "CNG ${branch} GO_VERSION: ${stable_version}" >&2
    versions="${versions} ${stable_version}"
  done
  echo "${versions}"
}

if [ -z "${SHIPPED_GO_VERSIONS:-}" ]; then
  echo "Fetching GO_VERSION from the last three released CNG stable branches..."
  SHIPPED_GO_VERSIONS=$(fetch_shipped_go_versions)
fi
OLDEST_SHIPPED_MINOR=$(for version in ${SHIPPED_GO_VERSIONS}; do
  [[ "${version}" =~ ^[0-9]+\.[0-9]+(\.[0-9]+)?$ ]] || die "unexpected shipped Go version '${version}'"
  minor_of "${version}"
done | sort -uV | head -n 1)
[ -n "${OLDEST_SHIPPED_MINOR}" ] || die "no shipped Go versions to compare against"
echo "Oldest Go minor shipped in the last three GitLab releases: ${OLDEST_SHIPPED_MINOR}"

if [ -z "${GO_MODS:-}" ]; then
  GO_MODS=$(find . -name go.mod -not -path '*/node_modules/*' -not -path './target/*' -not -path './.git/*' | sort)
fi
[ -n "${GO_MODS}" ] || die "no go.mod files found"

RESULT=0
violation() {
  echo "Error: $*"
  RESULT=1
}

HIGHEST_MODULE_MINOR=""
for go_mod in ${GO_MODS}; do
  [ -f "${go_mod}" ] || die "${go_mod} does not exist"
  go_version=$(awk '/^go[[:space:]]+[0-9]/ { print $2; exit }' "${go_mod}" | tr -d '\r')
  [ -n "${go_version}" ] || die "could not find a go directive in ${go_mod}"
  toolchain=$(awk '/^toolchain[[:space:]]+go[0-9]/ { print $2; exit }' "${go_mod}" | tr -d '\r' | sed 's/^go//')
  echo "${go_mod}: go ${go_version}${toolchain:+, toolchain go${toolchain}}"

  if ! [[ "${go_version}" =~ ^[0-9]+\.[0-9]+(\.[0-9]+)?$ ]]; then
    violation "${go_mod}: the go directive (${go_version}) is not X.Y or X.Y.Z"
    continue
  fi
  if ! [[ "${go_version}" =~ ^[0-9]+\.[0-9]+(\.0)?$ ]]; then
    violation "${go_mod}: the go directive (${go_version}) must have a .0 patch version"
  fi
  module_minor=$(minor_of "${go_version}")
  if minor_gt "${module_minor}" "${OLDEST_SHIPPED_MINOR}"; then
    violation "${go_mod}: the go directive requires Go ${module_minor}, but the oldest Go shipped in the last three GitLab releases is ${OLDEST_SHIPPED_MINOR}"
  fi
  if [ -n "${toolchain}" ]; then
    toolchain_minor=$(minor_of "${toolchain}")
    if minor_gt "${toolchain_minor}" "${OLDEST_SHIPPED_MINOR}"; then
      violation "${go_mod}: the toolchain directive (go${toolchain}) is newer than the oldest Go shipped in the last three GitLab releases (${OLDEST_SHIPPED_MINOR})"
    fi
  fi
  if [ -z "${HIGHEST_MODULE_MINOR}" ] || minor_gt "${module_minor}" "${HIGHEST_MODULE_MINOR}"; then
    HIGHEST_MODULE_MINOR="${module_minor}"
  fi
done

IMAGES=$(grep -oE '^[[:space:]]*(-[[:space:]]*)?image:[[:space:]]*["'\'']?golang:[0-9]+\.[0-9]+[^[:space:]"'\'']*' "${CI_FILE}" | sed -E 's/.*golang:([0-9]+\.[0-9]+).*/\1/' | sort -uV || true)
if [ -z "${IMAGES}" ]; then
  echo "No golang images in ${CI_FILE}, skipping the image check."
fi
for image_minor in ${IMAGES}; do
  echo "${CI_FILE}: golang image ${image_minor}"
  if minor_gt "${image_minor}" "${OLDEST_SHIPPED_MINOR}"; then
    violation "${CI_FILE}: the golang:${image_minor} image is newer than the oldest Go shipped in the last three GitLab releases (${OLDEST_SHIPPED_MINOR})"
  fi
  if [ -n "${HIGHEST_MODULE_MINOR}" ] && minor_gt "${HIGHEST_MODULE_MINOR}" "${image_minor}"; then
    violation "${CI_FILE}: the golang:${image_minor} image is older than the Go ${HIGHEST_MODULE_MINOR} that a go.mod requires (golang images set GOTOOLCHAIN=local)"
  fi
done

exit "${RESULT}"
