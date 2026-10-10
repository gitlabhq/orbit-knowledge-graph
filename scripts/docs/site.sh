#!/usr/bin/env bash
set -euo pipefail

DOCS_REPO="https://gitlab.com/gitlab-org/technical-writing/docs-gitlab-com.git"
ORBIT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SITE_DIR="${DOCS_SITE_DIR:-$ORBIT_ROOT/target/docs-site}"
DOCS_DIR="$SITE_DIR/docs-gitlab-com"
export MISE_CEILING_PATHS="$SITE_DIR" MISE_TRUSTED_CONFIG_PATHS="$DOCS_DIR"

docs_branch() {
    local branch=${CI_MERGE_REQUEST_TARGET_BRANCH_NAME:-}
    if [[ ! $branch =~ ^[0-9]+-[0-9]+-stable ]]; then
        echo main
        return
    fi

    local candidate=${branch%%-stable*}
    candidate=${candidate/-/.}
    if git ls-remote --heads --exit-code "$DOCS_REPO" "refs/heads/$candidate" >/dev/null 2>&1; then
        echo "Stable branch $branch: using docs-gitlab-com branch $candidate." >&2
        echo "$candidate"
    else
        echo "Stable branch $branch: docs-gitlab-com has no branch $candidate, using main." >&2
        echo main
    fi
}

run() {
    if command -v mise >/dev/null; then
        mise exec -- "$@"
    else
        "$@"
    fi
}

prepare_site() {
    local branch=${DOCS_BRANCH:-$(docs_branch)}
    if [[ ! -d $DOCS_DIR/.git ]]; then
        git clone --depth 1 --branch "$branch" "$DOCS_REPO" "$DOCS_DIR"
    fi
    ln -sfn "$ORBIT_ROOT" "$SITE_DIR/orbit"
    cd "$DOCS_DIR"

    local current
    current="$(git branch --show-current)"
    echo "docs-gitlab-com: $current $(git log -1 --format='%h, committed %cr: %s' HEAD)"
    if [[ $current != "$branch" ]]; then
        echo "WARNING: $DOCS_DIR is on branch '$current', not '$branch'. Using it as is."
    fi

    export PATH="$SITE_DIR/bin:$PATH" COREPACK_ENABLE_DOWNLOAD_PROMPT=0
    if ! run yarn --version >/dev/null 2>&1; then
        rm -rf "${SITE_DIR:?}/bin"
        mkdir "$SITE_DIR/bin"
        run corepack enable --install-directory "$SITE_DIR/bin" yarn
    fi
    run make install-nodejs-dependencies
}

check_navigation() {
    run make check-pages-not-in-nav

    local report broken
    report="$(run make check-global-navigation 2>&1 || true)"
    if ! grep -qE 'No broken links found|No sitemap entry found' <<<"$report" || grep -q 'Unexpected error' <<<"$report"; then
        echo "$report"
        echo "ERROR: the global navigation check did not finish."
        return 1
    fi
    broken="$(grep -E '(^|[[:space:]])/?orbit/' <<<"$report" || true)"
    if [[ -n $broken ]]; then
        echo "$broken"
        echo "ERROR: global navigation entries under orbit/ have no page."
        return 1
    fi
}

build() {
    run make add-latest-icons build-data
    rm -rf public
    run hugo --gc --printPathWarnings --panicOnWarning
    run make check-index-pages SEARCH_DIR=../orbit/docs/source
    check_navigation
}

case "${1:-}" in
    serve)
        prepare_site
        run make view
        ;;
    build)
        prepare_site
        build
        ;;
    *)
        echo "Usage: $0 serve|build" >&2
        exit 2
        ;;
esac
