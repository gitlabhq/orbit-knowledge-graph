#!/usr/bin/env bash
# Print the CNG toolbox image that e2e pins, so the contract matches the e2e GitLab.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
digest=$(yq -r '.gitlab.images.toolbox.digest' "$ROOT/e2e/config/versions.yaml")
echo "registry.gitlab.com/gitlab-org/build/cng/gitlab-toolbox-ee@${digest}"
