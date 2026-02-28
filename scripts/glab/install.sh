#!/bin/sh
# Install the glab CLI from GitLab releases.
# Usage: scripts/glab/install.sh [version]

GLAB_VERSION="${1:-1.52.1}"

curl -fsSLo /tmp/glab.deb \
  "https://gitlab.com/gitlab-org/cli/-/releases/v${GLAB_VERSION}/downloads/glab_${GLAB_VERSION}_linux_amd64.deb"
dpkg -i /tmp/glab.deb
rm /tmp/glab.deb
