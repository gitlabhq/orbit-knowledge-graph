#!/usr/bin/env bash
# Installs the kube tooling the e2e job needs. Idempotent so it can be called
# from both before_script and after_script (GitLab SaaS runs after_script in a
# fresh container, so anything installed during the job is gone by then).
set -euo pipefail

KUBECTL_VERSION="${KUBECTL_VERSION:-1.35.2}"
HELM_VERSION="${HELM_VERSION:-3.20.0}"
HELMFILE_VERSION="${HELMFILE_VERSION:-1.4.2}"

if ! command -v openssl >/dev/null 2>&1 || ! command -v python3 >/dev/null 2>&1; then
  apt-get update -qq
  apt-get install -y -qq openssl python3
fi

if ! command -v kubectl >/dev/null 2>&1; then
  curl -sLO "https://dl.k8s.io/release/v${KUBECTL_VERSION}/bin/linux/amd64/kubectl"
  chmod +x kubectl
  mv kubectl /usr/local/bin/
fi

if ! command -v helm >/dev/null 2>&1; then
  curl -sL "https://get.helm.sh/helm-v${HELM_VERSION}-linux-amd64.tar.gz" | tar xz
  mv linux-amd64/helm /usr/local/bin/
  rm -rf linux-amd64
fi

if ! command -v helmfile >/dev/null 2>&1; then
  curl -sL "https://github.com/helmfile/helmfile/releases/download/v${HELMFILE_VERSION}/helmfile_${HELMFILE_VERSION}_linux_amd64.tar.gz" \
    | tar xz -C /usr/local/bin
fi
