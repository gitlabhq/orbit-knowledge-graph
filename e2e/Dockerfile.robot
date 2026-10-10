# Pins mirror e2e/charts/robot-runner/values.yaml; tag = hash of this file.
FROM python:3.14-slim@sha256:f85c5697265c178cc6887276c55fe16cf3d14ca35c3df6a5eab3b360534a55d2

RUN apt-get update -qq \
  && apt-get install -qq -y --no-install-recommends git ca-certificates \
  && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --disable-pip-version-check --root-user-action=ignore \
  robotframework==7.5 \
  robotframework-requests==0.9.7 \
  robotframework-pabot==5.2.2
