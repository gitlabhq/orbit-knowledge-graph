# Pins mirror e2e/charts/robot-runner/values.yaml; tag = hash of this file.
FROM python:3.12-slim@sha256:86d3e4424d5e963e60594a3a6b4d597cc4d41f5152fe67a97a40dca9ea092475

RUN apt-get update -qq \
  && apt-get install -qq -y --no-install-recommends git ca-certificates \
  && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --disable-pip-version-check --root-user-action=ignore \
  robotframework==7.4.2 \
  robotframework-requests==0.9.7 \
  robotframework-pabot==5.2.2
