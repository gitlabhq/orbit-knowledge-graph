#!/bin/bash
set -e

# /mnt/workspace is the read-only bind mount from the host.
# Copy it to /workspace so the agent can read/write freely.
if [ -d /mnt/workspace ]; then
    cp -a /mnt/workspace /workspace
else
    mkdir -p /workspace
fi

cd /workspace
exec opencode serve --port "${PORT:-4096}" --print-logs
