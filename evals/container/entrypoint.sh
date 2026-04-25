#!/bin/bash
set -e

# /mnt/workspace is the read-only bind mount from the host.
# /workspace is a writable tmpfs. Copy contents so the agent can read/write.
if [ -d /mnt/workspace ]; then
    cp -a /mnt/workspace/. /workspace/
fi

cd /workspace
exec opencode serve --port "${PORT:-4096}" --print-logs
