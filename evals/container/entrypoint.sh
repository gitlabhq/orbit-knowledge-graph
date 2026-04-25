#!/bin/bash
set -e

if [ -d /mnt/workspace ]; then
    cp -a /mnt/workspace/. /workspace/
fi

cd /workspace
mise trust 2>/dev/null || true
exec opencode serve --port "${PORT:-4096}" --hostname 0.0.0.0 --print-logs
