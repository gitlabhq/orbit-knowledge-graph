#!/bin/bash
set -e

if [ -d /mnt/workspace ]; then
    cp -a /mnt/workspace/. /workspace/
fi

# Configure provider credentials from env vars
mkdir -p /root/.local/share/opencode
auth="{}"
if [ -n "$ANTHROPIC_API_KEY" ]; then
    auth=$(echo "$auth" | python3 -c "import json,sys; d=json.load(sys.stdin); d['anthropic']={'type':'api','key':'$ANTHROPIC_API_KEY'}; print(json.dumps(d))")
fi
if [ -n "$OPENAI_API_KEY" ]; then
    auth=$(echo "$auth" | python3 -c "import json,sys; d=json.load(sys.stdin); d['openai']={'type':'api','key':'$OPENAI_API_KEY'}; print(json.dumps(d))")
fi
echo "$auth" > /root/.local/share/opencode/auth.json

cd /workspace
mise trust 2>/dev/null || true
exec opencode serve --port "${PORT:-4096}" --hostname 0.0.0.0 --print-logs
