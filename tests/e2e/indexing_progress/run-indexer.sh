#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../../"
set -a && source .env.local && set +a
exec target/release/gkg-server --mode=indexer
