#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../../"
set -a && source .env.local && set +a
export GKG__SCHEDULE__TASKS__GLOBAL__CRON='* * * * * *'
export GKG__SCHEDULE__TASKS__NAMESPACE__CRON='* * * * * *'
export GKG__SCHEDULE__TASKS__CODE_INDEXING_TASK__CRON='* * * * * *'
export GKG__SCHEDULE__TASKS__NAMESPACE_CODE_BACKFILL__CRON='* * * * * *'
exec target/release/gkg-server --mode=dispatch-indexing
