#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/../.."

export GKG_GITLAB__JWT__VERIFYING_KEY="$(cat ~/gitlab/gdk/gitlab/.gitlab_knowledge_graph_secret)"
export GKG_GITLAB__JWT__SIGNING_KEY="$GKG_GITLAB__JWT__VERIFYING_KEY"
export GKG_GITLAB__BASE_URL="https://gdk.test:3443"
export GKG_NATS__URL="localhost:4222"
export GKG_GRAPH__URL="http://127.0.0.1:8123"
export GKG_GRAPH__DATABASE="gitlab_clickhouse_main_development"
export GKG_GRAPH__USERNAME="default"
export GKG_DATALAKE__URL="http://127.0.0.1:8123"
export GKG_DATALAKE__DATABASE="gitlab_clickhouse_development"
export GKG_DATALAKE__USERNAME="default"
export GKG_BIND_ADDRESS="127.0.0.1:4200"
export GKG_GRPC_BIND_ADDRESS="127.0.0.1:50054"
export GKG_INDEXER_HEALTH_BIND_ADDRESS="127.0.0.1:4202"
export GKG_DISPATCHER_HEALTH_BIND_ADDRESS="127.0.0.1:4203"
export GKG_ENGINE__HANDLERS__CODE_PUSH_EVENT__EVENTS_STREAM_NAME="siphon_stream"
export RUST_LOG=info

# Faster dispatch intervals for local e2e testing
export GKG_SCHEDULE__TASKS__GLOBAL__CRON="*/10 * * * * *"
export GKG_SCHEDULE__TASKS__NAMESPACE__CRON="*/10 * * * * *"

exec target/release/gkg-server --mode="$1"
