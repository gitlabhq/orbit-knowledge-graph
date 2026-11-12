#!/usr/bin/env bash
# Starts a throwaway local Grafana with GitLab-shaped dummy data and prints a Viewer token.
# Re-running it replaces the container and its data.
set -euo pipefail
NAME=orbit-grafana-dev
PORT=${PORT:-3000}
URL="http://localhost:$PORT"
ADMIN="admin:orbit-dev"

docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" -p "$PORT:3000" \
  -e GF_SECURITY_ADMIN_PASSWORD=orbit-dev \
  -e GF_AUTH_ANONYMOUS_ENABLED=false \
  -e GF_ANALYTICS_REPORTING_ENABLED=false \
  -e GF_NEWS_NEWS_FEED_ENABLED=false \
  grafana/grafana:13.0.2 >/dev/null

for _ in $(seq 1 60); do
  curl -sf "$URL/api/health" >/dev/null && break
  sleep 1
done
curl -sf "$URL/api/health" >/dev/null || { echo "Grafana did not start" >&2; exit 1; }

api() { curl -sf -u "$ADMIN" -H 'Content-Type: application/json' "$@"; }

folder() { api -X POST "$URL/api/folders" -d "{\"uid\":\"$1\",\"title\":\"$2\"}" >/dev/null; }
folder gitlab-com "GitLab.com"
folder orbit "Orbit"
folder ci-runners "CI Runners"

dashboard() {
  local uid=$1 title=$2 folder=$3 tags=$4
  api -X POST "$URL/api/dashboards/db" -d "{
    \"folderUid\": \"$folder\", \"overwrite\": true,
    \"dashboard\": {\"uid\": \"$uid\", \"title\": \"$title\", \"tags\": $tags, \"panels\": [], \"schemaVersion\": 39}
  }" >/dev/null
}
dashboard web-overview      "Web: Overview"                gitlab-com '["web","slo","gitlab.com"]'
dashboard api-overview      "API: Overview"                gitlab-com '["api","slo","gitlab.com"]'
dashboard patroni-repl      "Patroni: Postgres replication" gitlab-com '["database","postgres","patroni"]'
dashboard sidekiq-queues    "Sidekiq: Queue detail"        gitlab-com '["sidekiq","queues"]'
dashboard orbit-indexer     "Orbit: Indexer"               orbit      '["orbit","indexer","clickhouse"]'
dashboard orbit-webserver   "Orbit: Webserver latency"     orbit      '["orbit","webserver","slo"]'
dashboard orbit-clickhouse  "Orbit: ClickHouse"            orbit      '["orbit","clickhouse","database"]'
dashboard runners-saas      "Runners: SaaS Linux fleet"    ci-runners '["ci","runners"]'

ds=$(api -X POST "$URL/api/datasources" -d '{"name":"testdata","type":"grafana-testdata-datasource","access":"proxy"}' | sed -E 's/.*"uid":"([^"]+)".*/\1/')

rule() {
  local uid=$1 title=$2 folder=$3 group=$4 dashboard=$5 summary=$6 paused=$7
  api -X POST "$URL/api/v1/provisioning/alert-rules" -H 'X-Disable-Provenance: true' -d "{
    \"uid\": \"$uid\", \"title\": \"$title\", \"folderUID\": \"$folder\", \"ruleGroup\": \"$group\",
    \"condition\": \"A\", \"for\": \"5m\", \"noDataState\": \"OK\", \"execErrState\": \"OK\", \"isPaused\": $paused,
    \"annotations\": {\"summary\": \"$summary\", \"runbook_url\": \"https://runbooks.example.com/$uid\", \"__dashboardUid__\": \"$dashboard\", \"__panelId__\": \"1\"},
    \"data\": [{\"refId\": \"A\", \"datasourceUid\": \"$ds\", \"relativeTimeRange\": {\"from\": 600, \"to\": 0},
              \"model\": {\"refId\": \"A\", \"scenarioId\": \"random_walk\"}}]
  }" >/dev/null
}
rule web-error-rate      "Web: error rate above SLO"        gitlab-com web     web-overview     "web 5xx ratio above 0.1% for 5m" false
rule api-apdex           "API: apdex below SLO"             gitlab-com api     api-overview     "api apdex below 0.995 for 10m" false
rule patroni-lag         "Patroni: replication lag"         gitlab-com database patroni-repl    "replica lag above 30s" false
rule sidekiq-backlog     "Sidekiq: urgent queue backlog"    gitlab-com sidekiq sidekiq-queues   "urgent-other queue above 10k jobs" true
rule orbit-indexer-stuck "Orbit: indexer stopped advancing" orbit      orbit   orbit-indexer    "no projects indexed for 30m; check the ClickHouse migration" false
rule orbit-p99           "Orbit: webserver p99 latency"     orbit      orbit   orbit-webserver  "orbit query p99 above 2s for 10m" false
rule orbit-ch-disk       "Orbit: ClickHouse disk usage"     orbit      orbit   orbit-clickhouse "ClickHouse disk above 85%" false

sa=$(api -X POST "$URL/api/serviceaccounts" -d '{"name":"orbit-dev","role":"Viewer"}' | sed -E 's/.*"id":([0-9]+).*/\1/')
token=$(api -X POST "$URL/api/serviceaccounts/$sa/tokens" -d '{"name":"orbit-dev"}' | sed -E 's/.*"key":"([^"]+)".*/\1/')

echo "Grafana is up at $URL (admin / orbit-dev): 3 folders, 8 dashboards, 7 alert rules."
echo "Viewer token: $token"
