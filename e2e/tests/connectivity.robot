*** Settings ***
Library    RequestsLibrary

*** Variables ***
${NATS_URL}          %{NATS_URL}
${CLICKHOUSE_URL}    %{CLICKHOUSE_URL}
${GITLAB_URL}        %{GITLAB_URL}
${GKG_URL}           %{GKG_URL}

*** Test Cases ***
NATS Monitor Is Healthy
    GET    ${NATS_URL}/healthz    expected_status=200

ClickHouse HTTP Is Reachable
    GET    ${CLICKHOUSE_URL}/ping    expected_status=200

GitLab Webservice Is Ready
    GET    ${GITLAB_URL}/-/readiness    expected_status=200

GKG Liveness Probe Passes
    GET    ${GKG_URL}/live    expected_status=200

GKG Readiness Probe Passes
    GET    ${GKG_URL}/ready    expected_status=200
