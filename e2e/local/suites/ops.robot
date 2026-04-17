*** Settings ***
Documentation    O-series: ops/integration checks — webserver health and
...              metrics alongside graph_status calls.
Resource         ../lib/common.resource

*** Variables ***
${ORBIT_STATUS_PATH}    /api/v4/orbit/status

*** Test Cases ***
O1 Orbit Status Healthy While Graph Status Works
    ${headers}=    Auth Headers
    ${health}=    GET    ${GITLAB_URL}${ORBIT_STATUS_PATH}    headers=${headers}    verify=${VERIFY_SSL}
    Should Be Equal As Integers    ${health.status_code}    200
    Should Be Equal    ${health.json()["status"]}    healthy

    ${r}=    Graph Status By Namespace    22    200
    Assert Response Shape    ${r.json()}
