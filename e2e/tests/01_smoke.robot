*** Settings ***
Library    RequestsLibrary
Library    Collections

*** Variables ***
${GITLAB_URL}     %{GITLAB_URL}
${GITLAB_PAT}     %{GITLAB_PAT}

*** Keywords ***
GitLab Auth Headers
    RETURN    ${{{"PRIVATE-TOKEN": "${GITLAB_PAT}", "Content-Type": "application/json"}}}

GitLab API Is Ready
    ${headers}=    GitLab Auth Headers
    ${resp}=    GET    ${GITLAB_URL}/api/v4/user    headers=${headers}    expected_status=200

Orbit Status Is Healthy
    ${headers}=    GitLab Auth Headers
    ${resp}=    GET    ${GITLAB_URL}/api/v4/orbit/status    headers=${headers}    expected_status=200
    Should Be Equal    ${resp.json()["status"]}    healthy

User Exists In Graph
    ${headers}=    GitLab Auth Headers
    ${node}=    Create Dictionary    id=n    entity=User    node_ids=${{[1]}}
    ${query}=    Create Dictionary    query_type=search    node=${node}
    ${body}=    Create Dictionary    query=${query}
    ${resp}=    POST    ${GITLAB_URL}/api/v4/orbit/query
    ...    headers=${headers}    json=${body}    expected_status=200
    Should Be True    ${resp.json()["row_count"]} >= 1    No users found in graph

*** Test Cases ***
GitLab Is Ready
    [Documentation]    Wait for GitLab API to respond before testing Orbit
    Wait Until Keyword Succeeds    60s    3s    GitLab API Is Ready

Orbit Is Healthy
    [Documentation]    Wait for all components (GKG, Siphon, NATS, ClickHouse) to report healthy
    Wait Until Keyword Succeeds    30s    3s    Orbit Status Is Healthy

User Data Is Available Via Orbit Query
    [Documentation]    Verify full data pipeline: PG -> Siphon -> ClickHouse -> GKG indexer -> Orbit API
    Wait Until Keyword Succeeds    60s    3s    User Exists In Graph
