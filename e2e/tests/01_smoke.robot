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

Enable Feature Flag
    [Arguments]    ${flag}
    ${headers}=    GitLab Auth Headers
    ${resp}=    POST    ${GITLAB_URL}/api/v4/features/${flag}
    ...    headers=${headers}    data=value=true    expected_status=201

Feature Flag Is Enabled
    [Arguments]    ${flag}
    ${headers}=    GitLab Auth Headers
    ${resp}=    GET    ${GITLAB_URL}/api/v4/features    headers=${headers}    expected_status=200
    ${flags}=    Set Variable    ${resp.json()}
    FOR    ${f}    IN    @{flags}
        IF    "${f["name"]}" == "${flag}"
            Should Be Equal As Strings    ${f["state"]}    on    Feature flag ${flag} not enabled
            RETURN
        END
    END
    Fail    Feature flag ${flag} not found

Users Indexed In Graph
    ${headers}=    GitLab Auth Headers
    ${agg}=    Create Dictionary    function=count    target=n
    ${node}=    Create Dictionary    id=n    entity=User
    ${query}=    Create Dictionary    query_type=aggregation    nodes=${{[${node}]}}    aggregations=${{[${agg}]}}
    ${body}=    Create Dictionary    query=${query}
    ${resp}=    POST    ${GITLAB_URL}/api/v4/orbit/query
    ...    headers=${headers}    json=${body}    expected_status=200
    ${count}=    Set Variable    ${resp.json()["result"]["columns"][0]["value"]}
    Should Be True    ${count} >= 1    No users indexed in graph (count=${count})

*** Test Cases ***
GitLab Is Ready
    [Documentation]    Wait for GitLab API to respond before testing Orbit
    Wait Until Keyword Succeeds    60s    3s    GitLab API Is Ready

Feature Flags Are Enabled
    [Documentation]    Enable knowledge graph feature flags via API and verify
    Enable Feature Flag    knowledge_graph_infra
    Enable Feature Flag    knowledge_graph
    Wait Until Keyword Succeeds    30s    3s    Feature Flag Is Enabled    knowledge_graph_infra

Orbit Is Healthy
    [Documentation]    Wait for all components (GKG, Siphon, NATS, ClickHouse) to report healthy
    Wait Until Keyword Succeeds    30s    3s    Orbit Status Is Healthy

User Data Is Available Via Orbit Query
    [Documentation]    Verify full data pipeline: PG -> Siphon -> ClickHouse -> GKG indexer -> Orbit API
    Wait Until Keyword Succeeds    60s    3s    Users Indexed In Graph
