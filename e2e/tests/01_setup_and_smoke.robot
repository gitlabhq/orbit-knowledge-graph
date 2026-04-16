*** Settings ***
Library    RequestsLibrary
Library    Collections
Library    DateTime

Suite Setup    Bootstrap E2E Credentials

*** Variables ***
${GITLAB_URL}            %{GITLAB_URL}
${GITLAB_ROOT_PASS}      %{GITLAB_ROOT_PASSWORD}

*** Keywords ***
Get Root OAuth Token
    ${auth}=    Create Dictionary    grant_type=password    username=root    password=${GITLAB_ROOT_PASS}
    ${resp}=    POST    ${GITLAB_URL}/oauth/token    data=${auth}    expected_status=200
    RETURN    ${resp.json()["access_token"]}

Bootstrap E2E Credentials
    [Documentation]    Create e2e-bot user and PAT via GitLab API using root OAuth credentials
    ${token}=    Wait Until Keyword Succeeds    120s    5s    Get Root OAuth Token
    ${headers}=    Create Dictionary    Authorization=Bearer ${token}    Content-Type=application/json

    ${user_data}=    Create Dictionary
    ...    username=e2e-bot    email=e2e-bot@example.com    name=E2E Bot
    ...    password=E2eB0tP@ssw0rd!    skip_confirmation=true    admin=true
    ${resp}=    POST    ${GITLAB_URL}/api/v4/users
    ...    headers=${headers}    json=${user_data}    expected_status=any
    IF    ${resp.status_code} == 201
        ${user_id}=    Set Variable    ${resp.json()["id"]}
    ELSE IF    ${resp.status_code} == 409
        ${resp}=    GET    ${GITLAB_URL}/api/v4/users?username=e2e-bot
        ...    headers=${headers}    expected_status=200
        ${user_id}=    Set Variable    ${resp.json()[0]["id"]}
    ELSE
        Fail    Failed to create e2e-bot: ${resp.status_code} ${resp.text}
    END

    ${scopes}=    Create List    api    read_api
    ${expiry}=    Evaluate    (datetime.date.today() + datetime.timedelta(days=30)).isoformat()    modules=datetime
    ${pat_data}=    Create Dictionary    name=e2e-pat    scopes=${scopes}    expires_at=${expiry}
    ${resp}=    POST    ${GITLAB_URL}/api/v4/users/${user_id}/personal_access_tokens
    ...    headers=${headers}    json=${pat_data}    expected_status=201
    Set Global Variable    ${GITLAB_PAT}    ${resp.json()["token"]}
    Log    E2E credentials bootstrapped (user_id=${user_id})

GitLab Auth Headers
    RETURN    ${{{"PRIVATE-TOKEN": "${GITLAB_PAT}", "Content-Type": "application/json"}}}

Enable Feature Flag
    [Arguments]    ${flag}
    ${headers}=    Create Dictionary    PRIVATE-TOKEN=${GITLAB_PAT}
    ${data}=    Create Dictionary    value=true
    ${resp}=    POST    ${GITLAB_URL}/api/v4/features/${flag}
    ...    headers=${headers}    data=${data}    expected_status=any
    Should Be True    ${resp.status_code} in [200, 201]
    ...    Failed to enable ${flag}: ${resp.status_code} ${resp.text}

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

Orbit Status Is Healthy
    ${headers}=    GitLab Auth Headers
    ${resp}=    GET    ${GITLAB_URL}/api/v4/orbit/status    headers=${headers}    expected_status=200
    Should Be Equal    ${resp.json()["status"]}    healthy

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
Feature Flags Are Enabled
    [Documentation]    Enable knowledge graph feature flags via API and verify
    Enable Feature Flag    knowledge_graph_infra
    Enable Feature Flag    knowledge_graph
    Wait Until Keyword Succeeds    30s    3s    Feature Flag Is Enabled    knowledge_graph_infra
    Wait Until Keyword Succeeds    30s    3s    Feature Flag Is Enabled    knowledge_graph

Orbit Is Healthy
    [Documentation]    Wait for all components (GKG, Siphon, NATS, ClickHouse) to report healthy
    Wait Until Keyword Succeeds    30s    3s    Orbit Status Is Healthy

User Data Is Available Via Orbit Query
    [Documentation]    Verify full data pipeline: PG -> Siphon -> ClickHouse -> GKG indexer -> Orbit API
    Wait Until Keyword Succeeds    120s    3s    Users Indexed In Graph
