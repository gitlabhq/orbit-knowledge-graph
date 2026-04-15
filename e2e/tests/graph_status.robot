*** Settings ***
Documentation       E2E tests for GET /api/v4/orbit/graph_status.
...                 Verifies the REST API accepts namespace_id, project_id, and full_path
...                 parameters and returns correct graph status from NATS KV.

Library    RequestsLibrary
Library    Collections
Library    OperatingSystem
Library    String

Suite Setup    Suite Init


*** Variables ***
${GITLAB_URL}     %{GITLAB_URL}
${GITLAB_PAT}     %{GITLAB_PAT}


*** Keywords ***
GitLab Auth Headers
    RETURN    ${{{"PRIVATE-TOKEN": "${GITLAB_PAT}", "Content-Type": "application/json"}}}

Verify SSL Flag
    ${raw}=    Get Environment Variable    VERIFY_SSL    true
    ${flag}=    Evaluate    $raw.lower() not in ('false', '0', 'no')
    RETURN    ${flag}

Random Suffix
    ${rand}=    Generate Random String    6    [LOWER][NUMBERS]
    RETURN    ${rand}

Create Group
    [Arguments]    ${name}    ${path}
    ${headers}=    GitLab Auth Headers
    ${body}=    Create Dictionary    name=${name}    path=${path}    visibility=public
    ${resp}=    POST    ${GITLAB_URL}/api/v4/groups
    ...    headers=${headers}    json=${body}    expected_status=201    verify=${VERIFY_SSL}
    RETURN    ${resp.json()}

Create Subgroup
    [Arguments]    ${name}    ${path}    ${parent_id}
    ${headers}=    GitLab Auth Headers
    ${body}=    Create Dictionary    name=${name}    path=${path}    parent_id=${parent_id}    visibility=public
    ${resp}=    POST    ${GITLAB_URL}/api/v4/groups
    ...    headers=${headers}    json=${body}    expected_status=201    verify=${VERIFY_SSL}
    RETURN    ${resp.json()}

Create Project
    [Arguments]    ${name}    ${namespace_id}
    ${headers}=    GitLab Auth Headers
    ${body}=    Create Dictionary    name=${name}    namespace_id=${namespace_id}    visibility=public
    ${resp}=    POST    ${GITLAB_URL}/api/v4/projects
    ...    headers=${headers}    json=${body}    expected_status=201    verify=${VERIFY_SSL}
    RETURN    ${resp.json()}

Enable Knowledge Graph
    [Arguments]    ${namespace_id}
    Wait Until Keyword Succeeds    30s    5s
    ...    Enable Knowledge Graph Once    ${namespace_id}

Enable Knowledge Graph Once
    [Arguments]    ${namespace_id}
    ${headers}=    Create Dictionary    PRIVATE-TOKEN=${GITLAB_PAT}
    ${resp}=    PUT    ${GITLAB_URL}/api/v4/admin/knowledge_graph/namespaces/${namespace_id}
    ...    headers=${headers}    expected_status=any    verify=${VERIFY_SSL}
    IF    ${resp.status_code} != 200
        Fail    Enable KG returned ${resp.status_code}: ${resp.text}
    END

Get Graph Status By Namespace
    [Arguments]    ${namespace_id}
    ${headers}=    GitLab Auth Headers
    ${resp}=    GET    ${GITLAB_URL}/api/v4/orbit/graph_status
    ...    headers=${headers}    params=namespace_id=${namespace_id}    expected_status=any    verify=${VERIFY_SSL}
    RETURN    ${resp}

Get Graph Status By Project ID
    [Arguments]    ${project_id}
    ${headers}=    GitLab Auth Headers
    ${resp}=    GET    ${GITLAB_URL}/api/v4/orbit/graph_status
    ...    headers=${headers}    params=project_id=${project_id}    expected_status=any    verify=${VERIFY_SSL}
    RETURN    ${resp}

Get Graph Status By Full Path
    [Arguments]    ${full_path}
    ${headers}=    GitLab Auth Headers
    ${resp}=    GET    ${GITLAB_URL}/api/v4/orbit/graph_status
    ...    headers=${headers}    params=full_path=${full_path}    expected_status=any    verify=${VERIFY_SSL}
    RETURN    ${resp}

Verify Graph Status Is Idle
    [Arguments]    ${namespace_id}
    ${resp}=    Get Graph Status By Namespace    ${namespace_id}
    Should Be Equal As Integers    ${resp.status_code}    200
    ${body}=    Set Variable    ${resp.json()}
    Should Be Equal    ${body["state"]}    idle
    Should Be True    ${body["initial_backfill_done"]}

Get Domain Item Count
    [Arguments]    ${response}    ${domain_name}    ${entity_name}
    ${domains}=    Set Variable    ${response.json()["domains"]}
    FOR    ${domain}    IN    @{domains}
        IF    '${domain["name"]}' == '${domain_name}'
            FOR    ${item}    IN    @{domain["items"]}
                IF    '${item["name"]}' == '${entity_name}'
                    RETURN    ${item["count"]}
                END
            END
        END
    END
    RETURN    ${0}

Suite Init
    ${ssl}=    Verify SSL Flag
    Set Suite Variable    ${VERIFY_SSL}    ${ssl}
    ${suffix}=    Random Suffix
    Set Suite Variable    ${RUN_ID}    ${suffix}

    ${group}=    Create Group    gs-e2e-${suffix}    gs-e2e-${suffix}
    Set Suite Variable    ${TOP_GROUP_ID}    ${group["id"]}
    Set Suite Variable    ${GROUP_FULL_PATH}    ${group["full_path"]}

    ${sub}=    Create Subgroup    sub-${suffix}    sub-${suffix}    ${group["id"]}
    Set Suite Variable    ${SUB_GROUP_ID}    ${sub["id"]}

    ${project}=    Create Project    app-${suffix}    ${group["id"]}
    Set Suite Variable    ${PROJECT_ID}    ${project["id"]}
    Set Suite Variable    ${PROJECT_FULL_PATH}    ${project["path_with_namespace"]}

    Enable Knowledge Graph    ${group["id"]}

    Wait Until Keyword Succeeds    5m    5s
    ...    Verify Graph Status Is Idle    ${group["id"]}


*** Test Cases ***
Graph Status Returns Valid Response For Namespace
    [Documentation]    Query graph status by namespace_id for an indexed group.
    [Tags]    smoke
    ${resp}=    Get Graph Status By Namespace    ${TOP_GROUP_ID}
    Should Be Equal As Integers    ${resp.status_code}    200

    ${body}=    Set Variable    ${resp.json()}
    Should Contain Any    ${body["state"]}    idle    indexing
    Should Be True    ${body["initial_backfill_done"]}
    Dictionary Should Contain Key    ${body}    domains
    Dictionary Should Contain Key    ${body}    edge_counts

Graph Status By Project ID
    [Documentation]    Query graph status using a project_id.
    [Tags]    lookup
    ${resp}=    Get Graph Status By Project ID    ${PROJECT_ID}
    Should Be Equal As Integers    ${resp.status_code}    200
    Should Contain Any    ${resp.json()["state"]}    idle    indexing

Graph Status By Full Path Group
    [Documentation]    Query graph status using the full_path of a group.
    [Tags]    lookup
    ${resp}=    Get Graph Status By Full Path    ${GROUP_FULL_PATH}
    Should Be Equal As Integers    ${resp.status_code}    200
    Should Contain Any    ${resp.json()["state"]}    idle    indexing

Graph Status By Full Path Project
    [Documentation]    Query graph status using the full_path of a project.
    [Tags]    lookup
    ${resp}=    Get Graph Status By Full Path    ${PROJECT_FULL_PATH}
    Should Be Equal As Integers    ${resp.status_code}    200
    Should Contain Any    ${resp.json()["state"]}    idle    indexing

All Lookup Methods Return Same State
    [Documentation]    namespace_id, project_id, and full_path resolve to the same state.
    [Tags]    consistency
    ${r1}=    Get Graph Status By Namespace    ${TOP_GROUP_ID}
    ${r2}=    Get Graph Status By Project ID    ${PROJECT_ID}
    ${r3}=    Get Graph Status By Full Path    ${GROUP_FULL_PATH}

    Should Be Equal    ${r1.json()["state"]}    ${r2.json()["state"]}
    Should Be Equal    ${r1.json()["state"]}    ${r3.json()["state"]}

Missing Parameters Returns 400
    [Tags]    validation
    ${headers}=    GitLab Auth Headers
    ${resp}=    GET    ${GITLAB_URL}/api/v4/orbit/graph_status
    ...    headers=${headers}    expected_status=400    verify=${VERIFY_SSL}

Non-Existent Namespace Returns 404
    [Tags]    validation
    ${resp}=    Get Graph Status By Namespace    999999999
    Should Be Equal As Integers    ${resp.status_code}    404

Non-Existent Project Returns 404
    [Tags]    validation
    ${resp}=    Get Graph Status By Project ID    999999999
    Should Be Equal As Integers    ${resp.status_code}    404

Non-Existent Full Path Returns 404
    [Tags]    validation
    ${resp}=    Get Graph Status By Full Path    nonexistent/path/does-not-exist
    Should Be Equal As Integers    ${resp.status_code}    404

Response Contains Domain Structure
    [Documentation]    Verify response has expected ontology domains with items.
    [Tags]    schema
    ${resp}=    Get Graph Status By Namespace    ${TOP_GROUP_ID}
    Should Be Equal As Integers    ${resp.status_code}    200

    ${domains}=    Set Variable    ${resp.json()["domains"]}
    Should Not Be Empty    ${domains}

    ${names}=    Evaluate    [d["name"] for d in $domains]
    Should Contain    ${names}    core
    Should Contain    ${names}    code_review
    Should Contain    ${names}    ci

    FOR    ${domain}    IN    @{domains}
        Should Not Be Empty    ${domain["items"]}
        FOR    ${item}    IN    @{domain["items"]}
            Dictionary Should Contain Key    ${item}    name
            Dictionary Should Contain Key    ${item}    status
            Dictionary Should Contain Key    ${item}    count
        END
    END

Response Contains SDLC Progress
    [Tags]    metadata
    ${resp}=    Get Graph Status By Namespace    ${TOP_GROUP_ID}
    Should Be Equal As Integers    ${resp.status_code}    200

    ${sdlc}=    Set Variable    ${resp.json()["sdlc"]}
    Should Not Be Equal    ${sdlc}    ${None}
    Dictionary Should Contain Key    ${sdlc}    last_completed_at
    Dictionary Should Contain Key    ${sdlc}    last_duration_ms

Response Contains Code Overview
    [Tags]    metadata
    ${resp}=    Get Graph Status By Namespace    ${TOP_GROUP_ID}
    Should Be Equal As Integers    ${resp.status_code}    200

    ${code}=    Set Variable    ${resp.json()["code"]}
    Should Not Be Equal    ${code}    ${None}
    Dictionary Should Contain Key    ${code}    projects_indexed
    Dictionary Should Contain Key    ${code}    projects_total

Unauthenticated Request Fails
    [Tags]    auth
    ${resp}=    GET    ${GITLAB_URL}/api/v4/orbit/graph_status
    ...    params=namespace_id=${TOP_GROUP_ID}    expected_status=any    verify=${VERIFY_SSL}
    Should Be True    ${resp.status_code} == 401 or ${resp.status_code} == 403

Subgroup Has Fewer Entities Than Parent
    [Tags]    hierarchy
    ${parent}=    Get Graph Status By Namespace    ${TOP_GROUP_ID}
    ${child}=    Get Graph Status By Namespace    ${SUB_GROUP_ID}

    Should Be Equal As Integers    ${parent.status_code}    200
    Should Be Equal As Integers    ${child.status_code}    200

    ${pg}=    Get Domain Item Count    ${parent}    core    Group
    ${cg}=    Get Domain Item Count    ${child}    core    Group

    Should Be True    ${pg} >= ${cg}
    ...    Parent groups (${pg}) should >= child groups (${cg})
