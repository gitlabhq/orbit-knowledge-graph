*** Settings ***
Library    RequestsLibrary
Library    Collections
Library    String

*** Variables ***
${GITLAB_URL}     %{GITLAB_URL}

*** Keywords ***
GitLab Auth Headers
    RETURN    ${{{"PRIVATE-TOKEN": "${GITLAB_PAT}", "Content-Type": "application/json"}}}

Random Suffix
    ${rand}=    Generate Random String    6    [LOWER][NUMBERS]
    RETURN    ${rand}

Create Group
    [Arguments]    ${name}    ${path}
    ${headers}=    GitLab Auth Headers
    ${body}=    Create Dictionary    name=${name}    path=${path}    visibility=public
    ${resp}=    POST    ${GITLAB_URL}/api/v4/groups
    ...    headers=${headers}    json=${body}    expected_status=201
    RETURN    ${resp.json()}

Enable Knowledge Graph
    [Arguments]    ${namespace_id}
    Wait Until Keyword Succeeds    30s    5s
    ...    Enable Knowledge Graph Once    ${namespace_id}

Enable Knowledge Graph Once
    [Arguments]    ${namespace_id}
    ${headers}=    Create Dictionary    PRIVATE-TOKEN=${GITLAB_PAT}
    ${resp}=    PUT    ${GITLAB_URL}/api/v4/admin/knowledge_graph/namespaces/${namespace_id}
    ...    headers=${headers}    expected_status=any
    IF    ${resp.status_code} != 200
        Fail    Enable KG returned ${resp.status_code}: ${resp.text}
    END

Create Project
    [Arguments]    ${name}    ${namespace_id}
    ${headers}=    GitLab Auth Headers
    ${body}=    Create Dictionary    name=${name}    namespace_id=${namespace_id}    visibility=public
    ${resp}=    POST    ${GITLAB_URL}/api/v4/projects
    ...    headers=${headers}    json=${body}    expected_status=201
    RETURN    ${resp.json()}

Orbit Query Node
    [Arguments]    ${entity}    ${node_id}
    ${headers}=    GitLab Auth Headers
    ${node}=    Create Dictionary    id=n    entity=${entity}    node_ids=${{[${node_id}]}}
    ${query}=    Create Dictionary    query_type=search    node=${node}
    ${body}=    Create Dictionary    query=${query}
    ${resp}=    POST    ${GITLAB_URL}/api/v4/orbit/query
    ...    headers=${headers}    json=${body}    expected_status=200
    RETURN    ${resp.json()}

Verify Node Indexed
    [Arguments]    ${entity}    ${node_id}    ${expected_name}
    ${result}=    Orbit Query Node    ${entity}    ${node_id}
    Should Be True    ${result["row_count"]} >= 1
    ...    ${entity} id=${node_id} not found via Orbit query
    Should Be Equal    ${result["result"]["nodes"][0]["name"]}    ${expected_name}

*** Test Cases ***
Namespace Is Indexed After Enablement
    [Documentation]    Create group, enable KG, wait for indexing, verify Group node via Orbit API
    ${suffix}=    Random Suffix
    ${name}=    Set Variable    e2e-idx-${suffix}
    ${group}=    Create Group    ${name}    ${name}
    ${group_id}=    Set Variable    ${group["id"]}

    Enable Knowledge Graph    ${group_id}

    Wait Until Keyword Succeeds    30s    3s
    ...    Verify Node Indexed    Group    ${group_id}    ${name}

Project Is Indexed Under Enabled Namespace
    [Documentation]    Create group, enable KG, then create project under it, verify both via Orbit API
    ${suffix}=    Random Suffix
    ${name}=    Set Variable    e2e-prj-${suffix}
    ${group}=    Create Group    ${name}    ${name}
    ${group_id}=    Set Variable    ${group["id"]}

    Enable Knowledge Graph    ${group_id}
    Wait Until Keyword Succeeds    30s    3s
    ...    Verify Node Indexed    Group    ${group_id}    ${name}

    ${project}=    Create Project    proj-${suffix}    ${group_id}
    ${project_id}=    Set Variable    ${project["id"]}

    Wait Until Keyword Succeeds    60s    3s
    ...    Verify Node Indexed    Project    ${project_id}    proj-${suffix}
