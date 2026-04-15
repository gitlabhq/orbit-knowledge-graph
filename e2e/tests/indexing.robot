*** Settings ***
Library    RequestsLibrary
Library    Collections
Library    String

*** Variables ***
${GITLAB_URL}     %{GITLAB_URL}
${GITLAB_PAT}     %{GITLAB_PAT}

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
    ${headers}=    Create Dictionary    PRIVATE-TOKEN=${GITLAB_PAT}
    PUT    ${GITLAB_URL}/api/v4/admin/knowledge_graph/namespaces/${namespace_id}
    ...    headers=${headers}    expected_status=200

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

*** Test Cases ***
Namespace Is Indexed After Enablement
    [Documentation]    Create group, enable KG, wait for indexing, verify Group node via Orbit API
    ${suffix}=    Random Suffix
    ${name}=    Set Variable    e2e-idx-${suffix}
    ${group}=    Create Group    ${name}    ${name}
    ${group_id}=    Set Variable    ${group["id"]}

    Enable Knowledge Graph    ${group_id}

    Sleep    25s    Wait for Siphon CDC + dispatcher + indexer cycle

    ${result}=    Orbit Query Node    Group    ${group_id}
    ${count}=    Set Variable    ${result["row_count"]}
    Should Be True    ${count} >= 1    Group ${name} (id=${group_id}) not found via Orbit query

    ${node_name}=    Set Variable    ${result["result"]["nodes"][0]["name"]}
    Should Be Equal    ${node_name}    ${name}

Project Is Indexed Under Enabled Namespace
    [Documentation]    Create group + project, enable KG, wait, verify both via Orbit API
    ${suffix}=    Random Suffix
    ${name}=    Set Variable    e2e-prj-${suffix}
    ${group}=    Create Group    ${name}    ${name}
    ${group_id}=    Set Variable    ${group["id"]}

    Enable Knowledge Graph    ${group_id}
    ${project}=    Create Project    proj-${suffix}    ${group_id}
    ${project_id}=    Set Variable    ${project["id"]}

    Sleep    25s    Wait for Siphon CDC + indexing

    ${grp_result}=    Orbit Query Node    Group    ${group_id}
    Should Be True    ${grp_result["row_count"]} >= 1    Group ${name} not found via Orbit query

    ${prj_result}=    Orbit Query Node    Project    ${project_id}
    Should Be True    ${prj_result["row_count"]} >= 1    Project proj-${suffix} not found via Orbit query

    ${prj_name}=    Set Variable    ${prj_result["result"]["nodes"][0]["name"]}
    Should Be Equal    ${prj_name}    proj-${suffix}
