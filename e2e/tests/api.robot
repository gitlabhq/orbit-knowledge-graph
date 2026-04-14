*** Settings ***
Library    RequestsLibrary
Library    Collections

*** Variables ***
${GITLAB_URL}     %{GITLAB_URL}
${GITLAB_PAT}     %{GITLAB_PAT}

*** Keywords ***
GitLab Auth Headers
    RETURN    ${{{"PRIVATE-TOKEN": "${GITLAB_PAT}"}}}

*** Test Cases ***
GitLab User Info Returns OK
    ${headers}=    GitLab Auth Headers
    GET    ${GITLAB_URL}/api/v4/user    headers=${headers}    expected_status=200

GitLab Projects List Returns OK
    ${headers}=    GitLab Auth Headers
    GET    ${GITLAB_URL}/api/v4/projects    headers=${headers}    expected_status=200

Orbit Status Returns Healthy
    [Documentation]    Full pipeline: GitLab -> gRPC TLS -> GKG -> ClickHouse
    ${headers}=    GitLab Auth Headers
    ${resp}=    GET    ${GITLAB_URL}/api/v4/orbit/status    headers=${headers}    expected_status=200
    ${json}=    Set Variable    ${resp.json()}
    Should Be Equal    ${json["status"]}    healthy
