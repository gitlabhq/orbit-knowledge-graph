*** Settings ***
Documentation       GitLab API helpers — auth, feature flags, group/project/issue/note CRUD.
...                 Imported by every test suite. Holds no test cases.

Library             RequestsLibrary
Library             Collections
Library             String
Library             DateTime


*** Variables ***
${GITLAB_URL}                       %{GITLAB_URL}
${GITLAB_ROOT_PASSWORD}             %{GITLAB_ROOT_PASSWORD}
${E2E_BOT_USERNAME}                 e2e-bot
${E2E_BOT_EMAIL}                    e2e-bot@example.com
${E2E_BOT_NAME}                     E2E Bot
${E2E_BOT_PASSWORD}                 E2eB0tP@ssw0rd!
${PAT_NAME}                         e2e-pat


*** Keywords ***
Get Root OAuth Token
    [Documentation]    Exchange the deploy-time root password for an OAuth token.
    ${auth}=    Create Dictionary
    ...    grant_type=password    username=root    password=${GITLAB_ROOT_PASSWORD}
    ${resp}=    POST    ${GITLAB_URL}/oauth/token    data=${auth}    expected_status=200
    RETURN    ${resp.json()["access_token"]}

Bootstrap E2E Credentials
    [Documentation]    Idempotently create e2e-bot admin user and a PAT, expose PAT as a global variable.
    ${token}=    Wait Until Keyword Succeeds    120s    5s    Get Root OAuth Token
    ${root_headers}=    Create Dictionary
    ...    Authorization=Bearer ${token}    Content-Type=application/json

    ${user_id}=    Ensure E2E Bot User    ${root_headers}
    ${pat}=    Issue PAT For User    ${root_headers}    ${user_id}
    Set Global Variable    ${GITLAB_PAT}    ${pat}
    Log    E2E credentials bootstrapped (user_id=${user_id})

Ensure E2E Bot User
    [Arguments]    ${root_headers}
    ${user_data}=    Create Dictionary
    ...    username=${E2E_BOT_USERNAME}    email=${E2E_BOT_EMAIL}    name=${E2E_BOT_NAME}
    ...    password=${E2E_BOT_PASSWORD}    skip_confirmation=true    admin=true
    ${resp}=    POST    ${GITLAB_URL}/api/v4/users
    ...    headers=${root_headers}    json=${user_data}    expected_status=any
    IF    ${resp.status_code} == 201
        RETURN    ${resp.json()["id"]}
    ELSE IF    ${resp.status_code} == 409
        ${params}=    Create Dictionary    username=${E2E_BOT_USERNAME}
        ${resp}=    GET    url=${GITLAB_URL}/api/v4/users
        ...    headers=${root_headers}    params=${params}    expected_status=200
        RETURN    ${resp.json()[0]["id"]}
    END
    Fail    Failed to create ${E2E_BOT_USERNAME}: ${resp.status_code} ${resp.text}

Issue PAT For User
    [Arguments]    ${root_headers}    ${user_id}
    ${scopes}=    Create List    api    read_api
    ${expiry}=    Evaluate
    ...    (datetime.date.today() + datetime.timedelta(days=30)).isoformat()    modules=datetime
    ${pat_data}=    Create Dictionary    name=${PAT_NAME}    scopes=${scopes}    expires_at=${expiry}
    ${resp}=    POST    ${GITLAB_URL}/api/v4/users/${user_id}/personal_access_tokens
    ...    headers=${root_headers}    json=${pat_data}    expected_status=201
    RETURN    ${resp.json()["token"]}

GitLab Auth Headers
    [Documentation]    Standard JSON headers authenticated with the e2e-bot PAT.
    RETURN    ${{{"PRIVATE-TOKEN": "${GITLAB_PAT}", "Content-Type": "application/json"}}}

Random Suffix
    [Documentation]    Six-char lowercase+digit suffix for unique resource names.
    ${rand}=    Generate Random String    6    [LOWER][NUMBERS]
    RETURN    ${rand}

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
    FOR    ${f}    IN    @{resp.json()}
        IF    "${f["name"]}" == "${flag}"
            Should Be Equal As Strings    ${f["state"]}    on
            ...    Feature flag ${flag} not enabled
            RETURN
        END
    END
    Fail    Feature flag ${flag} not found

Create Group
    [Arguments]    ${name}    ${path}=${name}
    ${headers}=    GitLab Auth Headers
    ${body}=    Create Dictionary    name=${name}    path=${path}    visibility=public
    ${resp}=    POST    ${GITLAB_URL}/api/v4/groups
    ...    headers=${headers}    json=${body}    expected_status=201
    RETURN    ${resp.json()}

Enable Knowledge Graph
    [Arguments]    ${namespace_id}
    [Documentation]    Retry-wrapped enable; admin endpoint can briefly 5xx during siphon snapshot.
    Wait Until Keyword Succeeds    30s    5s    Enable Knowledge Graph Once    ${namespace_id}

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

Create Issue
    [Arguments]    ${project_id}    ${title}
    ${headers}=    GitLab Auth Headers
    ${body}=    Create Dictionary    title=${title}
    ${resp}=    POST    ${GITLAB_URL}/api/v4/projects/${project_id}/issues
    ...    headers=${headers}    json=${body}    expected_status=201
    RETURN    ${resp.json()}

Create Epic
    [Documentation]    Create a group-scoped epic and return its dict. The response includes
    ...                both the epic id (REST-scoped) and work_item_id (the global id indexed
    ...                by Orbit as a WorkItem with work_item_type=epic).
    [Arguments]    ${group_id}    ${title}
    ${headers}=    GitLab Auth Headers
    ${body}=    Create Dictionary    title=${title}
    ${resp}=    POST    ${GITLAB_URL}/api/v4/groups/${group_id}/epics
    ...    headers=${headers}    json=${body}    expected_status=201
    RETURN    ${resp.json()}

Add Issue To Epic
    [Documentation]    Parent ${issue_id} under epic ${epic_iid} in ${group_id}. Creates a
    ...                WorkItem CONTAINS WorkItem edge once Siphon replicates the
    ...                work_item_parent_links row.
    [Arguments]    ${group_id}    ${epic_iid}    ${issue_id}
    ${headers}=    GitLab Auth Headers
    ${resp}=    POST    ${GITLAB_URL}/api/v4/groups/${group_id}/epics/${epic_iid}/issues/${issue_id}
    ...    headers=${headers}    expected_status=201
    RETURN    ${resp.json()}

Create Note On Issue
    [Arguments]    ${project_id}    ${issue_iid}    ${body_text}
    ${headers}=    GitLab Auth Headers
    ${body}=    Create Dictionary    body=${body_text}
    ${resp}=    POST    ${GITLAB_URL}/api/v4/projects/${project_id}/issues/${issue_iid}/notes
    ...    headers=${headers}    json=${body}    expected_status=201
    RETURN    ${resp.json()}
