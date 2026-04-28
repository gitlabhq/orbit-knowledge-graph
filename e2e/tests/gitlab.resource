*** Settings ***
Documentation       GitLab API helpers — auth, feature flags, group/project/issue/note CRUD.
...                 Imported by every test suite. Holds no test cases.

Library             RequestsLibrary
Library             Collections
Library             String
Library             DateTime


*** Variables ***
${GITLAB_URL}                       %{GITLAB_URL}
${GITLAB_ROOT_PAT}                  %{GITLAB_ROOT_PAT}
${E2E_BOT_USERNAME}                 e2e-bot
${E2E_BOT_EMAIL}                    e2e-bot@example.com
${E2E_BOT_NAME}                     E2E Bot
${E2E_BOT_PASSWORD}                 E2eB0tP@ssw0rd!
${PAT_NAME}                         e2e-pat


*** Keywords ***
Bootstrap E2E Credentials
    [Documentation]    Idempotently create e2e-bot admin user and a PAT, expose PAT and user id as global variables.
    ...                Uses the deploy-time root PAT (created via rails runner since GitLab 19.0 removed
    ...                the OAuth password grant) instead of an OAuth token.
    ${root_headers}=    Create Dictionary
    ...    PRIVATE-TOKEN=${GITLAB_ROOT_PAT}    Content-Type=application/json

    ${user_id}=    Wait Until Keyword Succeeds    120s    5s
    ...    Ensure E2E Bot User    ${root_headers}
    ${pat}=    Issue PAT For User    ${root_headers}    ${user_id}
    Set Global Variable    ${GITLAB_PAT}    ${pat}
    Set Global Variable    ${E2E_BOT_USER_ID}    ${user_id}
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

Root Auth Headers
    [Documentation]    Headers carrying the deploy-time root PAT. Required for admin-only
    ...                operations like creating users or issuing PATs for arbitrary users.
    RETURN    ${{{"PRIVATE-TOKEN": "${GITLAB_ROOT_PAT}", "Content-Type": "application/json"}}}

Create User
    [Documentation]    Create a non-admin user. Requires root PAT. Returns the parsed user dict.
    [Arguments]    ${username}    ${name}=${username}    ${email}=${username}@example.com
    ...    ${password}=Password1234!
    ${headers}=    Root Auth Headers
    ${body}=    Create Dictionary    username=${username}    name=${name}    email=${email}
    ...    password=${password}    skip_confirmation=true
    ${resp}=    POST    ${GITLAB_URL}/api/v4/users
    ...    headers=${headers}    json=${body}    expected_status=201
    RETURN    ${resp.json()}

Create Subgroup
    [Documentation]    Create a child group under ${parent_id}. Returns the parsed group dict.
    [Arguments]    ${parent_id}    ${name}    ${path}=${name}
    ${headers}=    GitLab Auth Headers
    ${body}=    Create Dictionary    name=${name}    path=${path}    parent_id=${parent_id}
    ...    visibility=public
    ${resp}=    POST    ${GITLAB_URL}/api/v4/groups
    ...    headers=${headers}    json=${body}    expected_status=201
    RETURN    ${resp.json()}

Add Group Member
    [Documentation]    Add ${user_id} to ${group_id} at the given built-in ${access_level}
    ...                (10 Guest, 15 Planner, 20 Reporter, 25 Security Manager, 30 Developer,
    ...                40 Maintainer, 50 Owner). Returns the parsed member dict.
    [Arguments]    ${group_id}    ${user_id}    ${access_level}
    ${headers}=    GitLab Auth Headers
    ${body}=    Create Dictionary    user_id=${user_id}    access_level=${access_level}
    ${resp}=    POST    ${GITLAB_URL}/api/v4/groups/${group_id}/members
    ...    headers=${headers}    json=${body}    expected_status=201
    RETURN    ${resp.json()}

Create Vulnerability
    [Documentation]    Create a manually-reported Vulnerability via the GraphQL `vulnerabilityCreate`
    ...                mutation. Requires Ultimate license (e2e/scripts/activate-license.sh) and
    ...                Maintainer+ on the project; the e2e-bot is admin so this works on any project.
    ...                `report_type` is always `generic` for manually-created vulns; `created_at` is
    ...                stamped by the server (only `detected_at` is settable). Returns a dict with
    ...                numeric id (parsed from gid://gitlab/Vulnerability/N), title, severity, state,
    ...                report_type, and createdAt.
    [Arguments]    ${project_id}    ${title}    ${severity}=critical    ${state}=DETECTED
    ...    ${description}=role scoped authz fixture
    ${headers}=    GitLab Auth Headers
    ${project_gid}=    Set Variable    gid://gitlab/Project/${project_id}
    ${scanner}=    Create Dictionary    id=role-scoped-authz-e2e
    ...    name=role-scoped-authz-e2e    vendor=GitLab
    ${identifier}=    Create Dictionary    name=CWE-89    url=https://cwe.mitre.org/data/definitions/89.html
    ...    externalType=cwe    externalId=CWE-89
    ${identifiers}=    Create List    ${identifier}
    ${input}=    Create Dictionary    project=${project_gid}    name=${title}    description=${description}
    ...    severity=${severity}    state=${state}    scanner=${scanner}    identifiers=${identifiers}
    ${query}=    Catenate    SEPARATOR=
    ...    mutation($input: VulnerabilityCreateInput!) {
    ...    \ \ vulnerabilityCreate(input: $input) {
    ...    \ \ \ \ vulnerability { id title severity state reportType createdAt }
    ...    \ \ \ \ errors
    ...    \ \ }
    ...    \ }
    ${variables}=    Create Dictionary    input=${input}
    ${body}=    Create Dictionary    query=${query}    variables=${variables}
    ${resp}=    POST    ${GITLAB_URL}/api/graphql
    ...    headers=${headers}    json=${body}    expected_status=200
    ${payload}=    Set Variable    ${resp.json()}
    Should Not Contain    ${payload}    errors
    ...    GraphQL transport error: ${payload}
    ${data}=    Set Variable    ${payload["data"]["vulnerabilityCreate"]}
    Should Be Empty    ${data["errors"]}
    ...    vulnerabilityCreate returned errors: ${data["errors"]}
    ${vuln}=    Set Variable    ${data["vulnerability"]}
    ${numeric_id}=    Evaluate    int($vuln["id"].rsplit("/", 1)[-1])
    ${result}=    Create Dictionary    id=${numeric_id}    title=${vuln["title"]}
    ...    severity=${vuln["severity"]}    state=${vuln["state"]}
    ...    report_type=${vuln["reportType"]}    createdAt=${vuln["createdAt"]}
    RETURN    ${result}
