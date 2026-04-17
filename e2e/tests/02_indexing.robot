*** Settings ***
Documentation       Verify SDLC entities created under the shared namespace flow through
...                 PG → Siphon → ClickHouse → GKG and become queryable via Orbit.
...                 Depends on ${SHARED_NAMESPACE_ID} set by 01_setup_and_smoke.robot.

Resource            gitlab.resource
Resource            orbit.resource


*** Test Cases ***
Project Issue And Note Are Indexed
    [Documentation]    Create a project, an issue inside it, and a note on that issue,
    ...                then assert each becomes queryable via Orbit.
    [Tags]    indexing
    ${suffix}=    Random Suffix

    ${project_name}=    Set Variable    e2e-prj-${suffix}
    ${project}=    Create Project    ${project_name}    ${SHARED_NAMESPACE_ID}
    Wait For Node Indexed    Project    ${project["id"]}    ${project_name}    timeout=30s

    ${issue_title}=    Set Variable    e2e-issue-${suffix}
    ${issue}=    Create Issue    ${project["id"]}    ${issue_title}
    Wait For Node Indexed    WorkItem    ${issue["id"]}    ${issue_title}
    ...    label_field=title    timeout=30s

    ${note_body}=    Set Variable    e2e-note-${suffix}
    ${note}=    Create Note On Issue    ${project["id"]}    ${issue["iid"]}    ${note_body}
    Wait For Node Indexed    Note    ${note["id"]}    timeout=30s
