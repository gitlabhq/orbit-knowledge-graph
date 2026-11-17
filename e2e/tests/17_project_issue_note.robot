*** Settings ***
Documentation       Verify a project, an issue, and a note flow through PG → Siphon → ClickHouse
...                 → GKG and become queryable via Orbit. Split from 02_indexing so the two
...                 indexing cases run in parallel pabot processes.

Resource            gitlab.resource
Resource            orbit.resource

Suite Setup         Attach To Shared Fixture


*** Test Cases ***
Project Issue And Note Are Indexed
    [Documentation]    Create a project, an issue inside it, and a note on that issue,
    ...                then assert each becomes queryable via Orbit.
    [Tags]    indexing
    ${suffix}=    Random Suffix
    Start Indexing Budget    240

    ${project_name}=    Set Variable    e2e-prj-${suffix}
    ${issue_title}=    Set Variable    e2e-issue-${suffix}
    ${note_body}=    Set Variable    e2e-note-${suffix}
    ${project}=    Create Project    ${project_name}    ${SHARED_NAMESPACE_ID}
    ${issue}=    Create Issue    ${project["id"]}    ${issue_title}
    ${note}=    Create Note On Issue    ${project["id"]}    ${issue["iid"]}    ${note_body}

    Wait For Node Indexed Within Budget    Project    ${project["id"]}    ${project_name}
    Wait For Node Indexed Within Budget    WorkItem    ${issue["id"]}    ${issue_title}
    ...    label_field=title
    Wait For Node Indexed Within Budget    Note    ${note["id"]}
