*** Settings ***
Documentation       Bootstrap credentials, smoke-test the Orbit pipeline, and provision a
...                 knowledge-graph-enabled namespace shared by every later suite.

Resource            gitlab.resource
Resource            orbit.resource

Suite Setup         Bootstrap E2E Credentials


*** Test Cases ***
Feature Flags Are Enabled
    [Documentation]    Enable knowledge graph feature flags via API and verify they propagate.
    [Tags]    smoke
    Enable Feature Flag    knowledge_graph_infra
    Enable Feature Flag    knowledge_graph
    Wait Until Keyword Succeeds    30s    3s    Feature Flag Is Enabled    knowledge_graph_infra
    Wait Until Keyword Succeeds    30s    3s    Feature Flag Is Enabled    knowledge_graph

Orbit Is Healthy
    [Documentation]    Wait for all components (GKG, Siphon, NATS, ClickHouse) to report healthy.
    [Tags]    smoke
    Wait Until Keyword Succeeds    30s    3s    Orbit Status Is Healthy

User Data Is Available Via Orbit Query
    [Documentation]    Verify the full pipeline: PG → Siphon → ClickHouse → GKG indexer → Orbit API.
    [Tags]    smoke
    Wait For Node Indexed    User    ${E2E_BOT_USER_ID}    ${E2E_BOT_USERNAME}    label_field=username

Shared Namespace Is Enabled And Indexed
    [Documentation]    Provision the namespace reused by downstream suites; verify it indexes end-to-end.
    ...                Exposes ${SHARED_NAMESPACE_ID} and ${SHARED_NAMESPACE_NAME} as global variables.
    [Tags]    smoke    setup
    ${suffix}=    Random Suffix
    ${name}=    Set Variable    e2e-shared-${suffix}
    ${group}=    Create Group    ${name}
    Set Global Variable    ${SHARED_NAMESPACE_ID}    ${group["id"]}
    Set Global Variable    ${SHARED_NAMESPACE_NAME}    ${name}
    Enable Knowledge Graph    ${SHARED_NAMESPACE_ID}
    Wait For Node Indexed    Group    ${SHARED_NAMESPACE_ID}    ${name}

Pipeline Is At Steady State
    [Documentation]    Create a canary project + issue + note in the shared namespace and wait for
    ...                each to be indexed within a single shared 5-minute budget. Once this passes,
    ...                Siphon's initial snapshot has reached the slowest tables we depend on
    ...                (notes, work_items) and downstream suites can use short per-call timeouts.
    [Tags]    smoke    setup
    ${suffix}=    Random Suffix
    ${project_name}=    Set Variable    canary-prj-${suffix}
    ${issue_title}=    Set Variable    canary-issue-${suffix}
    ${note_body}=    Set Variable    canary-note-${suffix}

    ${project}=    Create Project    ${project_name}    ${SHARED_NAMESPACE_ID}
    ${issue}=    Create Issue    ${project["id"]}    ${issue_title}
    ${note}=    Create Note On Issue    ${project["id"]}    ${issue["iid"]}    ${note_body}

    Start Indexing Budget    300
    Wait For Node Indexed Within Budget    Project    ${project["id"]}    ${project_name}
    Wait For Node Indexed Within Budget    WorkItem    ${issue["id"]}    ${issue_title}    label_field=title
    Wait For Node Indexed Within Budget    Note    ${note["id"]}
