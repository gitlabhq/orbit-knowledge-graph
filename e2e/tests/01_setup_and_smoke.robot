*** Settings ***
Documentation       Bootstrap credentials, smoke-test the Orbit pipeline, and provision a
...                 knowledge-graph-enabled namespace shared by every later suite.

Resource            gitlab.resource
Resource            orbit.resource

Suite Setup         Provision Smoke Fixtures


*** Test Cases ***
Feature Flags Are Enabled
    [Documentation]    Flags are enabled during suite setup; verify they propagate.
    [Tags]    smoke
    Wait Until Keyword Succeeds    30s    2s    Feature Flag Is Enabled    knowledge_graph_infra
    Wait Until Keyword Succeeds    30s    2s    Feature Flag Is Enabled    knowledge_graph

Orbit Is Healthy
    [Documentation]    Wait for all components (GKG, Siphon, NATS, ClickHouse) to report healthy.
    [Tags]    smoke
    Wait Until Keyword Succeeds    30s    2s    Orbit Status Is Healthy

User Data Is Available Via Orbit Query
    [Documentation]    Verify the full pipeline: PG → Siphon → ClickHouse → GKG indexer → Orbit API.
    [Tags]    smoke
    Wait For Node Indexed    User    ${E2E_BOT_USER_ID}    ${E2E_BOT_USERNAME}    label_field=username

Shared Namespace Is Enabled And Indexed
    [Documentation]    The namespace reused by downstream suites is provisioned in suite setup;
    ...                verify it indexes end-to-end, then publish it for the parallel pool.
    [Tags]    smoke    setup
    Wait For Node Indexed    Group    ${SHARED_NAMESPACE_ID}    ${SHARED_NAMESPACE_NAME}    timeout=300s
    Set Parallel Value For Key    SHARED_NAMESPACE_ID    ${SHARED_NAMESPACE_ID}
    Set Parallel Value For Key    SHARED_NAMESPACE_NAME    ${SHARED_NAMESPACE_NAME}

Pipeline Is At Steady State
    [Documentation]    The canary project indexes before its issue and note are created.
    ...                Wait for all three nodes within the shared setup budget.
    [Tags]    smoke    setup
    Wait For Node Indexed Within Budget    Project    ${CANARY_PROJECT_ID}    ${CANARY_PROJECT_NAME}
    Wait For Node Indexed Within Budget    WorkItem    ${CANARY_ISSUE_ID}    ${CANARY_ISSUE_TITLE}    label_field=title
    Wait For Node Indexed Within Budget    Note    ${CANARY_NOTE_ID}


*** Keywords ***
Provision Smoke Fixtures
    [Documentation]    Index the project before creating its issue so the namespace path is available.
    ...                Keep all canary waits within one shared budget.
    Bootstrap E2E Credentials
    Enable Feature Flag    knowledge_graph_infra
    Enable Feature Flag    knowledge_graph
    Wait Until Keyword Succeeds    30s    2s    Feature Flag Is Enabled    knowledge_graph_infra
    Wait Until Keyword Succeeds    30s    2s    Feature Flag Is Enabled    knowledge_graph
    ${suffix}=    Random Suffix
    ${name}=    Set Variable    e2e-shared-${suffix}
    ${group}=    Create Group    ${name}
    Set Global Variable    ${SHARED_NAMESPACE_ID}    ${group["id"]}
    Set Global Variable    ${SHARED_NAMESPACE_NAME}    ${name}
    Enable Orbit    ${SHARED_NAMESPACE_ID}
    Start Indexing Budget    300
    ${project}=    Create Project    canary-prj-${suffix}    ${SHARED_NAMESPACE_ID}
    Wait For Node Indexed Within Budget    Project    ${project}[id]    ${project}[name]
    ${issue}=    Create Issue    ${project["id"]}    canary-issue-${suffix}
    ${note}=    Create Note On Issue    ${project["id"]}    ${issue["iid"]}    canary-note-${suffix}
    Set Suite Variable    ${CANARY_PROJECT_ID}    ${project["id"]}
    Set Suite Variable    ${CANARY_PROJECT_NAME}    canary-prj-${suffix}
    Set Suite Variable    ${CANARY_ISSUE_ID}    ${issue["id"]}
    Set Suite Variable    ${CANARY_ISSUE_TITLE}    canary-issue-${suffix}
    Set Suite Variable    ${CANARY_NOTE_ID}    ${note["id"]}
