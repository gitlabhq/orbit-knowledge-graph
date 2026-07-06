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
    [Documentation]    The canary project + issue + note are created in suite setup so they index
    ...                while earlier tests run; wait for each within a single shared budget. Once
    ...                this passes, Siphon's initial snapshot has reached the slowest tables we
    ...                depend on (notes, work_items) and downstream suites can use short
    ...                per-call timeouts.
    [Tags]    smoke    setup
    Start Indexing Budget    300
    Wait For Node Indexed Within Budget    Project    ${CANARY_PROJECT_ID}    ${CANARY_PROJECT_NAME}
    Wait For Node Indexed Within Budget    WorkItem    ${CANARY_ISSUE_ID}    ${CANARY_ISSUE_TITLE}    label_field=title
    Wait For Node Indexed Within Budget    Note    ${CANARY_NOTE_ID}


*** Keywords ***
Provision Smoke Fixtures
    [Documentation]    Provision everything up front so the shared group and the canary trio
    ...                index concurrently instead of serially across test cases.
    Bootstrap E2E Credentials
    Enable Feature Flag    knowledge_graph_infra
    Enable Feature Flag    knowledge_graph
    # Verify propagation before Enable Knowledge Graph reads the flags.
    Wait Until Keyword Succeeds    30s    2s    Feature Flag Is Enabled    knowledge_graph_infra
    Wait Until Keyword Succeeds    30s    2s    Feature Flag Is Enabled    knowledge_graph
    ${suffix}=    Random Suffix
    ${name}=    Set Variable    e2e-shared-${suffix}
    ${group}=    Create Group    ${name}
    Set Global Variable    ${SHARED_NAMESPACE_ID}    ${group["id"]}
    Set Global Variable    ${SHARED_NAMESPACE_NAME}    ${name}
    Enable Knowledge Graph    ${SHARED_NAMESPACE_ID}
    ${project}=    Create Project    canary-prj-${suffix}    ${SHARED_NAMESPACE_ID}
    ${issue}=    Create Issue    ${project["id"]}    canary-issue-${suffix}
    ${note}=    Create Note On Issue    ${project["id"]}    ${issue["iid"]}    canary-note-${suffix}
    Set Suite Variable    ${CANARY_PROJECT_ID}    ${project["id"]}
    Set Suite Variable    ${CANARY_PROJECT_NAME}    canary-prj-${suffix}
    Set Suite Variable    ${CANARY_ISSUE_ID}    ${issue["id"]}
    Set Suite Variable    ${CANARY_ISSUE_TITLE}    canary-issue-${suffix}
    Set Suite Variable    ${CANARY_NOTE_ID}    ${note["id"]}
