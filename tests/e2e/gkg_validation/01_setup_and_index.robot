*** Settings ***
Documentation       Phase 1: Verify services, seed data, and run the indexing pipeline.
...                 Creates test entities via GitLab API, waits for CDC replication,
...                 starts GKG services, and waits for indexing to complete.

Resource            resources/common.robot

Suite Setup         Suite Init
Suite Teardown      Stop GKG Services


*** Test Cases ***
Services Are Running
    [Documentation]    Verify ClickHouse, NATS, and GDK are reachable
    [Tags]    services    critical

    # GDK Rails
    ${gdk}=    Run Process    bash    -c
    ...    curl -s ${GDK_URL}/-/readiness 2>/dev/null | head -c 100
    ...    timeout=10
    Should Contain    ${gdk.stdout}    ok    msg=GDK Rails not responding

    # ClickHouse
    ${ch}=    Run Process    bash    -c
    ...    curl -s "${CLICKHOUSE_URL}/?query=SELECT+1" 2>/dev/null
    ...    timeout=10
    ${ch_out}=    Strip String    ${ch.stdout}
    Should Be Equal    ${ch_out}    1    msg=ClickHouse not responding: ${ch.stdout}

Seed Test Data
    [Documentation]    Create groups, projects, MRs, issues via GitLab API
    [Tags]    seed    critical

    ${group_id}=    Create Top Level Group    gkg-e2e-${RUN_ID}    gkg-e2e-${RUN_ID}
    Set Suite Variable    ${TOP_GROUP_ID}    ${group_id}

    ${sub_id}=    Create Subgroup    sub-${RUN_ID}    sub-${RUN_ID}    ${group_id}
    Set Suite Variable    ${SUB_GROUP_ID}    ${sub_id}

    ${proj1_id}=    Create Project    app-one-${RUN_ID}    ${group_id}
    ${proj2_id}=    Create Project    app-two-${RUN_ID}    ${sub_id}
    Set Suite Variable    ${PROJECT1_ID}    ${proj1_id}
    Set Suite Variable    ${PROJECT2_ID}    ${proj2_id}

    Create Merge Request    ${proj1_id}    Fix auth bug    fix/auth-${RUN_ID}
    Create Merge Request    ${proj1_id}    Add unit tests    feat/tests-${RUN_ID}
    Create Merge Request    ${proj2_id}    Update dependencies    chore/deps-${RUN_ID}

    Create Issue    ${proj1_id}    Track performance metrics
    Create Issue    ${proj2_id}    Security audit follow-up

    Enable Namespace For KG    ${group_id}

    Set Suite Variable    ${ORG_ID}    1
    Set Suite Variable    ${TRAVERSAL_PATH}    1/${group_id}/

    Log    Seeded: group=${group_id} sub=${sub_id} proj1=${proj1_id} proj2=${proj2_id}

CDC Replication Completes
    [Documentation]    Wait for Siphon to replicate test data to ClickHouse datalake
    [Tags]    cdc    critical
    [Setup]    Variable Should Exist    ${TOP_GROUP_ID}

    Wait For CDC

    # Verify data arrived in datalake
    ${result}=    ClickHouse Query
    ...    SELECT count() FROM siphon_namespaces FINAL WHERE id = ${TOP_GROUP_ID}
    ...    ${DATALAKE_DB}
    ${count}=    Strip String    ${result}
    Should Not Be Equal    ${count}    0    msg=Namespace not in datalake after CDC wait

Indexing Pipeline Completes
    [Documentation]    Start GKG services and wait for initial backfill
    [Tags]    indexing    critical
    [Setup]    Variable Should Exist    ${TRAVERSAL_PATH}

    Start GKG Services

    ${resp}=    Wait For Indexing Complete    ${TRAVERSAL_PATH}
    ${body}=    Set Variable    ${resp.json()}

    Should Be Equal    ${body['state']}    idle
    Should Be True    ${body['initial_backfill_done']}
    Should Not Be True    ${body['stale']}

    Log    Indexing complete: state=${body['state']}


*** Keywords ***
Suite Init
    Create API Session
    ${ts}=    Evaluate    str(int(time.time()))[-6:]    modules=time
    Set Suite Variable    ${RUN_ID}    ${ts}

    # Verify API is accessible (feature flags are set by seed-data.rb)
    ${resp}=    GitLab API GET    /version
    Should Be Equal As Integers    ${resp.status_code}    200    msg=GitLab API not accessible
