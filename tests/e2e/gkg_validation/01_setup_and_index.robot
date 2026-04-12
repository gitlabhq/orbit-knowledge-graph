*** Settings ***
Documentation       Phase 1: Verify services are running, create test data via API,
...                 and verify CDC replication to ClickHouse.

Resource            resources/common.robot

Suite Setup         Suite Init


*** Test Cases ***
Services Are Running
    [Documentation]    Verify GDK Rails and ClickHouse are reachable
    [Tags]    services    critical

    # GDK Rails
    ${gdk}=    Run Process    bash    -c
    ...    curl -s ${GDK_URL}/-/readiness 2>/dev/null | head -c 100
    ...    timeout=10
    Should Contain    ${gdk.stdout}    ok    msg=GDK Rails not responding

    # ClickHouse
    ${ch}=    ClickHouse Query    SELECT 1
    ${ch}=    Strip String    ${ch}
    Should Be Equal    ${ch}    1    msg=ClickHouse not responding: ${ch}

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
    Create Merge Request    ${proj2_id}    Update dependencies    chore/deps-${RUN_ID}

    Create Issue    ${proj1_id}    Track performance metrics

    Set Suite Variable    ${ORG_ID}    1
    Set Suite Variable    ${TRAVERSAL_PATH}    1/${group_id}/

    Log    Seeded: group=${group_id} sub=${sub_id} proj1=${proj1_id} proj2=${proj2_id}

GitLab API Returns Created Data
    [Documentation]    Verify the seeded data is accessible via the API
    [Tags]    api    critical
    [Setup]    Variable Should Exist    ${TOP_GROUP_ID}

    ${resp}=    GitLab API GET    /groups/${TOP_GROUP_ID}
    Should Be Equal As Integers    ${resp.status_code}    200
    Should Be Equal    ${resp.json()['name']}    gkg-e2e-${RUN_ID}

    ${resp}=    GitLab API GET    /groups/${TOP_GROUP_ID}/projects
    Should Be Equal As Integers    ${resp.status_code}    200
    ${count}=    Evaluate    len(${resp.json()})
    Should Be True    ${count} >= 1    msg=Expected at least 1 project


*** Keywords ***
Suite Init
    Create API Session
    ${ts}=    Evaluate    str(int(time.time()))[-6:]    modules=time
    Set Suite Variable    ${RUN_ID}    ${ts}

    # Verify API is accessible
    ${resp}=    GitLab API GET    /version
    Should Be Equal As Integers    ${resp.status_code}    200    msg=GitLab API not accessible
