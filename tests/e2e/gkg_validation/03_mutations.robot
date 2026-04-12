*** Settings ***
Documentation       Phase 3: Verify data mutations via the GitLab API.
...                 Basic test that group updates propagate correctly.

Resource            resources/common.robot

Suite Setup         Suite Init


*** Test Cases ***
Group Can Be Created And Retrieved
    [Documentation]    Create a group and verify it's accessible
    [Tags]    mutation    critical

    ${name}=    Set Variable    mutation-${RUN_ID}
    ${group_id}=    Create Top Level Group    ${name}    ${name}
    Should Be True    ${group_id} > 0    msg=Group creation failed

    ${resp}=    GitLab API GET    /groups/${group_id}
    Should Be Equal As Integers    ${resp.status_code}    200
    Should Be Equal    ${resp.json()['name']}    ${name}

    Log    Group ${group_id} created and verified


*** Keywords ***
Suite Init
    Create API Session
    ${ts}=    Evaluate    str(int(time.time()))[-6:]    modules=time
    Set Suite Variable    ${RUN_ID}    ${ts}
