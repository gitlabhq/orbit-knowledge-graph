*** Settings ***
Documentation    D-series: namespace deletion lifecycle. Verifies that
...              disabling KG eventually clears KV keys and returns pending.
...              The deletion handler runs on a daily cron in prod, so these
...              tests poll with a generous timeout.
Resource         ../lib/common.resource

*** Variables ***
${WAIT_IDLE_SECS}           %{WAIT_IDLE_SECS=300}
${DELETION_WAIT_SECS}       %{DELETION_WAIT_SECS=600}

*** Test Cases ***
D1 Disable KG Eventually Pending
    [Documentation]    Full lifecycle: enable, wait for idle, disable, wait
    ...                for deletion handler to run, assert state=pending.
    ${suffix}=    Random Suffix
    ${group}=    Create Group    d1-${suffix}    d1-${suffix}
    ${gid}=    Set Variable    ${group["id"]}
    Enable Knowledge Graph    ${gid}
    Wait For Idle    ${gid}    ${WAIT_IDLE_SECS}s

    Disable Knowledge Graph    ${gid}

    # Deletion is scheduled once daily in prod. For the e2e VM we just wait
    # for the dispatcher to pick it up; in practice the schedule can be
    # bumped via config. Accept either 404 (group gone from Rails view) or
    # 200 with state=pending (KV cleaned).
    Wait Until Keyword Succeeds    ${DELETION_WAIT_SECS}s    30s
    ...    Post Deletion State Is Reset    ${gid}
    [Teardown]    Delete Group    ${gid}

D2 Re Enable Before Deletion Preserves KV
    [Documentation]    Disable then re-enable quickly. The re-enabled path
    ...                in the namespace deletion handler MUST NOT purge KV
    ...                keys, so cycle_count and counts survive.
    ${suffix}=    Random Suffix
    ${group}=    Create Group    d2-${suffix}    d2-${suffix}
    ${gid}=    Set Variable    ${group["id"]}
    Enable Knowledge Graph    ${gid}
    Wait For Idle    ${gid}    ${WAIT_IDLE_SECS}s

    ${before}=    Graph Status By Namespace    ${gid}    200
    ${cycle_before}=    Set Variable    ${before.json()["sdlc"]["cycle_count"]}

    Disable Knowledge Graph    ${gid}
    Sleep    5s
    Enable Knowledge Graph    ${gid}

    # The re-enabled namespace keeps its prior KV data. Accept any cycle ≥ before.
    ${after}=    Graph Status By Namespace    ${gid}    200
    ${cycle_after}=    Set Variable    ${after.json()["sdlc"]["cycle_count"]}
    Should Be True    ${cycle_after} >= ${cycle_before}
    ...    cycle regressed ${cycle_before} -> ${cycle_after} on re-enable
    [Teardown]    Delete Group    ${gid}

*** Keywords ***
Post Deletion State Is Reset
    [Arguments]    ${gid}
    ${resp}=    Graph Status By Namespace    ${gid}
    IF    ${resp.status_code} == 404
        RETURN
    END
    Should Be Equal As Integers    ${resp.status_code}    200
    # After deletion, either state=pending (KV purged) or the group is gone.
    Should Be Equal    ${resp.json()["state"]}    pending
    ...    post-deletion state=${resp.json()["state"]} (expected pending or 404)
