*** Settings ***
Documentation    T/M/F-series: state transitions, monotonic invariants,
...              staleness. Uses polling; tune timeouts via %{WAIT_IDLE_SECS}.
Resource         ../lib/common.resource
Suite Setup      Resolve Known NS

*** Variables ***
${NS}                     ${None}
${WAIT_IDLE_SECS}         %{WAIT_IDLE_SECS=300}
${DISPATCH_CYCLE_SECS}    %{DISPATCH_CYCLE_SECS=150}

*** Keywords ***
Resolve Known NS
    ${id}=    Ensure Known Indexed Namespace
    Set Suite Variable    ${NS}    ${id}

*** Test Cases ***
T1 First Cycle Transitions To Idle
    [Documentation]    Fresh namespace walks from pending (or missing) to idle
    ...                within ${WAIT_IDLE_SECS} seconds once KG is enabled.
    ${suffix}=    Random Suffix
    ${group}=    Create Group    t1-${suffix}    t1-${suffix}
    ${gid}=    Set Variable    ${group["id"]}

    # Snapshot before enabling: must be 404 or pending.
    ${pre}=    Graph Status By Namespace    ${gid}
    Should Be True    ${pre.status_code} in [200, 404]
    IF    ${pre.status_code} == 200
        Should Be Equal    ${pre.json()["state"]}    pending
    END

    Enable Knowledge Graph    ${gid}
    Wait For Idle    ${gid}    ${WAIT_IDLE_SECS}s

    ${final}=    Graph Status By Namespace    ${gid}    200
    Should Be Equal    ${final.json()["state"]}    idle
    Should Be True    ${final.json()["initial_backfill_done"]}
    [Teardown]    Delete Group    ${gid}

T2 Subsequent Cycle Advances Cycle Count
    [Documentation]    Observe cycle_count advance by ≥ 1 on a namespace
    ...                already at idle.
    ${a}=    Graph Status By Namespace    ${NS}    200
    ${c1}=    Set Variable    ${a.json()["sdlc"]["cycle_count"]}
    Sleep    ${DISPATCH_CYCLE_SECS}s
    ${b}=    Graph Status By Namespace    ${NS}    200
    ${c2}=    Set Variable    ${b.json()["sdlc"]["cycle_count"]}
    Should Be True    ${c2} >= ${c1} + 1
    ...    expected at least one additional cycle (${c1} -> ${c2}) over ${DISPATCH_CYCLE_SECS}s

T3 Zero Row Skip Preserves Counts
    [Documentation]    On a no-new-data cycle, entity totals remain stable
    ...                while cycle_count and updated_at advance.
    ${a}=    Graph Status By Namespace    ${NS}    200
    ${total_a}=    Sum Entity Counts    ${a.json()["domains"]}
    ${upd_a}=    Set Variable    ${a.json()["updated_at"]}
    Sleep    ${DISPATCH_CYCLE_SECS}s
    ${b}=    Graph Status By Namespace    ${NS}    200
    ${total_b}=    Sum Entity Counts    ${b.json()["domains"]}
    ${upd_b}=    Set Variable    ${b.json()["updated_at"]}
    Should Be Equal As Integers    ${total_a}    ${total_b}
    ...    entity totals drifted without new data (${total_a} -> ${total_b})
    Assert Timestamp Non Decreasing    ${upd_a}    ${upd_b}    updated_at

T4 State Never Regresses To Pending
    [Documentation]    After observing idle once, state must not drop back
    ...                to pending across subsequent polls.
    ${seen_idle}=    Set Variable    ${False}
    FOR    ${i}    IN RANGE    6
        ${r}=    Graph Status By Namespace    ${NS}    200
        ${s}=    Set Variable    ${r.json()["state"]}
        IF    "${s}" == "idle"
            ${seen_idle}=    Set Variable    ${True}
        END
        IF    ${seen_idle}
            Should Not Be Equal    ${s}    pending
            ...    state regressed to pending after idle was observed
        END
        Sleep    30s
    END
    Should Be True    ${seen_idle}    never observed idle in 180s

M1 Initial Backfill Done Monotonic
    ${snapshots}=    Poll Graph Status    ${NS}    5    20
    ${seen_true}=    Set Variable    ${False}
    FOR    ${snap}    IN    @{snapshots}
        IF    ${snap["initial_backfill_done"]}
            ${seen_true}=    Set Variable    ${True}
        END
        IF    ${seen_true}
            Should Be True    ${snap["initial_backfill_done"]}
            ...    regressed to false after observing true
        END
    END

M2 Cycle Count Non Decreasing
    ${snapshots}=    Poll Graph Status    ${NS}    5    20
    ${prev}=    Set Variable    ${-1}
    FOR    ${snap}    IN    @{snapshots}
        ${c}=    Set Variable    ${snap["sdlc"]["cycle_count"]}
        Should Be True    ${c} >= ${prev}
        ...    cycle_count regressed ${prev} -> ${c}
        ${prev}=    Set Variable    ${c}
    END

M5 Updated At Non Decreasing
    ${snapshots}=    Poll Graph Status    ${NS}    5    20
    ${prev}=    Set Variable    ${EMPTY}
    FOR    ${snap}    IN    @{snapshots}
        Assert Timestamp Non Decreasing    ${prev}    ${snap["updated_at"]}    updated_at
        ${prev}=    Set Variable    ${snap["updated_at"]}
    END

M6 Started At Precedes Completed At When Idle
    Wait For Idle    ${NS}    60s
    ${r}=    Graph Status By Namespace    ${NS}    200
    ${sdlc}=    Set Variable    ${r.json()["sdlc"]}
    ${started}=    Set Variable    ${sdlc["last_started_at"]}
    ${completed}=    Set Variable    ${sdlc["last_completed_at"]}
    Should Not Be Empty    ${started}
    Should Not Be Empty    ${completed}
    Assert Timestamp Non Decreasing    ${started}    ${completed}    last_started_at vs last_completed_at

M8 Completed Advances With Cycle
    ${a}=    Graph Status By Namespace    ${NS}    200
    ${c1}=    Set Variable    ${a.json()["sdlc"]["cycle_count"]}
    ${t1}=    Set Variable    ${a.json()["sdlc"]["last_completed_at"]}
    Sleep    ${DISPATCH_CYCLE_SECS}s
    ${b}=    Graph Status By Namespace    ${NS}    200
    ${c2}=    Set Variable    ${b.json()["sdlc"]["cycle_count"]}
    ${t2}=    Set Variable    ${b.json()["sdlc"]["last_completed_at"]}
    IF    ${c2} > ${c1}
        Assert Timestamp Non Decreasing    ${t1}    ${t2}    last_completed_at
        Should Not Be Equal    ${t1}    ${t2}
        ...    last_completed_at did not advance despite cycle_count ${c1}->${c2}
    END

M9 Last Duration Ms Is Positive After Cycle
    ${r}=    Graph Status By Namespace    ${NS}    200
    ${d}=    Set Variable    ${r.json()["sdlc"]["last_duration_ms"]}
    Should Be True    ${d} >= 0
    Should Be True    ${d} < 300000
    ...    last_duration_ms ${d} unreasonably large (>5min)

F2 Stale False Right After Idle
    Wait For Idle    ${NS}    60s
    ${r}=    Graph Status By Namespace    ${NS}    200
    Should Be Equal    ${r.json()["stale"]}    ${False}
    ...    stale must be false right after idle, updated_at=${r.json()["updated_at"]}

F3 Fresh Namespace Stale True
    ${suffix}=    Random Suffix
    ${group}=    Create Group    f3-${suffix}    f3-${suffix}
    ${gid}=    Set Variable    ${group["id"]}
    Enable Knowledge Graph    ${gid}
    ${r}=    Graph Status By Namespace    ${gid}    200
    # Immediately after enabling, no counts key exists -> stale=true.
    IF    "${r.json()['updated_at']}" == ""
        Should Be Equal    ${r.json()["stale"]}    ${True}
    END
    [Teardown]    Delete Group    ${gid}
