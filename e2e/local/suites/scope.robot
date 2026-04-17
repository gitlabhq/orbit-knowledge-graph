*** Settings ***
Documentation    S-series: the three scopes (namespace_id / project_id /
...              full_path) must resolve to consistent meta and well-formed
...              counts. Subgroup queries inherit top-group state.
Resource         ../lib/common.resource
Suite Setup      Prepare Fixtures
Suite Teardown   Teardown Fixtures

*** Variables ***
${WAIT_IDLE_SECS}    %{WAIT_IDLE_SECS=300}
${TOP_ID}            ${None}
${TOP_PATH}          ${None}
${SUB_ID}            ${None}
${SUB_PATH}          ${None}
${DEEP_ID}           ${None}
${DEEP_PATH}         ${None}
${PROJECT_ID}        ${None}
${PROJECT_PATH}      ${None}

*** Keywords ***
Prepare Fixtures
    ${suffix}=    Random Suffix
    Set Suite Variable    ${SUFFIX}    ${suffix}
    # top > sub > deep, with one project under each of sub and deep.
    ${top}=    Create Group    s-top-${suffix}    s-top-${suffix}
    ${sub}=    Create Group    sub-${suffix}    sub-${suffix}    ${top["id"]}
    ${deep}=    Create Group    deep-${suffix}    deep-${suffix}    ${sub["id"]}
    ${proj}=    Create Project    p-${suffix}    ${sub["id"]}
    Set Suite Variable    ${TOP_ID}        ${top["id"]}
    Set Suite Variable    ${TOP_PATH}      ${top["full_path"]}
    Set Suite Variable    ${SUB_ID}        ${sub["id"]}
    Set Suite Variable    ${SUB_PATH}      ${sub["full_path"]}
    Set Suite Variable    ${DEEP_ID}       ${deep["id"]}
    Set Suite Variable    ${DEEP_PATH}     ${deep["full_path"]}
    Set Suite Variable    ${PROJECT_ID}    ${proj["id"]}
    Set Suite Variable    ${PROJECT_PATH}    ${proj["path_with_namespace"]}
    Enable Knowledge Graph    ${TOP_ID}
    Wait For Idle    ${TOP_ID}    ${WAIT_IDLE_SECS}s

Teardown Fixtures
    IF    $TOP_ID is not None
        Delete Group    ${TOP_ID}
    END

*** Test Cases ***
S1 Project Scope Returns 200 With Code Section
    [Documentation]    `projects_total` is populated by the code-progress
    ...                writer on the next cycle that sees the project row.
    ...                The CI may need a couple of cycles after the group
    ...                reaches idle before it lands — poll for it.
    ${resp}=    Graph Status By Project    ${PROJECT_ID}    200
    ${body}=    Set Variable    ${resp.json()}
    Assert Response Shape    ${body}
    Should Be Equal    ${body["state"]}    idle
    Dictionary Should Contain Key    ${body}    code
    Wait Until Keyword Succeeds    360s    15s
    ...    Projects Total At Least    ${TOP_ID}    1

S2 Full Path And Namespace Id Agree
    ${a}=    Graph Status By Namespace    ${TOP_ID}    200
    ${b}=    Graph Status By Full Path    ${TOP_PATH}    200
    ${ja}=    Set Variable    ${a.json()}
    ${jb}=    Set Variable    ${b.json()}
    Should Be Equal    ${ja["state"]}    ${jb["state"]}
    Should Be Equal    ${ja["initial_backfill_done"]}    ${jb["initial_backfill_done"]}
    Should Be Equal    ${ja["sdlc"]["cycle_count"]}    ${jb["sdlc"]["cycle_count"]}
    # updated_at, edge_counts, domain counts may differ only if a cycle landed
    # between the two calls. Assert meta fields match; counts may be ±1 cycle.

S3 Subgroup Inherits Top Group Meta
    ${top}=    Graph Status By Namespace    ${TOP_ID}    200
    ${sub}=    Graph Status By Full Path    ${SUB_PATH}    200
    # state and initial_backfill_done come from meta.<root_ns_id>, so they
    # must match between top and subgroup.
    Should Be Equal    ${top.json()["state"]}    ${sub.json()["state"]}
    Should Be Equal    ${top.json()["initial_backfill_done"]}    ${sub.json()["initial_backfill_done"]}
    Should Be Equal    ${top.json()["sdlc"]["cycle_count"]}    ${sub.json()["sdlc"]["cycle_count"]}

S4 Subgroup Totals At Most Equal Parent
    ${top}=    Graph Status By Namespace    ${TOP_ID}    200
    ${sub}=    Graph Status By Full Path    ${SUB_PATH}    200
    ${total_top}=    Sum Entity Counts    ${top.json()["domains"]}
    ${total_sub}=    Sum Entity Counts    ${sub.json()["domains"]}
    Should Be True    ${total_sub} <= ${total_top}
    ...    subgroup total ${total_sub} must be <= parent total ${total_top}

S5 Deep Nested Path Resolves
    ${resp}=    Graph Status By Full Path    ${DEEP_PATH}    200
    Assert Response Shape    ${resp.json()}
    # Deep group has no project, so may report small or zero counts but must
    # still resolve successfully and share the top-group's state.
    Should Be Equal    ${resp.json()["state"]}    idle

*** Keywords ***
Projects Total At Least
    [Arguments]    ${ns_id}    ${n}
    ${r}=    Graph Status By Namespace    ${ns_id}    200
    Should Be True    ${r.json()["code"]["projects_total"]} >= ${n}
    ...    projects_total=${r.json()["code"]["projects_total"]} (want >= ${n})

*** Test Cases ***
S6 Project Without Code Index Reports Source Code Pending
    [Documentation]    A project that hasn't had a push indexed yet should
    ...                show source_code items all at count=0 / status=pending.
    ${resp}=    Graph Status By Project    ${PROJECT_ID}    200
    ${body}=    Set Variable    ${resp.json()}
    FOR    ${domain}    IN    @{body["domains"]}
        IF    "${domain["name"]}" == "source_code"
            FOR    ${item}    IN    @{domain["items"]}
                # The item may or may not exist; if present and count=0, status must be pending.
                IF    ${item["count"]} == 0
                    Should Be Equal    ${item["status"]}    pending
                    ...    ${item["name"]}: zero count but status=${item["status"]}
                END
            END
        END
    END
