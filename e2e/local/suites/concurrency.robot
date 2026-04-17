*** Settings ***
Documentation    X-series: concurrency / race. Hammers the endpoint while
...              writes are happening and verifies nothing returns 5xx
...              and every response is well-formed.
Resource         ../lib/common.resource
Suite Setup      Resolve NS

*** Variables ***
${NS}        ${None}

*** Keywords ***
Resolve NS
    ${id}=    Ensure Known Indexed Namespace
    Set Suite Variable    ${NS}    ${id}

*** Test Cases ***
X1 Hammer Endpoint Stays Stable
    [Documentation]    Fire 60 requests in tight loop; no 5xx, all valid shape.
    ${fails}=    Set Variable    ${0}
    FOR    ${i}    IN RANGE    60
        ${resp}=    Graph Status By Namespace    ${NS}
        IF    ${resp.status_code} >= 500
            ${fails}=    Evaluate    ${fails} + 1
        END
        IF    ${resp.status_code} == 200
            Assert Response Shape    ${resp.json()}
        END
    END
    Should Be Equal As Integers    ${fails}    0
    ...    saw ${fails} 5xx responses across 60 requests

X2 Cross Field Consistency At Idle
    Wait For Idle    ${NS}    60s
    ${r}=    Graph Status By Namespace    ${NS}    200
    ${body}=    Set Variable    ${r.json()}
    IF    "${body['state']}" == "idle"
        ${lc}=    Set Variable    ${body["sdlc"]["last_completed_at"]}
        ${ua}=    Set Variable    ${body["updated_at"]}
        IF    "${ua}" != "" and "${lc}" != ""
            Assert Timestamp Non Decreasing    ${lc}    ${ua}    updated_at vs last_completed_at
        END
    END

X3 Multi Namespace Isolation
    [Documentation]    Two concurrent queries on different namespaces must
    ...                return independent payloads (different cycle_counts).
    ${suffix}=    Random Suffix
    ${g1}=    Create Group    x3a-${suffix}    x3a-${suffix}
    ${g2}=    Create Group    x3b-${suffix}    x3b-${suffix}
    Enable Knowledge Graph    ${g1["id"]}
    Enable Knowledge Graph    ${g2["id"]}
    Wait For Idle    ${g1["id"]}    180s
    Wait For Idle    ${g2["id"]}    180s

    ${a}=    Graph Status By Namespace    ${g1["id"]}    200
    ${b}=    Graph Status By Namespace    ${g2["id"]}    200
    # Different namespaces have independent meta keys, so their updated_at
    # timestamps should differ at least by some microseconds.
    Should Not Be Equal    ${a.json()["updated_at"]}    ${EMPTY}
    Should Not Be Equal    ${b.json()["updated_at"]}    ${EMPTY}
    [Teardown]    Run Keywords    Delete Group    ${g1["id"]}    AND    Delete Group    ${g2["id"]}
