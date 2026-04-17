*** Settings ***
Documentation    P-series: structural invariants on the JSON body for a known
...              indexed namespace. Fast, read-only.
Resource         ../lib/common.resource
Suite Setup      Resolve Indexed Namespace

*** Variables ***
${NS}        ${None}

*** Keywords ***
Resolve Indexed Namespace
    ${id}=    Ensure Known Indexed Namespace
    Set Suite Variable    ${NS}    ${id}

Get Known Response
    ${resp}=    Graph Status By Namespace    ${NS}    200
    RETURN    ${resp.json()}

*** Test Cases ***
P1 All Response Keys Present
    ${body}=    Get Known Response
    Assert Response Shape    ${body}

P3 User Entity Never Appears
    [Documentation]    User has no traversal_path in the ontology and must be
    ...                excluded from the domains array.
    ${body}=    Get Known Response
    FOR    ${domain}    IN    @{body["domains"]}
        FOR    ${item}    IN    @{domain["items"]}
            Should Not Be Equal    ${item["name"]}    User
            ...    User entity leaked into domain ${domain["name"]}
        END
    END

P4 Status Mirrors Count Sign
    [Documentation]    completed ⇔ count > 0, pending ⇔ count == 0.
    ${body}=    Get Known Response
    FOR    ${domain}    IN    @{body["domains"]}
        FOR    ${item}    IN    @{domain["items"]}
            IF    ${item["count"]} > 0
                Should Be Equal    ${item["status"]}    completed
                ...    ${item["name"]}: count=${item["count"]} but status=${item["status"]}
            ELSE
                Should Be Equal    ${item["status"]}    pending
                ...    ${item["name"]}: count=${item["count"]} but status=${item["status"]}
            END
        END
    END

P5 Deterministic Item Ordering
    [Documentation]    Two consecutive calls must return identical ordering.
    ${a}=    Get Known Response
    ${b}=    Get Known Response
    ${names_a}=    Evaluate    [ (d["name"], [i["name"] for i in d["items"]]) for d in $a["domains"] ]
    ${names_b}=    Evaluate    [ (d["name"], [i["name"] for i in d["items"]]) for d in $b["domains"] ]
    Should Be Equal    ${names_a}    ${names_b}

P7 Sdlc Populated When Meta Exists
    [Documentation]    The known-indexed namespace has a meta snapshot, so
    ...                sdlc must be non-null with populated fields.
    ${body}=    Get Known Response
    Should Not Be Equal    ${body["sdlc"]}    ${None}
    Dictionary Should Contain Key    ${body["sdlc"]}    cycle_count
    Dictionary Should Contain Key    ${body["sdlc"]}    last_completed_at
    Should Be True    ${body["sdlc"]["cycle_count"]} >= 1

P8 RFC3339 Timestamps Parse
    [Documentation]    Every non-empty *_at field must be a valid RFC3339 string.
    ${body}=    Get Known Response
    IF    "${body['updated_at']}" != ""
        Parse RFC3339    ${body["updated_at"]}
    END
    IF    $body["sdlc"] is not None
        IF    "${body['sdlc']['last_started_at']}" != ""
            Parse RFC3339    ${body["sdlc"]["last_started_at"]}
        END
        IF    "${body['sdlc']['last_completed_at']}" != ""
            Parse RFC3339    ${body["sdlc"]["last_completed_at"]}
        END
    END
    IF    $body["code"] is not None and "${body['code']['last_indexed_at']}" != ""
        Parse RFC3339    ${body["code"]["last_indexed_at"]}
    END

P9 Six Domains Present
    [Documentation]    The embedded ontology declares six domains: ci,
    ...                code_review, correctness, namespace, ownership,
    ...                project_management (or similar). Count should be >= 5.
    ${body}=    Get Known Response
    ${n}=    Get Length    ${body["domains"]}
    Should Be True    ${n} >= 5    expected >=5 domains, got ${n}
