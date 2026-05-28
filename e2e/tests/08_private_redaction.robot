*** Settings ***
Documentation       Verify per-resource redaction on a normal traversal: a private project and its
...                 issue are hidden from a user who cannot read them, and become visible once the
...                 user is granted membership. Complements 05 (which covers the security-role
...                 threshold for Vulnerability aggregation) by exercising read_project on Project
...                 and WorkItem.
...
...                 The victim is a Reporter on the shared (enabled) namespace so they clear the
...                 require_enabled_namespaces gate, but is NOT a member of the target group, so the
...                 private project there must be redacted until membership is granted.

Resource            gitlab.resource
Resource            orbit.resource

Suite Setup         Build Private Redaction Fixture


*** Test Cases ***
Non-Member Cannot See Private Project Or Issue
    [Documentation]    The victim has no access to the target group, so both the private project and
    ...                its issue are redacted from the victim's query results.
    [Tags]    authz    redaction
    Victim Node Count Is    Project    ${TARGET_PROJECT_ID}    0
    Victim Node Count Is    WorkItem    ${TARGET_ISSUE_ID}    0

Member Can See Private Project And Issue
    [Documentation]    Granting Reporter on the target group reveals the previously redacted rows.
    [Tags]    authz    redaction
    Add Group Member    ${TARGET_GROUP_ID}    ${VICTIM_USER_ID}    20
    Wait Until Keyword Succeeds    60s    3s    Victim Sees Node    Project    ${TARGET_PROJECT_ID}
    Wait Until Keyword Succeeds    60s    3s    Victim Sees Node    WorkItem    ${TARGET_ISSUE_ID}


*** Keywords ***
Build Private Redaction Fixture
    [Documentation]    Mint a victim with Reporter on the shared namespace (the enabled-namespace
    ...                gate), provision a separate target group with a private project + issue, and
    ...                wait for them to index.
    ${suffix}=    Random Suffix
    ${victim}=    Create User    redaction-${suffix}-victim
    ${root_headers}=    Root Auth Headers
    ${victim_pat}=    Issue PAT For User    ${root_headers}    ${victim["id"]}
    Set Suite Variable    ${VICTIM_USER_ID}    ${victim["id"]}
    Set Suite Variable    ${VICTIM_PAT}    ${victim_pat}
    Add Group Member    ${SHARED_NAMESPACE_ID}    ${victim["id"]}    20

    ${target_group}=    Create Group    redaction-${suffix}-target
    Set Suite Variable    ${TARGET_GROUP_ID}    ${target_group["id"]}
    Enable Knowledge Graph    ${TARGET_GROUP_ID}

    Start Indexing Budget    300
    ${project}=    Create Project    redaction-${suffix}-prj    ${TARGET_GROUP_ID}    visibility=private
    ${issue}=    Create Issue    ${project["id"]}    redaction-${suffix}-issue
    Set Suite Variable    ${TARGET_PROJECT_ID}    ${project["id"]}
    Set Suite Variable    ${TARGET_ISSUE_ID}    ${issue["id"]}
    Wait For Node Indexed Within Budget    Project    ${TARGET_PROJECT_ID}    redaction-${suffix}-prj
    Wait For Node Indexed Within Budget    WorkItem    ${TARGET_ISSUE_ID}    redaction-${suffix}-issue
    ...    label_field=title

Victim Node Count
    [Documentation]    Single-node lookup by id, run with the victim's PAT, returning the redacted
    ...                row count.
    [Arguments]    ${entity}    ${node_id}
    ${node}=    Create Dictionary    id=n    entity=${entity}    node_ids=${{[int($node_id)]}}
    ${query}=    Create Dictionary    query_type=traversal    node=${node}
    ${resp}=    Orbit Query With Token    ${query}    ${VICTIM_PAT}
    RETURN    ${resp["row_count"]}

Victim Node Count Is
    [Arguments]    ${entity}    ${node_id}    ${expected}
    ${count}=    Victim Node Count    ${entity}    ${node_id}
    Should Be Equal As Integers    ${count}    ${expected}
    ...    ${entity} ${node_id}: victim saw ${count} rows, expected ${expected}

Victim Sees Node
    [Arguments]    ${entity}    ${node_id}
    ${count}=    Victim Node Count    ${entity}    ${node_id}
    Should Be True    ${count} >= 1    ${entity} ${node_id} not visible to victim (count=${count})
