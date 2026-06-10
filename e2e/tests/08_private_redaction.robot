*** Settings ***
Documentation       Verify per-resource redaction on a normal traversal. A private project and its
...                 issue are present in the graph (the admin e2e-bot can query them) but are
...                 redacted from a user who cannot read them. Complements 05 (the security-role
...                 threshold for Vulnerability aggregation) by exercising read_project redaction on
...                 Project and WorkItem.
...
...                 The control (admin sees the node) and the redacted query (victim sees zero) use
...                 the same single-node lookup, so a zero result proves redaction rather than the
...                 node being absent. The victim is a Reporter on the shared (enabled) namespace so
...                 it clears the require_enabled_namespaces gate, but is NOT a member of the target
...                 group, so the private project there is redacted. Each principal queries with its
...                 own fixed membership, so there is no membership-propagation timing to race.

Resource            gitlab.resource
Resource            orbit.resource

Suite Setup         Run Keywords    Attach To Shared Fixture    AND    Build Private Redaction Fixture


*** Test Cases ***
Private Project And Issue Are Redacted From A Non-Member
    [Documentation]    Admin sees both nodes (they are indexed); the non-member victim sees neither.
    [Tags]    authz    redaction
    Verify Node Indexed    Project    ${TARGET_PROJECT_ID}
    Verify Node Indexed    WorkItem    ${TARGET_ISSUE_ID}
    Victim Node Count Is    Project    ${TARGET_PROJECT_ID}    0
    Victim Node Count Is    WorkItem    ${TARGET_ISSUE_ID}    0


*** Keywords ***
Build Private Redaction Fixture
    [Documentation]    Mint a victim with Reporter on the shared namespace (the enabled-namespace
    ...                gate), provision a separate target group with a private project + issue, and
    ...                wait for them to index.
    ${suffix}=    Random Suffix
    ${victim}=    Create User    redaction-${suffix}-victim
    ${root_headers}=    Root Auth Headers
    ${victim_pat}=    Issue PAT For User    ${root_headers}    ${victim["id"]}
    Set Suite Variable    ${VICTIM_PAT}    ${victim_pat}
    Add Group Member    ${SHARED_NAMESPACE_ID}    ${victim["id"]}    20

    ${target_group}=    Create Group    redaction-${suffix}-target
    Enable Knowledge Graph    ${target_group["id"]}

    Start Indexing Budget    300
    ${project}=    Create Project    redaction-${suffix}-prj    ${target_group["id"]}    visibility=private
    ${issue}=    Create Issue    ${project["id"]}    redaction-${suffix}-issue
    Set Suite Variable    ${TARGET_PROJECT_ID}    ${project["id"]}
    Set Suite Variable    ${TARGET_ISSUE_ID}    ${issue["id"]}
    Wait For Node Indexed Within Budget    Project    ${TARGET_PROJECT_ID}    redaction-${suffix}-prj
    Wait For Node Indexed Within Budget    WorkItem    ${TARGET_ISSUE_ID}    redaction-${suffix}-issue
    ...    label_field=title

Victim Node Count Is
    [Documentation]    Single-node lookup by id run with the victim's PAT; assert the redacted row
    ...                count equals ${expected}.
    [Arguments]    ${entity}    ${node_id}    ${expected}
    ${node}=    Create Dictionary    id=n    entity=${entity}    node_ids=${{[int($node_id)]}}
    ${query}=    Create Dictionary    query_type=traversal    node=${node}
    ${resp}=    Orbit Query With Token    ${query}    ${VICTIM_PAT}
    Should Be Equal As Integers    ${resp["row_count"]}    ${expected}
    ...    ${entity} ${node_id}: victim saw ${resp["row_count"]} rows, expected ${expected}
