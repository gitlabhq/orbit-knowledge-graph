*** Settings ***
Documentation       Verify incremental SDLC reconciliation: a Rails-side row update propagates a new
...                 version into the graph, and a Rails-side delete tombstones the row so Orbit stops
...                 returning it. Reuses the shared namespace provisioned in 01.

Resource            gitlab.resource
Resource            orbit.resource


*** Test Cases ***
Issue Title Update Propagates To The Graph
    [Documentation]    Index an issue, rename it in Rails, then assert the WorkItem label reflects
    ...                the new title (the ReplacingMergeTree picks up the newer version).
    [Tags]    incremental
    ${suffix}=    Random Suffix
    Start Indexing Budget    180
    ${project}=    Create Project    e2e-inc-prj-${suffix}    ${SHARED_NAMESPACE_ID}
    ${issue}=    Create Issue    ${project["id"]}    e2e-inc-before-${suffix}
    Wait For Node Indexed Within Budget    WorkItem    ${issue["id"]}    e2e-inc-before-${suffix}
    ...    label_field=title

    ${new_title}=    Set Variable    e2e-inc-after-${suffix}
    Update Issue Title    ${project["id"]}    ${issue["iid"]}    ${new_title}
    Start Indexing Budget    180
    Wait For Node Indexed Within Budget    WorkItem    ${issue["id"]}    ${new_title}    label_field=title

Note Delete Tombstones The Graph Node
    [Documentation]    Index a note, delete it in Rails, then assert Orbit no longer returns it.
    ...                Uses a Note (a direct Siphon table) so the delete propagates without depending
    ...                on the work_items materialized view.
    [Tags]    incremental
    ${suffix}=    Random Suffix
    Start Indexing Budget    180
    ${project}=    Create Project    e2e-del-prj-${suffix}    ${SHARED_NAMESPACE_ID}
    ${issue}=    Create Issue    ${project["id"]}    e2e-del-issue-${suffix}
    ${note}=    Create Note On Issue    ${project["id"]}    ${issue["iid"]}    e2e-del-note-${suffix}
    Wait For Node Indexed Within Budget    Note    ${note["id"]}

    Delete Note On Issue    ${project["id"]}    ${issue["iid"]}    ${note["id"]}
    Wait For Node Removed    Note    ${note["id"]}
