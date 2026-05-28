*** Settings ***
Documentation       Verify incremental SDLC reconciliation: a Rails-side update propagates a new
...                 version into the graph, and a Rails-side delete tombstones the row so Orbit
...                 stops returning it. Reuses the shared namespace provisioned in 01.
...
...                 Designed to avoid flakiness:
...                 - Both cases gate on the entity being fully indexed BEFORE the mutation, so the
...                   create and the mutation can never collapse into a single indexed version.
...                 - The update uses a Project rename, which re-indexes through the same PUT path
...                   Touch Project already depends on (a direct table, no work_items MV hop).
...                 - The delete uses a Note (a direct Siphon table, no MV).
...                 - Node reads use FINAL (see compiler node_table_reads_use_final_for_latest_rows),
...                   so the latest version wins and _deleted rows drop immediately, with no
...                   dependence on background ReplacingMergeTree merges. Assertions poll within a
...                   budget, converging as soon as replication catches up rather than on a sleep.

Resource            gitlab.resource
Resource            orbit.resource


*** Test Cases ***
Project Rename Propagates To The Graph
    [Documentation]    Index a project, rename it in Rails, then assert the Project label reflects
    ...                the new name. FINAL guarantees the query returns the single latest version.
    [Tags]    incremental
    ${suffix}=    Random Suffix
    Start Indexing Budget    240
    ${project}=    Create Project    e2e-inc-prj-${suffix}    ${SHARED_NAMESPACE_ID}
    Wait For Node Indexed Within Budget    Project    ${project["id"]}    e2e-inc-prj-${suffix}

    ${new_name}=    Set Variable    e2e-inc-renamed-${suffix}
    Rename Project    ${project["id"]}    ${new_name}
    Start Indexing Budget    240
    Wait For Node Indexed Within Budget    Project    ${project["id"]}    ${new_name}

Note Delete Tombstones The Graph Node
    [Documentation]    Index a note, delete it in Rails, then assert Orbit no longer returns it.
    [Tags]    incremental
    ${suffix}=    Random Suffix
    Start Indexing Budget    240
    ${project}=    Create Project    e2e-del-prj-${suffix}    ${SHARED_NAMESPACE_ID}
    ${issue}=    Create Issue    ${project["id"]}    e2e-del-issue-${suffix}
    ${note}=    Create Note On Issue    ${project["id"]}    ${issue["iid"]}    e2e-del-note-${suffix}
    Wait For Node Indexed Within Budget    Note    ${note["id"]}

    Delete Note On Issue    ${project["id"]}    ${issue["iid"]}    ${note["id"]}
    Wait For Node Removed    Note    ${note["id"]}    timeout=240s
