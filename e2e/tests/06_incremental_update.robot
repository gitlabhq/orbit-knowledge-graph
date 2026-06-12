*** Settings ***
Documentation       Verify incremental SDLC reconciliation: a Rails-side delete propagates to the
...                 graph so Orbit stops returning the row. Reuses the shared namespace from 01.
...
...                 Designed to avoid flakiness:
...                 - Uses a Note, a direct Siphon table (no work_items / traversal-path materialized
...                   view), so the delete is a clean state transition driven only by the note row's
...                   own _siphon_replicated_at bump.
...                 - Gates on the note being fully indexed BEFORE deleting, so create and delete
...                   cannot collapse into a single observed state.
...                 - Node reads use FINAL (compiler node_table_reads_use_final_for_latest_rows), so
...                   the _deleted tombstone hides the row immediately, with no dependence on
...                   background ReplacingMergeTree merges. The assertion polls within a budget,
...                   converging as soon as replication catches up rather than on a fixed sleep.
...
...                 A value-update assertion is intentionally omitted: SDLC field updates propagate
...                 through entity-specific materialized views and path-keyed watermarks (a project
...                 rename does not re-fire the Project pipeline at all), which makes a non-flaky
...                 changed-value check hard. A delete is the clean reconciliation signal.

Resource            gitlab.resource
Resource            orbit.resource

Suite Setup         Attach To Shared Fixture


*** Test Cases ***
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
