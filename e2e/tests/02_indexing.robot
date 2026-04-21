*** Settings ***
Documentation       Verify SDLC entities AND their relationships flow through
...                 PG → Siphon → ClickHouse → GKG and become queryable via Orbit.
...                 Assumes the pipeline reached steady state in 01_setup_and_smoke (canary
...                 issue + note already indexed). Each case opens its own 180s budget that
...                 all per-entity waits draw from, so transient Siphon MV races (project
...                 row landing before its namespace's traversal_path materializes) resolve
...                 via the reconciler without bloating per-call timeouts.

Resource            gitlab.resource
Resource            orbit.resource


*** Test Cases ***
Project Issue And Note Are Indexed
    [Documentation]    Create a project, an issue inside it, and a note on that issue,
    ...                then assert each becomes queryable via Orbit.
    [Tags]    indexing
    ${suffix}=    Random Suffix
    Start Indexing Budget    180

    ${project_name}=    Set Variable    e2e-prj-${suffix}
    ${project}=    Create Project    ${project_name}    ${SHARED_NAMESPACE_ID}
    Wait For Node Indexed Within Budget    Project    ${project["id"]}    ${project_name}

    ${issue_title}=    Set Variable    e2e-issue-${suffix}
    ${issue}=    Create Issue    ${project["id"]}    ${issue_title}
    Wait For Node Indexed Within Budget    WorkItem    ${issue["id"]}    ${issue_title}
    ...    label_field=title

    ${note_body}=    Set Variable    e2e-note-${suffix}
    ${note}=    Create Note On Issue    ${project["id"]}    ${issue["iid"]}    ${note_body}
    Wait For Node Indexed Within Budget    Note    ${note["id"]}

Epic Issue And Notes Hierarchy Is Indexed
    [Documentation]    Build a full planning hierarchy in the shared group: an epic at the
    ...                group level, a project under it, an issue in that project parented to
    ...                the epic, and two notes on the issue. Assert every node becomes
    ...                queryable via Orbit. Stores the entities as suite variables so the
    ...                next case can assert the relationships between them.
    [Tags]    indexing    hierarchy
    ${suffix}=    Random Suffix
    Start Indexing Budget    180

    ${epic_title}=    Set Variable    e2e-epic-${suffix}
    ${epic}=    Create Epic    ${SHARED_NAMESPACE_ID}    ${epic_title}
    Wait For Node Indexed Within Budget    WorkItem    ${epic["work_item_id"]}    ${epic_title}
    ...    label_field=title

    ${project_name}=    Set Variable    e2e-hier-prj-${suffix}
    ${project}=    Create Project    ${project_name}    ${SHARED_NAMESPACE_ID}
    Wait For Node Indexed Within Budget    Project    ${project["id"]}    ${project_name}

    ${issue_title}=    Set Variable    e2e-hier-issue-${suffix}
    ${issue}=    Create Issue    ${project["id"]}    ${issue_title}
    Wait For Node Indexed Within Budget    WorkItem    ${issue["id"]}    ${issue_title}
    ...    label_field=title

    Add Issue To Epic    ${SHARED_NAMESPACE_ID}    ${epic["iid"]}    ${issue["id"]}

    ${note1}=    Create Note On Issue    ${project["id"]}    ${issue["iid"]}    e2e-note-a-${suffix}
    ${note2}=    Create Note On Issue    ${project["id"]}    ${issue["iid"]}    e2e-note-b-${suffix}
    Wait For Node Indexed Within Budget    Note    ${note1["id"]}
    Wait For Node Indexed Within Budget    Note    ${note2["id"]}

    Set Suite Variable    ${HIER_EPIC}    ${epic}
    Set Suite Variable    ${HIER_PROJECT}    ${project}
    Set Suite Variable    ${HIER_ISSUE}    ${issue}
    Set Suite Variable    ${HIER_NOTE1}    ${note1}
    Set Suite Variable    ${HIER_NOTE2}    ${note2}

Hierarchy Is Linked By Expected Edges
    [Documentation]    Assert the edges the indexer must materialize for the hierarchy built
    ...                in the previous case: CONTAINS (Epic→Issue — the parent link),
    ...                IN_GROUP (Epic→Group), IN_PROJECT (Issue→Project), and AUTHORED
    ...                (User→Issue, User→Note). Authored edges match any User, since the
    ...                indexer's bot user id isn't known to the suite.
    [Tags]    indexing    hierarchy    edges
    Start Indexing Budget    180

    Wait For Edge Indexed Within Budget    WorkItem    ${HIER_EPIC}[work_item_id]
    ...    CONTAINS    WorkItem    ${HIER_ISSUE}[id]

    Wait For Edge Indexed Within Budget    WorkItem    ${HIER_EPIC}[work_item_id]
    ...    IN_GROUP    Group    ${SHARED_NAMESPACE_ID}

    Wait For Edge Indexed Within Budget    WorkItem    ${HIER_ISSUE}[id]
    ...    IN_PROJECT    Project    ${HIER_PROJECT}[id]

    Wait For Edge Indexed Within Budget    User    ${None}
    ...    AUTHORED    WorkItem    ${HIER_ISSUE}[id]

    Wait For Edge Indexed Within Budget    User    ${None}
    ...    AUTHORED    Note    ${HIER_NOTE1}[id]

    Wait For Edge Indexed Within Budget    User    ${None}
    ...    AUTHORED    Note    ${HIER_NOTE2}[id]
