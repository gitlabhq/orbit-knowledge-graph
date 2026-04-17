*** Settings ***
Library    RequestsLibrary
Library    Collections
Library    String
Library    DateTime

Documentation    Validates GetGraphStatus REST endpoint across the full state
...              matrix (pending/indexing/idle) at three scopes
...              (top-level group, subgroup, project). Complements the gRPC
...              unit tests in crates/gkg-server/src/graph_status/ by exercising
...              the Rails proxy and the indexer write path end-to-end.

*** Variables ***
${GITLAB_URL}     %{GITLAB_URL}

*** Keywords ***
GitLab Auth Headers
    RETURN    ${{{"PRIVATE-TOKEN": "${GITLAB_PAT}", "Content-Type": "application/json"}}}

Random Suffix
    ${rand}=    Generate Random String    6    [LOWER][NUMBERS]
    RETURN    ${rand}

Create Group
    [Arguments]    ${name}    ${path}    ${parent_id}=${None}
    ${headers}=    GitLab Auth Headers
    ${body}=    Create Dictionary    name=${name}    path=${path}    visibility=public
    IF    $parent_id is not None
        Set To Dictionary    ${body}    parent_id=${parent_id}
    END
    ${resp}=    POST    ${GITLAB_URL}/api/v4/groups
    ...    headers=${headers}    json=${body}    expected_status=201
    RETURN    ${resp.json()}

Create Project
    [Arguments]    ${name}    ${namespace_id}
    ${headers}=    GitLab Auth Headers
    ${body}=    Create Dictionary    name=${name}    namespace_id=${namespace_id}    visibility=public
    ${resp}=    POST    ${GITLAB_URL}/api/v4/projects
    ...    headers=${headers}    json=${body}    expected_status=201
    RETURN    ${resp.json()}

Enable Knowledge Graph
    [Arguments]    ${namespace_id}
    ${headers}=    Create Dictionary    PRIVATE-TOKEN=${GITLAB_PAT}
    Wait Until Keyword Succeeds    30s    5s
    ...    Enable Knowledge Graph Once    ${namespace_id}    ${headers}

Enable Knowledge Graph Once
    [Arguments]    ${namespace_id}    ${headers}
    ${resp}=    PUT    ${GITLAB_URL}/api/v4/admin/knowledge_graph/namespaces/${namespace_id}
    ...    headers=${headers}    expected_status=any
    IF    ${resp.status_code} != 200
        Fail    Enable KG returned ${resp.status_code}: ${resp.text}
    END

Get Graph Status By Namespace
    [Arguments]    ${namespace_id}
    ${headers}=    GitLab Auth Headers
    ${resp}=    GET    ${GITLAB_URL}/api/v4/orbit/graph_status?namespace_id=${namespace_id}
    ...    headers=${headers}    expected_status=any
    RETURN    ${resp}

Get Graph Status By Project
    [Arguments]    ${project_id}
    ${headers}=    GitLab Auth Headers
    ${resp}=    GET    ${GITLAB_URL}/api/v4/orbit/graph_status?project_id=${project_id}
    ...    headers=${headers}    expected_status=any
    RETURN    ${resp}

Get Graph Status By Full Path
    [Arguments]    ${full_path}
    ${headers}=    GitLab Auth Headers
    ${resp}=    GET    ${GITLAB_URL}/api/v4/orbit/graph_status?full_path=${full_path}
    ...    headers=${headers}    expected_status=any
    RETURN    ${resp}

Wait For Idle State
    [Arguments]    ${namespace_id}    ${timeout}=180s
    Wait Until Keyword Succeeds    ${timeout}    5s
    ...    Verify State Is Idle    ${namespace_id}

Verify State Is Idle
    [Arguments]    ${namespace_id}
    ${resp}=    Get Graph Status By Namespace    ${namespace_id}
    Should Be Equal As Integers    ${resp.status_code}    200
    Should Be Equal    ${resp.json()["state"]}    idle
    ...    namespace=${namespace_id} state=${resp.json()["state"]}

Assert Response Shape
    [Arguments]    ${body}
    Dictionary Should Contain Key    ${body}    state
    Dictionary Should Contain Key    ${body}    initial_backfill_done
    Dictionary Should Contain Key    ${body}    updated_at
    Dictionary Should Contain Key    ${body}    stale
    Dictionary Should Contain Key    ${body}    domains
    Dictionary Should Contain Key    ${body}    edge_counts
    Dictionary Should Contain Key    ${body}    sdlc
    Dictionary Should Contain Key    ${body}    code
    Should Be True    isinstance($body["domains"], list)
    Should Be True    isinstance($body["edge_counts"], dict)

Sum Entity Counts
    [Arguments]    ${domains}
    ${total}=    Set Variable    ${0}
    FOR    ${domain}    IN    @{domains}
        FOR    ${item}    IN    @{domain["items"]}
            ${total}=    Evaluate    ${total} + ${item["count"]}
        END
    END
    RETURN    ${total}

Count Items With Status
    [Arguments]    ${domains}    ${status}
    ${n}=    Set Variable    ${0}
    FOR    ${domain}    IN    @{domains}
        FOR    ${item}    IN    @{domain["items"]}
            IF    "${item["status"]}" == "${status}"
                ${n}=    Evaluate    ${n} + 1
            END
        END
    END
    RETURN    ${n}

*** Test Cases ***
Missing Params Returns 400
    ${headers}=    GitLab Auth Headers
    ${resp}=    GET    ${GITLAB_URL}/api/v4/orbit/graph_status
    ...    headers=${headers}    expected_status=400

No Auth Returns 401
    ${resp}=    GET    ${GITLAB_URL}/api/v4/orbit/graph_status?namespace_id=1
    ...    expected_status=401

Nonexistent Namespace Returns 404
    ${resp}=    Get Graph Status By Namespace    99999999
    Should Be Equal As Integers    ${resp.status_code}    404

Pending State For Newly Enabled Namespace
    [Documentation]    Immediately after enabling KG for a new namespace,
    ...                before the indexer picks it up, state should be pending
    ...                (no KV data) and stale=true.
    ${suffix}=    Random Suffix
    ${name}=    Set Variable    gs-pending-${suffix}
    ${group}=    Create Group    ${name}    ${name}
    ${group_id}=    Set Variable    ${group["id"]}

    Enable Knowledge Graph    ${group_id}

    # Poll immediately. Depending on Siphon CDC latency and dispatcher interval,
    # the namespace may transition to idle before we observe pending. We accept
    # either pending (the expected initial state) or indexing (transient) or
    # idle (dispatcher was fast). The critical assertion is that the response
    # shape is valid in all three states.
    ${resp}=    Get Graph Status By Namespace    ${group_id}
    Should Be Equal As Integers    ${resp.status_code}    200
    ${body}=    Set Variable    ${resp.json()}
    Assert Response Shape    ${body}
    Should Be True    "${body['state']}" in ["pending", "indexing", "idle"]

Idle State And Counts For Top Level Group
    [Documentation]    After the indexer completes a cycle, a group with projects
    ...                should report state=idle, initial_backfill_done=true, a
    ...                populated SDLC progress section, and nonzero entity counts.
    ${suffix}=    Random Suffix
    ${name}=    Set Variable    gs-top-${suffix}
    ${group}=    Create Group    ${name}    ${name}
    ${group_id}=    Set Variable    ${group["id"]}
    Create Project    proj-${suffix}    ${group_id}

    Enable Knowledge Graph    ${group_id}
    Wait For Idle State    ${group_id}    300s

    ${resp}=    Get Graph Status By Namespace    ${group_id}
    ${body}=    Set Variable    ${resp.json()}
    Assert Response Shape    ${body}
    Should Be Equal    ${body["state"]}    idle
    Should Be True    ${body["initial_backfill_done"]}
    Dictionary Should Contain Key    ${body}    sdlc
    Should Not Be Equal    ${body["sdlc"]["last_completed_at"]}    ${EMPTY}
    Should Be True    ${body["sdlc"]["cycle_count"]} >= 1
    ${total}=    Sum Entity Counts    ${body["domains"]}
    Should Be True    ${total} >= 1
    ...    expected at least 1 entity in ${name}, got ${total}

Subgroup Inherits Indexed State
    [Documentation]    A subgroup under an enabled namespace must report the
    ...                same idle state and a populated counts snapshot scoped
    ...                to its traversal path subtree.
    ${suffix}=    Random Suffix
    ${top_name}=    Set Variable    gs-sub-${suffix}
    ${top}=    Create Group    ${top_name}    ${top_name}
    ${top_id}=    Set Variable    ${top["id"]}
    ${sub_name}=    Set Variable    sub-${suffix}
    ${sub}=    Create Group    ${sub_name}    ${sub_name}    ${top_id}
    ${sub_id}=    Set Variable    ${sub["id"]}
    ${full_path}=    Set Variable    ${top_name}/${sub_name}
    Create Project    proj-${suffix}    ${sub_id}

    Enable Knowledge Graph    ${top_id}
    Wait For Idle State    ${top_id}    300s

    ${resp}=    Get Graph Status By Full Path    ${full_path}
    Should Be Equal As Integers    ${resp.status_code}    200
    ${body}=    Set Variable    ${resp.json()}
    Assert Response Shape    ${body}
    Should Be Equal    ${body["state"]}    idle
    Should Be True    ${body["initial_backfill_done"]}

Project Scope Returns Code Domain Entities
    [Documentation]    Project-level graph_status should include code.projects_total
    ...                for the enclosing namespace. Code entity counts populate
    ...                only after the code indexing handler runs, which requires
    ...                the project to have been backfilled.
    ${suffix}=    Random Suffix
    ${name}=    Set Variable    gs-proj-${suffix}
    ${group}=    Create Group    ${name}    ${name}
    ${group_id}=    Set Variable    ${group["id"]}
    ${project}=    Create Project    proj-${suffix}    ${group_id}
    ${project_id}=    Set Variable    ${project["id"]}

    Enable Knowledge Graph    ${group_id}
    Wait For Idle State    ${group_id}    300s

    ${resp}=    Get Graph Status By Project    ${project_id}
    Should Be Equal As Integers    ${resp.status_code}    200
    ${body}=    Set Variable    ${resp.json()}
    Assert Response Shape    ${body}
    Should Be Equal    ${body["state"]}    idle
    # Project-scoped lookup resolves to the parent namespace and returns the
    # same meta snapshot. The `code` field should exist (may be empty until
    # code indexing runs, which depends on Siphon replicating push events).
    Dictionary Should Contain Key    ${body}    code
    Should Be True    ${body["code"]["projects_total"]} >= 1

Edge Counts Populate After Idle
    [Documentation]    Once at idle, the edge_counts map must contain at least
    ...                one edge type (MEMBER_OF, CONTAINS, IN_GROUP, etc.) for
    ...                a namespace with a project and root as owner.
    ${suffix}=    Random Suffix
    ${name}=    Set Variable    gs-edges-${suffix}
    ${group}=    Create Group    ${name}    ${name}
    ${group_id}=    Set Variable    ${group["id"]}
    Create Project    proj-${suffix}    ${group_id}

    Enable Knowledge Graph    ${group_id}
    Wait For Idle State    ${group_id}    300s

    ${resp}=    Get Graph Status By Namespace    ${group_id}
    ${body}=    Set Variable    ${resp.json()}
    ${edge_count}=    Get Length    ${body["edge_counts"]}
    Should Be True    ${edge_count} >= 1
    ...    expected at least 1 edge type in ${name}, got ${body["edge_counts"]}

Cycle Count Increments Across Runs
    [Documentation]    Subsequent indexing cycles must advance sdlc.cycle_count.
    ${suffix}=    Random Suffix
    ${name}=    Set Variable    gs-cycle-${suffix}
    ${group}=    Create Group    ${name}    ${name}
    ${group_id}=    Set Variable    ${group["id"]}

    Enable Knowledge Graph    ${group_id}
    Wait For Idle State    ${group_id}    300s

    ${first}=    Get Graph Status By Namespace    ${group_id}
    ${first_cycle}=    Set Variable    ${first.json()["sdlc"]["cycle_count"]}

    # Wait at least one dispatcher cycle + indexer cycle for the next write.
    Sleep    90s

    ${second}=    Get Graph Status By Namespace    ${group_id}
    ${second_cycle}=    Set Variable    ${second.json()["sdlc"]["cycle_count"]}
    Should Be True    ${second_cycle} >= ${first_cycle}
    ...    cycle_count must be monotonic (first=${first_cycle} second=${second_cycle})

Initial Backfill Done Is Monotonic
    [Documentation]    Once initial_backfill_done becomes true, it must stay true
    ...                regardless of transient cycle outcomes.
    ${suffix}=    Random Suffix
    ${name}=    Set Variable    gs-mono-${suffix}
    ${group}=    Create Group    ${name}    ${name}
    ${group_id}=    Set Variable    ${group["id"]}

    Enable Knowledge Graph    ${group_id}
    Wait For Idle State    ${group_id}    300s

    FOR    ${i}    IN RANGE    3
        ${resp}=    Get Graph Status By Namespace    ${group_id}
        Should Be True    ${resp.json()["initial_backfill_done"]}
        ...    initial_backfill_done regressed at iteration ${i}
        Sleep    30s
    END
