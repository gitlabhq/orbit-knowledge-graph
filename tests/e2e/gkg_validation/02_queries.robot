*** Settings ***
Documentation       Phase 2: Test all 5 Orbit query types against the indexed graph.
...                 Requires 01_setup_and_index to have run first (data must be indexed).

Resource            resources/common.robot

Suite Setup         Suite Init


*** Test Cases ***
Search Query Returns Groups
    [Documentation]    Simple entity search by type
    [Tags]    search    critical

    ${query}=    Evaluate    {"query_type": "search", "node": {"entity_type": "Group", "columns": ["name", "full_path"]}}
    ${resp}=    Orbit Query    ${query}
    Should Be Equal As Integers    ${resp.status_code}    200    msg=Search failed: ${resp.text}

    ${body}=    Set Variable    ${resp.json()}
    Should Be Equal    ${body['result']['query_type']}    search
    Should Be True    ${body['row_count']} > 0    msg=Expected groups in search results

    Log    Search returned ${body['row_count']} rows

Traversal Query Finds User-Group Membership
    [Documentation]    Multi-hop traversal: User -> MEMBER_OF -> Group
    [Tags]    traversal    critical

    ${query}=    Evaluate
    ...    {"query_type": "traversal", "nodes": [{"entity_type": "User", "columns": ["username"]}, {"entity_type": "Group", "columns": ["name", "full_path"]}], "relationships": [{"edge_type": "MEMBER_OF", "direction": "outgoing"}]}
    ${resp}=    Orbit Query    ${query}
    Should Be Equal As Integers    ${resp.status_code}    200    msg=Traversal failed: ${resp.text}

    ${body}=    Set Variable    ${resp.json()}
    Should Be Equal    ${body['result']['query_type']}    traversal
    Should Be True    ${body['row_count']} > 0    msg=Expected traversal results
    Should Not Be Empty    ${body['result']['nodes']}    msg=Expected nodes in result
    Should Not Be Empty    ${body['result']['edges']}    msg=Expected edges in result

    Log    Traversal returned ${body['row_count']} rows

Aggregation Query Counts Projects Per Group
    [Documentation]    Aggregate: count of Projects contained in each Group
    [Tags]    aggregation    critical

    ${query}=    Evaluate
    ...    {"query_type": "aggregation", "nodes": [{"entity_type": "Group", "columns": ["name"]}, {"entity_type": "Project", "columns": []}], "relationships": [{"edge_type": "CONTAINS", "direction": "outgoing"}], "aggregations": [{"function": "count", "alias": "project_count"}]}
    ${resp}=    Orbit Query    ${query}
    Should Be Equal As Integers    ${resp.status_code}    200    msg=Aggregation failed: ${resp.text}

    ${body}=    Set Variable    ${resp.json()}
    Should Be Equal    ${body['result']['query_type']}    aggregation
    Should Be True    ${body['row_count']} > 0    msg=Expected aggregation results

    Log    Aggregation returned ${body['row_count']} rows

Neighbors Query Finds Group Connections
    [Documentation]    Discover outgoing neighbors of a specific group node
    [Tags]    neighbors    critical
    [Setup]    Variable Should Exist    ${TOP_GROUP_ID}

    ${query}=    Evaluate
    ...    {"query_type": "neighbors", "node": {"entity_type": "Group", "node_ids": [${TOP_GROUP_ID}]}, "neighbors": {"direction": "outgoing"}}
    ${resp}=    Orbit Query    ${query}
    Should Be Equal As Integers    ${resp.status_code}    200    msg=Neighbors failed: ${resp.text}

    ${body}=    Set Variable    ${resp.json()}
    Should Be Equal    ${body['result']['query_type']}    neighbors
    Should Be True    ${body['row_count']} > 0    msg=Expected neighbors for group ${TOP_GROUP_ID}

    Log    Neighbors returned ${body['row_count']} rows

Path Finding Query Finds User-to-Group Path
    [Documentation]    Shortest path: User -> Group via MEMBER_OF with max depth
    [Tags]    path_finding    critical

    ${query}=    Evaluate
    ...    {"query_type": "path_finding", "nodes": [{"entity_type": "User", "columns": ["username"]}, {"entity_type": "Group", "columns": ["name"]}], "relationships": [{"edge_type": "MEMBER_OF", "direction": "outgoing"}], "path": {"type": "shortest", "max_depth": 3}}
    ${resp}=    Orbit Query    ${query}
    Should Be Equal As Integers    ${resp.status_code}    200    msg=Path finding failed: ${resp.text}

    ${body}=    Set Variable    ${resp.json()}
    Should Be Equal    ${body['result']['query_type']}    path_finding
    Should Be True    ${body['row_count']} > 0    msg=Expected path results

    Log    Path finding returned ${body['row_count']} rows


*** Keywords ***
Suite Init
    Create API Session
    # Re-load variables set by 01_setup_and_index via a ClickHouse lookup.
    # In CI the suites share state via a variable file; locally the variables
    # may already be set from running both suites in sequence.
    ${has_tp}=    Run Keyword And Return Status    Variable Should Exist    ${TOP_GROUP_ID}
    IF    not ${has_tp}
        # Find the most recent e2e test group
        ${result}=    Run Process    bash    -c
        ...    curl -sk "${GDK_URL}/api/v4/groups?search=gkg-e2e&private_token=$(cat %{HOME}/.gdk_token)" 2>/dev/null | python3 -c "import sys,json; groups=json.load(sys.stdin); print(groups[0]['id'] if groups else '')"
        ...    timeout=15
        ${group_id}=    Strip String    ${result.stdout}
        Should Not Be Empty    ${group_id}    msg=No gkg-e2e group found. Run 01_setup_and_index first.
        Set Suite Variable    ${TOP_GROUP_ID}    ${group_id}
        Set Suite Variable    ${ORG_ID}    1
        Set Suite Variable    ${TRAVERSAL_PATH}    1/${group_id}/
    END
