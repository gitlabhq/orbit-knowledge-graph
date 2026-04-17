*** Settings ***
Documentation       Orbit query helpers — health probe and graph node lookups.
...                 Imported by every test suite that talks to the Orbit API.

Library             RequestsLibrary
Library             Collections
Library             DateTime
Resource            gitlab.resource


*** Keywords ***
Orbit Status Is Healthy
    ${headers}=    GitLab Auth Headers
    ${resp}=    GET    ${GITLAB_URL}/api/v4/orbit/status    headers=${headers}    expected_status=200
    Should Be Equal    ${resp.json()["status"]}    healthy

Orbit Query Node
    [Documentation]    Search for a single node by entity type and primary id.
    [Arguments]    ${entity}    ${node_id}
    ${headers}=    GitLab Auth Headers
    ${node}=    Create Dictionary    id=n    entity=${entity}    node_ids=${{[${node_id}]}}
    ${query}=    Create Dictionary    query_type=search    node=${node}
    ${body}=    Create Dictionary    query=${query}
    ${resp}=    POST    ${GITLAB_URL}/api/v4/orbit/query
    ...    headers=${headers}    json=${body}    expected_status=200
    RETURN    ${resp.json()}

Orbit Count Nodes
    [Documentation]    Count all nodes of a given entity type. Used for smoke checks.
    [Arguments]    ${entity}
    ${headers}=    GitLab Auth Headers
    ${agg}=    Create Dictionary    function=count    target=n
    ${node}=    Create Dictionary    id=n    entity=${entity}
    ${query}=    Create Dictionary
    ...    query_type=aggregation    nodes=${{[${node}]}}    aggregations=${{[${agg}]}}
    ${body}=    Create Dictionary    query=${query}
    ${resp}=    POST    ${GITLAB_URL}/api/v4/orbit/query
    ...    headers=${headers}    json=${body}    expected_status=200
    RETURN    ${resp.json()["result"]["columns"][0]["value"]}

Verify Node Indexed
    [Documentation]    Assert that one node of ${entity} with id=${node_id} is queryable.
    ...                If ${expected_label} is given, also assert it matches the node's label field.
    [Arguments]    ${entity}    ${node_id}    ${expected_label}=${None}    ${label_field}=name
    ${result}=    Orbit Query Node    ${entity}    ${node_id}
    Should Be True    ${result["row_count"]} >= 1
    ...    ${entity} id=${node_id} not found via Orbit query
    IF    $expected_label is not None
        Should Be Equal
        ...    ${result["result"]["nodes"][0]["${label_field}"]}    ${expected_label}
    END

Wait For Node Indexed
    [Documentation]    Poll Verify Node Indexed until success or ${timeout} elapses.
    [Arguments]    ${entity}    ${node_id}    ${expected_label}=${None}
    ...    ${label_field}=name    ${timeout}=120s    ${interval}=3s
    Wait Until Keyword Succeeds    ${timeout}    ${interval}
    ...    Verify Node Indexed    ${entity}    ${node_id}    ${expected_label}    ${label_field}

Wait For Entity Population
    [Documentation]    Wait until at least one node of ${entity} is indexed.
    [Arguments]    ${entity}    ${minimum}=1    ${timeout}=120s    ${interval}=3s
    Wait Until Keyword Succeeds    ${timeout}    ${interval}
    ...    Entity Count Is At Least    ${entity}    ${minimum}

Entity Count Is At Least
    [Arguments]    ${entity}    ${minimum}
    ${count}=    Orbit Count Nodes    ${entity}
    Should Be True    ${count} >= ${minimum}
    ...    Only ${count} ${entity} nodes indexed (need >= ${minimum})

Start Indexing Budget
    [Documentation]    Begin a shared time budget consumed by Wait For * Within Budget calls.
    ...                Use one budget per logical "wait for the pipeline to catch up" sequence.
    [Arguments]    ${seconds}=300
    ${now}=    Get Current Date    result_format=epoch
    Set Global Variable    ${INDEXING_DEADLINE}    ${{${now} + ${seconds}}}

Remaining Budget
    [Documentation]    Seconds left until INDEXING_DEADLINE; fails fast if exhausted.
    ${now}=    Get Current Date    result_format=epoch
    ${remaining}=    Evaluate    int(${INDEXING_DEADLINE} - ${now})
    Should Be True    ${remaining} > 0    Indexing budget exhausted
    RETURN    ${remaining}s

Wait For Node Indexed Within Budget
    [Documentation]    Same as Wait For Node Indexed but pulls timeout from the shared budget.
    [Arguments]    ${entity}    ${node_id}    ${expected_label}=${None}    ${label_field}=name
    ${budget}=    Remaining Budget
    Wait Until Keyword Succeeds    ${budget}    3s
    ...    Verify Node Indexed    ${entity}    ${node_id}    ${expected_label}    ${label_field}
