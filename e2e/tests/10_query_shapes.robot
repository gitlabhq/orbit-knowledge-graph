*** Settings ***
Documentation       Exercise the query_type variants and response formats beyond the traversal /
...                 aggregation shapes already used by 02-05: neighbors, path_finding, and the llm
...                 (GOON) response format. Seeds one project + issue (IN_PROJECT) under the shared
...                 namespace and reuses them for all three shapes.

Resource            gitlab.resource
Resource            orbit.resource

Suite Setup         Seed Query Shape Fixture


*** Test Cases ***
Neighbors Query Returns Adjacent Nodes
    [Tags]    query-shapes
    ${query}=    Evaluate
    ...    {"query_type": "neighbors", "node": {"id": "p", "entity": "Project", "node_ids": [int($SHAPE_PROJECT_ID)]}, "neighbors": {"node": "p", "direction": "both"}}
    ${resp}=    Orbit Query    ${query}
    Should Be True    ${resp["row_count"]} >= 1    neighbors query returned no rows

Path Finding Connects Issue To Project
    [Tags]    query-shapes
    ${query}=    Evaluate
    ...    {"query_type": "path_finding", "nodes": [{"id": "w", "entity": "WorkItem", "node_ids": [int($SHAPE_ISSUE_ID)]}, {"id": "p", "entity": "Project", "node_ids": [int($SHAPE_PROJECT_ID)]}], "path": {"type": "shortest", "from": "w", "to": "p", "max_depth": 2, "rel_types": ["IN_PROJECT"]}}
    ${resp}=    Orbit Query    ${query}
    Should Be True    ${resp["row_count"]} >= 1    path_finding query found no path

LLM Format Returns A GOON Body
    [Tags]    query-shapes
    ${query}=    Evaluate
    ...    {"query_type": "traversal", "node": {"id": "p", "entity": "Project", "node_ids": [int($SHAPE_PROJECT_ID)]}}
    ${resp}=    Orbit Query LLM    ${query}
    Should Not Be Empty    ${resp.text}    llm response body was empty


*** Keywords ***
Seed Query Shape Fixture
    ${suffix}=    Random Suffix
    Start Indexing Budget    180
    ${project}=    Create Project    e2e-shape-prj-${suffix}    ${SHARED_NAMESPACE_ID}
    ${issue}=    Create Issue    ${project["id"]}    e2e-shape-issue-${suffix}
    Set Suite Variable    ${SHAPE_PROJECT_ID}    ${project["id"]}
    Set Suite Variable    ${SHAPE_ISSUE_ID}    ${issue["id"]}
    Wait For Node Indexed Within Budget    Project    ${SHAPE_PROJECT_ID}    e2e-shape-prj-${suffix}
    Wait For Edge Indexed Within Budget    WorkItem    ${SHAPE_ISSUE_ID}    IN_PROJECT
    ...    Project    ${SHAPE_PROJECT_ID}
