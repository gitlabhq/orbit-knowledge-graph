*** Settings ***
Documentation       Exercise the query_type variants beyond the traversal / aggregation shapes
...                 already used by 02-05: neighbors and path_finding. Seeds one project + issue
...                 (IN_PROJECT) under the shared namespace and reuses them for both shapes.
...
...                 The llm (GOON) response format is intentionally not asserted here: the pinned
...                 e2e GitLab + Workhorse relay returns an empty body for the llm format even when
...                 the same query returns rows in raw, so it cannot be meaningfully checked against
...                 this pinned stack. GOON encoding is covered by the formatters unit tests.

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
