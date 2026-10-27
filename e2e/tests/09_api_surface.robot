*** Settings ***
Documentation       Smoke the read-only Orbit API surface beyond /query (covered by 01):
...                 the schema and the scoped status over GraphQL, CALL db.schema(), tools, and
...                 agent commands. Each is a thin gRPC pass-through; this guards the Rails wiring.
...
...                 The GraphQL fields replaced the schema and graph_status REST routes. On a pinned
...                 GitLab image that predates a field, the test asserts the REST route it replaced
...                 still answers, which is the contract the orbit CLI fallback relies on.
...
...                 Each REST endpoint is asserted present (200) OR absent (404),
...                 because the e2e stack pins a gitlab-org/gitlab image that can predate the newer
...                 Orbit routes (which are owned upstream). A 5xx still fails the test, so a real
...                 GKG-side regression is caught.
...
...                 MCP (POST /orbit/mcp) is intentionally not covered: it requires an OAuth token
...                 carrying the mcp scope (PATs cannot grant mcp / mcp_orbit / ai_workflows — they
...                 are in unavailable_ai_features_scopes), so it needs an OAuth bootstrap. Tracked
...                 in #792. The tools listing is smoked here via GET /orbit/tools.

Resource            gitlab.resource
Resource            orbit.resource

Suite Setup         Attach To Shared Fixture


*** Variables ***
${SCHEMA_QUERY}             { orbit { schema { nodes(first: 100) { nodes { name } } } } }
${GROUP_STATUS_QUERY}       query($fullPath: ID!) { group(fullPath: $fullPath) { orbit {
...                         indexingStatus { domains { name } } itemCounts { domains { name } } } } }


*** Test Cases ***
Schema Lists Node Kinds
    [Tags]    api
    Wait Until Keyword Succeeds    30s    5s    Schema Node Kinds Are Served

Group Status Reports Indexing And Item Counts
    [Tags]    api
    Wait Until Keyword Succeeds    30s    5s    Group Status Is Served

Schema Call Describes A Node
    [Tags]    api
    ${resp}=    Orbit Query    CALL db.schema('Vulnerability')
    Should Be Equal    ${resp["result"]["domains"][0]["nodes"][0]["name"]}    Vulnerability

Tools Endpoint Responds
    [Tags]    api
    Orbit Endpoint Smoke    tools

Agent Commands Endpoint Responds
    [Tags]    api
    Orbit Endpoint Smoke    agent/commands

Query Names Cannot Use The Internal Column Prefix
    [Tags]    api
    Orbit Query Is Rejected
    ...    MATCH (g:Group {id: ${SHARED_NAMESPACE_ID}}) RETURN count(g) AS _orbit_total
    ...    output column "_orbit_total" conflicts with reserved prefix "_orbit_"
    Orbit Query Is Rejected
    ...    MATCH (_orbit_g:Group {id: ${SHARED_NAMESPACE_ID}}) RETURN _orbit_g
    ...    node id "_orbit_g" conflicts with reserved prefix "_orbit_"


*** Keywords ***
Schema Node Kinds Are Served
    ${data}=    Orbit GraphQL Data    ${SCHEMA_QUERY}
    IF    $data is None
        Orbit Legacy Route Serves    schema    needle=Project
    ELSE
        Should Not Be Equal    ${data["orbit"]}    ${None}    Orbit is not available to the bot user
        ${names}=    Evaluate    [n["name"] for n in $data["orbit"]["schema"]["nodes"]["nodes"]]
        List Should Contain Value    ${names}    Project
        List Should Contain Value    ${names}    Vulnerability
    END

Group Status Is Served
    ${variables}=    Create Dictionary    fullPath=${SHARED_NAMESPACE_NAME}
    ${data}=    Orbit GraphQL Data    ${GROUP_STATUS_QUERY}    ${variables}
    IF    $data is None
        ${params}=    Create Dictionary    namespace_id=${SHARED_NAMESPACE_ID}
        Orbit Legacy Route Serves    graph_status    ${params}    needle=domains
    ELSE
        ${orbit}=    Set Variable    ${data["group"]["orbit"]}
        Should Not Be Equal    ${orbit}    ${None}    Orbit is not available to the bot user
        Should Be True    $orbit["indexingStatus"]["domains"] and $orbit["itemCounts"]["domains"]    ${orbit}
    END
