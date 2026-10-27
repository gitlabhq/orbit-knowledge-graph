*** Settings ***
Documentation       Smoke the read-only Orbit API surface beyond /query (covered by 01):
...                 CALL db.schema(), tools, and agent commands. Each is a thin gRPC pass-through;
...                 this guards the Rails wiring.
...
...                 Each endpoint is asserted present-and-correct (200 + content) OR absent (404),
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


*** Test Cases ***
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
