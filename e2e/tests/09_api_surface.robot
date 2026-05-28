*** Settings ***
Documentation       Smoke the read-only Orbit API surface beyond /query and /status (already covered
...                 by 01): schema, schema/dsl, schema/format, graph_status, tools, and agent
...                 commands. Each is a thin gRPC pass-through; this guards the Rails wiring and the
...                 enabled-namespace gate on graph_status.
...
...                 MCP (POST /orbit/mcp) is intentionally not covered: it requires an OAuth token
...                 carrying the mcp scope (PATs cannot grant mcp / mcp_orbit / ai_workflows — they
...                 are in unavailable_ai_features_scopes), so it needs an OAuth bootstrap. Tracked
...                 in #792. The underlying tools are exercised here via GET /orbit/tools and /schema.

Resource            gitlab.resource
Resource            orbit.resource


*** Test Cases ***
Schema Lists Domains And Nodes
    [Tags]    api
    ${resp}=    Orbit Get    schema
    Should Not Be Empty    ${resp["nodes"]}
    Response Contains    ${resp}    Project
    Response Contains    ${resp}    Vulnerability

Query DSL Is Served
    [Tags]    api
    ${resp}=    Orbit Get    schema/dsl
    Response Contains    ${resp}    query_type

Response Format Guidance Is Served
    [Tags]    api
    ${resp}=    Orbit Get    schema/format
    Should Not Be Empty    ${resp}

Graph Status Reports Namespace Coverage
    [Tags]    api
    ${params}=    Create Dictionary    namespace_id=${SHARED_NAMESPACE_ID}
    ${resp}=    Orbit Get    graph_status    ${params}
    Response Contains    ${resp}    domains

Tools Endpoint Lists Orbit Tools
    [Tags]    api
    ${resp}=    Orbit Get    tools
    Should Not Be Empty    ${resp}
    Response Contains    ${resp}    get_graph_schema

Agent Commands Are Listed
    [Tags]    api
    ${resp}=    Orbit Get    agent/commands
    Should Not Be Empty    ${resp}


*** Keywords ***
Response Contains
    [Documentation]    Assert ${needle} appears anywhere in the JSON response (robust to the exact
    ...                Rails presenter wrapping).
    [Arguments]    ${resp}    ${needle}
    ${json}=    Evaluate    json.dumps($resp)    modules=json
    Should Contain    ${json}    ${needle}
