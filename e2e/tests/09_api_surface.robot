*** Settings ***
Documentation       Smoke the read-only Orbit API surface beyond /query and /status (covered by 01):
...                 schema, schema/dsl, schema/format, graph_status, tools, and agent commands. Each
...                 is a thin gRPC pass-through; this guards the Rails wiring and the enabled-namespace
...                 gate on graph_status.
...
...                 Each endpoint is asserted present-and-correct (200 + content) OR absent (404),
...                 because the e2e stack pins a gitlab-org/gitlab image that can predate the newer
...                 Orbit routes (which are owned upstream). A 5xx still fails the test, so a real
...                 GKG-side regression is caught. /schema is always present, so its content check
...                 always runs.
...
...                 MCP (POST /orbit/mcp) is intentionally not covered: it requires an OAuth token
...                 carrying the mcp scope (PATs cannot grant mcp / mcp_orbit / ai_workflows — they
...                 are in unavailable_ai_features_scopes), so it needs an OAuth bootstrap. Tracked
...                 in #792. The tools listing is smoked here via GET /orbit/tools and /schema.

Resource            gitlab.resource
Resource            orbit.resource

Suite Setup         Attach To Shared Fixture


*** Test Cases ***
Schema Lists Domains And Nodes
    [Tags]    api
    Orbit Endpoint Smoke    schema    Project
    Orbit Endpoint Smoke    schema    Vulnerability

Query DSL Is Served
    [Tags]    api
    Orbit Endpoint Smoke    schema/dsl    query_type

Response Format Guidance Is Served
    [Tags]    api
    Orbit Endpoint Smoke    schema/format

Graph Status Reports Namespace Coverage
    [Tags]    api
    ${params}=    Create Dictionary    namespace_id=${SHARED_NAMESPACE_ID}
    Orbit Endpoint Smoke    graph_status    domains    ${params}

Tools Endpoint Responds
    [Tags]    api
    Orbit Endpoint Smoke    tools

Agent Commands Endpoint Responds
    [Tags]    api
    Orbit Endpoint Smoke    agent/commands
