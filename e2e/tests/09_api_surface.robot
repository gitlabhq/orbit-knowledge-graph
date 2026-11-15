*** Settings ***
Documentation       Smoke the read-only Orbit API surface beyond /query (covered by 01): the
...                 schema and the scoped status over GraphQL, and CALL db.schema(). MCP needs an
...                 OAuth token with the mcp scope, which a PAT cannot carry.

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

Query Names Cannot Use The Internal Column Prefix
    [Tags]    api
    Orbit Query Is Rejected
    ...    MATCH (g:Group {id: ${SHARED_NAMESPACE_ID}}) RETURN count(g) AS _orbit_total
    ...    identifier "_orbit_total" conflicts with reserved prefix "_orbit_"
    Orbit Query Is Rejected
    ...    MATCH (_orbit_g:Group {id: ${SHARED_NAMESPACE_ID}}) RETURN _orbit_g
    ...    identifier "_orbit_g" conflicts with reserved prefix "_orbit_"


*** Keywords ***
Schema Node Kinds Are Served
    ${data}=    Orbit GraphQL Data    ${SCHEMA_QUERY}
    Should Not Be Equal    ${data["orbit"]}    ${None}    Orbit is not available to the bot user
    ${names}=    Evaluate    [n["name"] for n in $data["orbit"]["schema"]["nodes"]["nodes"]]
    List Should Contain Value    ${names}    Project
    List Should Contain Value    ${names}    Vulnerability

Group Status Is Served
    ${variables}=    Create Dictionary    fullPath=${SHARED_NAMESPACE_NAME}
    ${data}=    Orbit GraphQL Data    ${GROUP_STATUS_QUERY}    ${variables}
    ${orbit}=    Set Variable    ${data["group"]["orbit"]}
    Should Not Be Equal    ${orbit}    ${None}    Orbit is not available to the bot user
    Should Be True    $orbit["indexingStatus"]["domains"] and $orbit["itemCounts"]["domains"]    ${orbit}
