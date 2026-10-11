*** Settings ***
Documentation       type(r) on a relationship variable, end to end through Rails. Seeds a public
...                 project with a merge request that closes an issue in a private project under
...                 another top-level group, then checks that a type(r) filter returns the same
...                 edge as the typed pattern, that the gql format adds a type column in RETURN
...                 order sorted by type, that raw and llm list the edge type, that a
...                 variable-length type(r) rejects, and that a user without access to the private
...                 project does not see the edge through type(r).

Resource            gitlab.resource
Resource            orbit.resource

Suite Setup         Run Keywords    Attach To Shared Fixture    AND    Seed Relationship Type Fixture


*** Test Cases ***
Type Filter Returns The Same Edge As The Typed Pattern
    [Tags]    type-r
    Within Budget    Type Filter Matches Typed Pattern

Type In List Returns The Closing Edge
    [Tags]    type-r
    Within Budget    Type In List Returns Closing Edge

Gql Format Adds A Sorted Type Column In Return Order
    [Tags]    type-r
    Within Budget    Gql Type Column Follows Return Order

Raw And LLM Formats List The Edge Type
    [Tags]    type-r
    Within Budget    Raw And LLM List Edge Type

Variable Length Type Rejects With The One Hop Message
    [Tags]    type-r
    ${message}=    Orbit Query Rejected
    ...    MATCH (mr:MergeRequest {id: ${RT_MR_ID}})-[r:CLOSES*1..2]->(w:WorkItem) RETURN mr, type(r)
    Should Contain    ${message}    r has a variable length and binds a list

Type Filter Does Not Reveal An Edge To A User Without Access
    [Tags]    type-r    authz    redaction
    Within Budget    Type Filter Matches Typed Pattern
    ${raw}=    Orbit Query With Token    ${RT_FILTER_QUERY}    ${RT_VICTIM_PAT}
    ${ids}=    Evaluate    [n["id"] for n in $raw["result"].get("nodes", [])]
    Should Not Contain    ${ids}    ${{str($RT_ISSUE_ID)}}    victim saw the private issue
    ${edges}=    Edge Keys    ${raw}
    Should Not Contain    ${edges}    ${RT_EDGE_KEY}    victim saw the closing edge
    ${text}=    Orbit Query Text    ${RT_GQL_QUERY}    gql    ${RT_VICTIM_PAT}
    ${table}=    Gql Table    ${text}
    Length Should Be    ${table}    1    victim gql table has rows: ${text}


*** Keywords ***
Seed Relationship Type Fixture
    ${suffix}=    Random Suffix
    Start Indexing Budget    400
    ${public_group}=    Create Group    e2e-rt-pub-${suffix}
    ${private_group}=    Create Group    e2e-rt-priv-${suffix}
    Enable Orbit    ${public_group["id"]}
    Enable Orbit    ${private_group["id"]}
    ${public_project}=    Create Project    e2e-rt-prj-pub-${suffix}    ${public_group["id"]}
    ${private_project}=    Create Project    e2e-rt-prj-priv-${suffix}    ${private_group["id"]}
    ...    visibility=private
    ${issue}=    Create Issue    ${private_project["id"]}    e2e-rt-issue-${suffix}
    ${mr}=    Open Closing Merge Request    ${public_project["id"]}
    ...    ${private_project["path_with_namespace"]}    ${issue["iid"]}
    ${victim}=    Create User    e2e-rt-${suffix}-victim
    ${root_headers}=    Root Auth Headers
    ${victim_pat}=    Issue PAT For User    ${root_headers}    ${victim["id"]}
    Add Group Member    ${public_group["id"]}    ${victim["id"]}    20
    Set Suite Variable    ${RT_MR_ID}    ${mr["id"]}
    Set Suite Variable    ${RT_ISSUE_ID}    ${issue["id"]}
    Set Suite Variable    ${RT_VICTIM_PAT}    ${victim_pat}
    Set Suite Variable    ${RT_EDGE_KEY}    MergeRequest:${mr["id"]}-CLOSES->WorkItem:${issue["id"]}
    Set Suite Variable    ${RT_TYPED_QUERY}
    ...    MATCH (mr:MergeRequest {id: ${mr["id"]}})-[r:CLOSES]->(w:WorkItem) RETURN mr, w LIMIT 100
    Set Suite Variable    ${RT_FILTER_QUERY}
    ...    MATCH (mr:MergeRequest {id: ${mr["id"]}})-[r]->(w:WorkItem) WHERE type(r) = 'CLOSES' RETURN mr, type(r), w LIMIT 100
    Set Suite Variable    ${RT_GQL_QUERY}
    ...    MATCH (mr:MergeRequest {id: ${mr["id"]}})-[r]->(w:WorkItem) RETURN mr.iid, type(r) AS kind, w.title ORDER BY type(r) LIMIT 100
    Wait For Edge Indexed Within Budget    MergeRequest    ${RT_MR_ID}    CLOSES
    ...    WorkItem    ${RT_ISSUE_ID}

Within Budget
    [Arguments]    ${keyword}
    ${budget}=    Remaining Budget
    Wait Until Keyword Succeeds    ${budget}    2s    ${keyword}

Edge Keys
    [Arguments]    ${resp}
    ${keys}=    Evaluate
    ...    sorted(f'{e["from"]}:{e["from_id"]}-{e["type"]}->{e["to"]}:{e["to_id"]}' for e in $resp["result"].get("edges", []))
    RETURN    ${keys}

Gql Table
    [Documentation]    Header and row cells of a gql format table, one list per line.
    [Arguments]    ${text}
    ${table}=    Evaluate
    ...    [[cell.strip() for cell in line.strip().strip("|").split(" | ")] for line in $text.splitlines() if line.startswith("|")]
    RETURN    ${table}

Type Filter Matches Typed Pattern
    ${typed}=    Orbit Query    ${RT_TYPED_QUERY}
    ${filtered}=    Orbit Query    ${RT_FILTER_QUERY}
    ${typed_edges}=    Edge Keys    ${typed}
    ${filtered_edges}=    Edge Keys    ${filtered}
    Should Contain    ${filtered_edges}    ${RT_EDGE_KEY}
    Lists Should Be Equal    ${filtered_edges}    ${typed_edges}

Type In List Returns Closing Edge
    ${resp}=    Orbit Query
    ...    MATCH (mr:MergeRequest {id: ${RT_MR_ID}})-[r]->(w:WorkItem) WHERE type(r) IN ['CLOSES', 'MENTIONS'] RETURN mr, w LIMIT 100
    ${edges}=    Edge Keys    ${resp}
    Should Contain    ${edges}    ${RT_EDGE_KEY}
    ${types}=    Evaluate    {e["type"] for e in $resp["result"]["edges"]}
    Should Be True    $types <= {"CLOSES", "MENTIONS"}    unexpected edge types ${types}

Gql Type Column Follows Return Order
    ${text}=    Orbit Query Text    ${RT_GQL_QUERY}    gql
    ${table}=    Gql Table    ${text}
    ${header}=    Create List    mr    kind    w
    Lists Should Be Equal    ${table}[0]    ${header}    gql header is not in RETURN order: ${text}
    ${kinds}=    Evaluate    [row[1] for row in $table[1:]]
    Should Contain    ${kinds}    "CLOSES"    gql kind column has no CLOSES row: ${text}
    Should Be True    $kinds == sorted($kinds)    gql rows are not sorted by type: ${kinds}

Raw And LLM List Edge Type
    ${raw}=    Orbit Query    ${RT_FILTER_QUERY}
    ${edges}=    Edge Keys    ${raw}
    Should Contain    ${edges}    ${RT_EDGE_KEY}
    ${llm}=    Orbit Query Text    ${RT_FILTER_QUERY}    llm
    Should Contain    ${llm}    edges[    llm body has no edges table: ${llm}
    Should Contain    ${llm}    MergeRequest,${RT_MR_ID},CLOSES,WorkItem,${RT_ISSUE_ID}
    ...    llm edges table has no closing edge: ${llm}
