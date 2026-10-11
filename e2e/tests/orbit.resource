*** Settings ***
Documentation       Orbit query helpers — health probe and graph node lookups.
...                 Imported by every test suite that talks to the Orbit API.

Library             RequestsLibrary
Library             Collections
Library             DateTime
Resource            gitlab.resource


*** Variables ***
${INDEXING_BUDGET_MULTIPLIER}       %{INDEXING_BUDGET_MULTIPLIER=1}


*** Keywords ***
Orbit Status Is Healthy
    ${headers}=    GitLab Auth Headers
    ${resp}=    GET    ${GITLAB_URL}/api/v4/orbit/status    headers=${headers}    expected_status=200
    Should Be Equal    ${resp.json()["system"]["status"]}    healthy

Orbit Query POST
    [Documentation]    POST /orbit/query expecting 200, retrying once on 502: the webserver
    ...                recycles gRPC connections at a fixed age, and a query in flight at the
    ...                boundary dies with a one-off 502 (#971).
    [Arguments]    ${headers}    ${body}
    ${resp}=    POST    ${GITLAB_URL}/api/v4/orbit/query
    ...    headers=${headers}    json=${body}    expected_status=any
    IF    ${resp.status_code} == 502
        Sleep    1s
        ${resp}=    POST    ${GITLAB_URL}/api/v4/orbit/query
        ...    headers=${headers}    json=${body}    expected_status=any
    END
    Should Be Equal As Integers    ${resp.status_code}    200
    ...    POST /orbit/query returned ${resp.status_code}: ${resp.text}
    RETURN    ${resp}

Orbit Query Node
    [Documentation]    Search for a single node by entity type and primary id.
    [Arguments]    ${entity}    ${node_id}
    ${resp}=    Orbit Query    MATCH (n:${entity} {id: ${node_id}}) RETURN n
    RETURN    ${resp}

Orbit Count Nodes
    [Documentation]    Count the nodes of ${entity} that match the GQL ${properties} map.
    [Arguments]    ${entity}    ${properties}
    ${resp}=    Orbit Query    MATCH (n:${entity} ${properties}) RETURN count(n) AS count
    RETURN    ${resp["result"]["rows"][0]["count"]}

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
    ...    ${label_field}=name    ${timeout}=120s    ${interval}=2s
    ${scaled_timeout}=    Scaled Timeout    ${timeout}
    Wait Until Keyword Succeeds    ${scaled_timeout}    ${interval}
    ...    Verify Node Indexed    ${entity}    ${node_id}    ${expected_label}    ${label_field}

Verify Node Removed
    [Documentation]    Assert no live node of ${entity} with id=${node_id} is returned (deleted rows
    ...                are tombstoned with _deleted and filtered out of query results).
    [Arguments]    ${entity}    ${node_id}
    ${result}=    Orbit Query Node    ${entity}    ${node_id}
    Should Be Equal As Integers    ${result["row_count"]}    0
    ...    ${entity} id=${node_id} still queryable after delete

Wait For Node Removed
    [Arguments]    ${entity}    ${node_id}    ${timeout}=180s    ${interval}=3s
    ${scaled_timeout}=    Scaled Timeout    ${timeout}
    Wait Until Keyword Succeeds    ${scaled_timeout}    ${interval}
    ...    Verify Node Removed    ${entity}    ${node_id}

Start Indexing Budget
    [Documentation]    Begin a shared time budget consumed by Wait For * Within Budget calls.
    ...                Use one budget per logical "wait for the pipeline to catch up" sequence.
    [Arguments]    ${seconds}=300
    ${now}=    Get Current Date    result_format=epoch
    ${scaled_seconds}=    Evaluate    int(${seconds} * float(${INDEXING_BUDGET_MULTIPLIER}))
    Set Global Variable    ${INDEXING_DEADLINE}    ${{${now} + ${scaled_seconds}}}

Scaled Timeout
    [Documentation]    Scale simple Robot timeouts like 120s for slower VM e2e runs.
    [Arguments]    ${timeout}
    RETURN    ${{str(int(float(str($timeout).rstrip("s")) * float($INDEXING_BUDGET_MULTIPLIER))) + "s"}}

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
    Wait Until Keyword Succeeds    ${budget}    2s
    ...    Verify Node Indexed    ${entity}    ${node_id}    ${expected_label}    ${label_field}

Wait For Edge Indexed Within Budget
    [Documentation]    Same as Wait For Edge Indexed but pulls timeout from the shared budget.
    [Arguments]    ${src_entity}    ${src_id}    ${rel_type}    ${dst_entity}    ${dst_id}
    ${budget}=    Remaining Budget
    Wait Until Keyword Succeeds    ${budget}    2s
    ...    Verify Edge Indexed    ${src_entity}    ${src_id}    ${rel_type}    ${dst_entity}    ${dst_id}

Orbit Query
    [Documentation]    Low-level POST /orbit/query. Accepts GQL query text and returns the parsed JSON response.
    [Arguments]    ${query}
    ${headers}=    GitLab Auth Headers
    ${body}=    Create Dictionary    query=${query}
    ${resp}=    Orbit Query POST    ${headers}    ${body}
    RETURN    ${resp.json()}

Orbit Query With Token
    [Documentation]    Low-level POST /orbit/query using an explicit GitLab PAT.
    [Arguments]    ${query}    ${token}
    ${headers}=    Create Dictionary    PRIVATE-TOKEN=${token}    Content-Type=application/json
    ${body}=    Create Dictionary    query=${query}
    ${resp}=    Orbit Query POST    ${headers}    ${body}
    RETURN    ${resp.json()}

Orbit Endpoint Smoke
    [Documentation]    GET /api/v4/orbit/${path} with the bot PAT. When the endpoint is present (200)
    ...                assert the body contains ${needle} (when given). A 404 is tolerated: the e2e
    ...                stack pins a gitlab-org/gitlab image (versions.yaml) that can predate newer
    ...                Orbit endpoints, and those routes are owned upstream, not by this repo. A 5xx
    ...                or any other status still fails, so a real GKG-side regression is caught.
    [Arguments]    ${path}    ${needle}=${None}    ${params}=${None}
    ${headers}=    GitLab Auth Headers
    ${resp}=    GET    ${GITLAB_URL}/api/v4/orbit/${path}
    ...    headers=${headers}    params=${params}    expected_status=any
    IF    ${resp.status_code} == 404
        Log    /orbit/${path} absent in the pinned GitLab image; content check skipped.    level=WARN
        RETURN
    END
    Should Be Equal As Integers    ${resp.status_code}    200
    ...    /orbit/${path} returned ${resp.status_code}: ${resp.text}
    IF    $needle is not None
        Should Contain    ${resp.text}    ${needle}    /orbit/${path} body missing '${needle}'
    END

Orbit Query LLM
    [Documentation]    POST /orbit/query with response_format=llm. Returns the raw response so the
    ...                caller can assert on the TOON body.
    [Arguments]    ${query}
    ${headers}=    GitLab Auth Headers
    ${body}=    Create Dictionary    query=${query}    response_format=llm
    ${resp}=    Orbit Query POST    ${headers}    ${body}
    RETURN    ${resp}

Orbit Query Text
    [Documentation]    POST /orbit/query with ${response_format} and return the response body text.
    ...                Sends ${token} when given, else the e2e-bot PAT.
    [Arguments]    ${query}    ${response_format}    ${token}=${None}
    IF    $token is None
        ${headers}=    GitLab Auth Headers
    ELSE
        ${headers}=    Create Dictionary    PRIVATE-TOKEN=${token}    Content-Type=application/json
    END
    ${body}=    Create Dictionary    query=${query}    response_format=${response_format}
    ${resp}=    Orbit Query POST    ${headers}    ${body}
    RETURN    ${resp.text}

Orbit Query Rejected
    [Documentation]    POST /orbit/query expecting a 400 compile error; returns the error message.
    [Arguments]    ${query}
    ${headers}=    GitLab Auth Headers
    ${body}=    Create Dictionary    query=${query}
    ${resp}=    POST    ${GITLAB_URL}/api/v4/orbit/query
    ...    headers=${headers}    json=${body}    expected_status=400
    RETURN    ${resp.json()["message"]}

Result Node Ids Contain
    [Documentation]    Assert ${node_id} is among the ids of the nodes in a graph-shaped query
    ...                response (neighbors / path_finding return result.nodes with string ids).
    [Arguments]    ${resp}    ${node_id}
    ${ids}=    Evaluate    [n["id"] for n in $resp["result"]["nodes"]]
    Should Contain    ${ids}    ${{str($node_id)}}    node ${node_id} not in result nodes ${ids}

Wait Until Result Node Ids Contain
    [Documentation]    Re-run a graph-shaped query until every ${node_id} appears among result.nodes,
    ...                within the shared indexing budget. neighbors / path_finding DISCOVER their
    ...                result nodes, so each one is authorized per-resource through Rails redaction;
    ...                a just-created node is only eventually consistent on that authz path (the seed
    ...                waits confirm graph indexing via an explicit-id traversal that needs no
    ...                redaction), so a single immediate query can race the authz refresh and omit it.
    ...                Polling mirrors the Wait For * Within Budget keywords the rest of the suite uses.
    [Arguments]    ${query}    @{node_ids}
    ${budget}=    Remaining Budget
    Wait Until Keyword Succeeds    ${budget}    2s
    ...    Result Must Contain Node Ids    ${query}    @{node_ids}

Result Must Contain Node Ids
    [Arguments]    ${query}    @{node_ids}
    ${resp}=    Orbit Query    ${query}
    FOR    ${node_id}    IN    @{node_ids}
        Result Node Ids Contain    ${resp}    ${node_id}
    END

Aggregation Value
    [Documentation]    Return the ${column} value from the first row of an aggregation response.
    [Arguments]    ${resp}    ${column}
    ${rows}=    Set Variable    ${resp["result"]["rows"]}
    Should Not Be Empty    ${rows}    aggregation returned no rows
    RETURN    ${rows[0]["${column}"]}

Wait Until Aggregation At Least
    [Documentation]    Re-run an aggregation ${query} until ${column} in the first row is >= ${minimum},
    ...                within the shared indexing budget. A discovered cross-namespace node is
    ...                eventually consistent on the per-resource authz path, so an immediate count can
    ...                race the redaction refresh and undercount (same reason as the node-id poll above).
    [Arguments]    ${query}    ${column}    ${minimum}=1
    ${budget}=    Remaining Budget
    Wait Until Keyword Succeeds    ${budget}    2s
    ...    Aggregation At Least    ${query}    ${column}    ${minimum}

Aggregation At Least
    [Arguments]    ${query}    ${column}    ${minimum}
    ${resp}=    Orbit Query    ${query}
    ${value}=    Aggregation Value    ${resp}    ${column}
    Should Be True    ${value} >= ${minimum}    ${column}=${value} (need >= ${minimum})

Orbit Edge Count
    [Documentation]    Row count of a one-hop ${rel_type} traversal. ${src} and ${dst} are GQL node
    ...                patterns without the variable, such as File {project_id: 1, path: 'a.rb'}.
    [Arguments]    ${src}    ${rel_type}    ${dst}    ${where}=${EMPTY}
    ${resp}=    Orbit Query    MATCH (src:${src})-[:${rel_type}]->(dst:${dst}) ${where} RETURN src, dst LIMIT 10
    RETURN    ${resp["row_count"]}

# --- Generic edge assertions -------------------------------------------------

Wait For Edge Indexed
    [Documentation]    Poll until an edge of ${rel_type} between ${src_entity} (filtered by
    ...                id=${src_id} when not None) and ${dst_entity} (filtered by id=${dst_id}
    ...                when not None) is queryable. Pass ${None} on either side to match any
    ...                node of that entity (useful for e.g. "some User AUTHORED this Note").
    [Arguments]    ${src_entity}    ${src_id}    ${rel_type}    ${dst_entity}    ${dst_id}
    ...            ${timeout}=120s    ${interval}=2s
    ${scaled_timeout}=    Scaled Timeout    ${timeout}
    Wait Until Keyword Succeeds    ${scaled_timeout}    ${interval}
    ...    Verify Edge Indexed    ${src_entity}    ${src_id}    ${rel_type}    ${dst_entity}    ${dst_id}

Verify Edge Indexed
    [Arguments]    ${src_entity}    ${src_id}    ${rel_type}    ${dst_entity}    ${dst_id}
    ${ids}=    Evaluate
    ...    " AND ".join(f"{name}.id = {value}" for name, value in (("src", $src_id), ("dst", $dst_id)) if value is not None)
    ${count}=    Orbit Edge Count    ${src_entity}    ${rel_type}    ${dst_entity}
    ...    ${{"WHERE " + $ids if $ids else ""}}
    Should Be True    ${count} >= 1
    ...    No ${rel_type} edge ${src_entity}(${src_id}) -> ${dst_entity}(${dst_id})

# --- Code-graph assertions (per-project) ------------------------------------

File Count For Project Is
    [Documentation]    Wait until File node count under ${project_id} equals ${expected}.
    [Arguments]    ${project_id}    ${expected}    ${timeout}=180s    ${interval}=3s
    ${scaled_timeout}=    Scaled Timeout    ${timeout}
    Wait Until Keyword Succeeds    ${scaled_timeout}    ${interval}
    ...    Assert Entity Count For Project    File    ${project_id}    ${expected}

Definition Count For Project Is At Least
    [Arguments]    ${project_id}    ${minimum}    ${timeout}=180s    ${interval}=3s
    ${scaled_timeout}=    Scaled Timeout    ${timeout}
    Wait Until Keyword Succeeds    ${scaled_timeout}    ${interval}
    ...    Assert Entity Count At Least For Project    Definition    ${project_id}    ${minimum}

Assert Entity Count For Project
    [Arguments]    ${entity}    ${project_id}    ${expected}
    ${count}=    Orbit Count Nodes    ${entity}    {project_id: ${project_id}}
    Should Be Equal As Integers    ${count}    ${expected}
    ...    ${entity} count for project ${project_id} = ${count} (expected ${expected})

Assert Entity Count At Least For Project
    [Arguments]    ${entity}    ${project_id}    ${minimum}
    ${count}=    Orbit Count Nodes    ${entity}    {project_id: ${project_id}}
    Should Be True    ${count} >= ${minimum}
    ...    ${entity} count for project ${project_id} = ${count} (need >= ${minimum})

Definition Exists In Project
    [Documentation]    Assert that a Definition with ${fqn} (and optional ${definition_type}) exists under ${project_id}.
    ...                Returns the matched node dict.
    [Arguments]    ${project_id}    ${fqn}    ${definition_type}=${None}    ${timeout}=180s    ${interval}=3s
    ${scaled_timeout}=    Scaled Timeout    ${timeout}
    ${node}=    Wait Until Keyword Succeeds    ${scaled_timeout}    ${interval}
    ...    Lookup Definition By Fqn    ${project_id}    ${fqn}    ${definition_type}
    RETURN    ${node}

Lookup Definition By Fqn
    [Arguments]    ${project_id}    ${fqn}    ${definition_type}=${None}
    ${type}=    Set Variable If    $definition_type is None    ${EMPTY}    , definition_type: '${definition_type}'
    ${resp}=    Orbit Query
    ...    MATCH (n:Definition {project_id: ${project_id}, fqn: '${fqn}'${type}}) RETURN n LIMIT 5
    Should Be True    ${resp["row_count"]} >= 1
    ...    Definition fqn=${fqn} type=${definition_type} not found in project ${project_id}
    RETURN    ${resp["result"]["nodes"][0]}

Defines Edge Exists Between Definitions
    [Documentation]    Assert a DEFINES edge from Definition fqn=${parent_fqn} to Definition fqn=${child_fqn}
    ...                under ${project_id}. Models structural containment (e.g. Class defines Method).
    [Arguments]    ${project_id}    ${parent_fqn}    ${child_fqn}    ${timeout}=180s    ${interval}=3s
    ${scaled_timeout}=    Scaled Timeout    ${timeout}
    Wait Until Keyword Succeeds    ${scaled_timeout}    ${interval}
    ...    Assert Defines Edge Between Definitions    ${project_id}    ${parent_fqn}    ${child_fqn}

Assert Defines Edge Between Definitions
    [Arguments]    ${project_id}    ${parent_fqn}    ${child_fqn}
    ${count}=    Orbit Edge Count    Definition {project_id: ${project_id}, fqn: '${parent_fqn}'}
    ...    DEFINES    Definition {project_id: ${project_id}, fqn: '${child_fqn}'}
    Should Be True    ${count} >= 1
    ...    No DEFINES edge from ${parent_fqn} to ${child_fqn} in project ${project_id}

Defines Edge Exists Between File And Definition
    [Documentation]    Assert a DEFINES edge from File path=${file_path} to Definition fqn=${def_fqn}
    ...                under ${project_id}. Models the file→top-level-definition relationship.
    [Arguments]    ${project_id}    ${file_path}    ${def_fqn}    ${timeout}=180s    ${interval}=3s
    ${scaled_timeout}=    Scaled Timeout    ${timeout}
    Wait Until Keyword Succeeds    ${scaled_timeout}    ${interval}
    ...    Assert Defines Edge Between File And Definition    ${project_id}    ${file_path}    ${def_fqn}

Assert Defines Edge Between File And Definition
    [Arguments]    ${project_id}    ${file_path}    ${def_fqn}
    ${count}=    Orbit Edge Count    File {project_id: ${project_id}, path: '${file_path}'}
    ...    DEFINES    Definition {project_id: ${project_id}, fqn: '${def_fqn}'}
    Should Be True    ${count} >= 1
    ...    No DEFINES edge from File ${file_path} to Definition ${def_fqn} in project ${project_id}

Imports Edge Exists Between File And Imported Symbol
    [Documentation]    Assert an IMPORTS edge from File path=${file_path} to ImportedSymbol matching
    ...                (import_path=${import_path}, identifier_name=${identifier_name}) under ${project_id}.
    [Arguments]    ${project_id}    ${file_path}    ${import_path}    ${identifier_name}
    ...    ${timeout}=180s    ${interval}=3s
    ${scaled_timeout}=    Scaled Timeout    ${timeout}
    Wait Until Keyword Succeeds    ${scaled_timeout}    ${interval}
    ...    Assert Imports Edge Between File And Imported Symbol
    ...    ${project_id}    ${file_path}    ${import_path}    ${identifier_name}

Assert Imports Edge Between File And Imported Symbol
    [Arguments]    ${project_id}    ${file_path}    ${import_path}    ${identifier_name}
    ${count}=    Orbit Edge Count    File {project_id: ${project_id}, path: '${file_path}'}
    ...    IMPORTS
    ...    ImportedSymbol {project_id: ${project_id}, import_path: '${import_path}', identifier_name: '${identifier_name}'}
    Should Be True    ${count} >= 1
    ...    No IMPORTS edge from File ${file_path} to ${import_path}.${identifier_name} in project ${project_id}
