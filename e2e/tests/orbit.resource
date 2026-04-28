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
    ${scaled_timeout}=    Scaled Timeout    ${timeout}
    Wait Until Keyword Succeeds    ${scaled_timeout}    ${interval}
    ...    Verify Node Indexed    ${entity}    ${node_id}    ${expected_label}    ${label_field}

Wait For Entity Population
    [Documentation]    Wait until at least one node of ${entity} is indexed.
    [Arguments]    ${entity}    ${minimum}=1    ${timeout}=120s    ${interval}=3s
    ${scaled_timeout}=    Scaled Timeout    ${timeout}
    Wait Until Keyword Succeeds    ${scaled_timeout}    ${interval}
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
    Wait Until Keyword Succeeds    ${budget}    3s
    ...    Verify Node Indexed    ${entity}    ${node_id}    ${expected_label}    ${label_field}

Wait For Edge Indexed Within Budget
    [Documentation]    Same as Wait For Edge Indexed but pulls timeout from the shared budget.
    [Arguments]    ${src_entity}    ${src_id}    ${rel_type}    ${dst_entity}    ${dst_id}
    ${budget}=    Remaining Budget
    Wait Until Keyword Succeeds    ${budget}    3s
    ...    Verify Edge Indexed    ${src_entity}    ${src_id}    ${rel_type}    ${dst_entity}    ${dst_id}

# --- Generic Orbit query helpers (used for code-graph assertions) -----------
# These take a filter dict (project_id, fqn, definition_type, …) instead of
# requiring the caller to know the node's primary id ahead of time, which is
# essential for File/Definition where ids are produced inside the indexer.

Orbit Query
    [Documentation]    Low-level POST /orbit/query. Accepts a fully-formed query dict and returns the parsed JSON response.
    [Arguments]    ${query}
    ${headers}=    GitLab Auth Headers
    ${body}=    Create Dictionary    query=${query}
    ${resp}=    POST    ${GITLAB_URL}/api/v4/orbit/query
    ...    headers=${headers}    json=${body}    expected_status=200
    RETURN    ${resp.json()}

Orbit Query With Token
    [Documentation]    Low-level POST /orbit/query using an explicit GitLab PAT.
    [Arguments]    ${query}    ${token}
    ${headers}=    Create Dictionary    PRIVATE-TOKEN=${token}    Content-Type=application/json
    ${body}=    Create Dictionary    query=${query}
    ${resp}=    POST    ${GITLAB_URL}/api/v4/orbit/query
    ...    headers=${headers}    json=${body}    expected_status=200
    RETURN    ${resp.json()}

Orbit Search Nodes With Filters
    [Documentation]    Search query for nodes of ${entity} matching ${filters} (dict). Returns the parsed response.
    [Arguments]    ${entity}    ${filters}    ${limit}=30
    ${limit_int}=    Convert To Integer    ${limit}
    ${node}=    Create Dictionary    id=n    entity=${entity}    filters=${filters}
    ${query}=    Create Dictionary    query_type=search    node=${node}    limit=${limit_int}
    ${resp}=    Orbit Query    ${query}
    RETURN    ${resp}

Orbit Count Nodes With Filters
    [Documentation]    Aggregation count(*) of ${entity} matching ${filters} (dict). Returns int.
    [Arguments]    ${entity}    ${filters}
    ${node}=    Create Dictionary    id=n    entity=${entity}    filters=${filters}
    ${agg}=    Create Dictionary    function=count    target=n
    ${query}=    Create Dictionary    query_type=aggregation
    ...    nodes=${{[$node]}}    aggregations=${{[$agg]}}
    ${resp}=    Orbit Query    ${query}
    RETURN    ${resp["result"]["columns"][0]["value"]}

Orbit Traversal Two Hop
    [Documentation]    Traversal between two nodes via a single relationship. Returns the parsed response.
    [Arguments]    ${src_entity}    ${src_filters}    ${rel_type}    ${dst_entity}    ${dst_filters}    ${limit}=10
    ${limit_int}=    Convert To Integer    ${limit}
    ${src}=    Create Dictionary    id=src    entity=${src_entity}    filters=${src_filters}
    ${dst}=    Create Dictionary    id=dst    entity=${dst_entity}    filters=${dst_filters}
    ${rel}=    Create Dictionary    type=${rel_type}    from=src    to=dst
    ${query}=    Create Dictionary    query_type=traversal
    ...    nodes=${{[$src, $dst]}}    relationships=${{[$rel]}}    limit=${limit_int}
    ${resp}=    Orbit Query    ${query}
    RETURN    ${resp}

# --- Generic edge assertions -------------------------------------------------

Wait For Edge Indexed
    [Documentation]    Poll until an edge of ${rel_type} between ${src_entity} (filtered by
    ...                id=${src_id} when not None) and ${dst_entity} (filtered by id=${dst_id}
    ...                when not None) is queryable. Pass ${None} on either side to match any
    ...                node of that entity (useful for e.g. "some User AUTHORED this Note").
    [Arguments]    ${src_entity}    ${src_id}    ${rel_type}    ${dst_entity}    ${dst_id}
    ...            ${timeout}=120s    ${interval}=3s
    ${scaled_timeout}=    Scaled Timeout    ${timeout}
    Wait Until Keyword Succeeds    ${scaled_timeout}    ${interval}
    ...    Verify Edge Indexed    ${src_entity}    ${src_id}    ${rel_type}    ${dst_entity}    ${dst_id}

Verify Edge Indexed
    [Arguments]    ${src_entity}    ${src_id}    ${rel_type}    ${dst_entity}    ${dst_id}
    ${src_filters}=    Create Dictionary
    IF    $src_id is not None
        Set To Dictionary    ${src_filters}    id=${src_id}
    END
    ${dst_filters}=    Create Dictionary
    IF    $dst_id is not None
        Set To Dictionary    ${dst_filters}    id=${dst_id}
    END
    ${resp}=    Orbit Traversal Two Hop    ${src_entity}    ${src_filters}
    ...    ${rel_type}    ${dst_entity}    ${dst_filters}
    Should Be True    ${resp["row_count"]} >= 1
    ...    No ${rel_type} edge ${src_entity}(${src_id}) -> ${dst_entity}(${dst_id})

# --- Code-graph assertions (per-project) ------------------------------------

File Count For Project Is
    [Documentation]    Wait until File node count under ${project_id} equals ${expected}.
    [Arguments]    ${project_id}    ${expected}    ${timeout}=180s    ${interval}=5s
    ${scaled_timeout}=    Scaled Timeout    ${timeout}
    Wait Until Keyword Succeeds    ${scaled_timeout}    ${interval}
    ...    Assert Entity Count For Project    File    ${project_id}    ${expected}

Definition Count For Project Is At Least
    [Arguments]    ${project_id}    ${minimum}    ${timeout}=180s    ${interval}=5s
    ${scaled_timeout}=    Scaled Timeout    ${timeout}
    Wait Until Keyword Succeeds    ${scaled_timeout}    ${interval}
    ...    Assert Entity Count At Least For Project    Definition    ${project_id}    ${minimum}

Assert Entity Count For Project
    [Arguments]    ${entity}    ${project_id}    ${expected}
    ${filters}=    Create Dictionary    project_id=${project_id}
    ${count}=    Orbit Count Nodes With Filters    ${entity}    ${filters}
    Should Be Equal As Integers    ${count}    ${expected}
    ...    ${entity} count for project ${project_id} = ${count} (expected ${expected})

Assert Entity Count At Least For Project
    [Arguments]    ${entity}    ${project_id}    ${minimum}
    ${filters}=    Create Dictionary    project_id=${project_id}
    ${count}=    Orbit Count Nodes With Filters    ${entity}    ${filters}
    Should Be True    ${count} >= ${minimum}
    ...    ${entity} count for project ${project_id} = ${count} (need >= ${minimum})

Definition Exists In Project
    [Documentation]    Assert that a Definition with ${fqn} (and optional ${definition_type}) exists under ${project_id}.
    ...                Returns the matched node dict.
    [Arguments]    ${project_id}    ${fqn}    ${definition_type}=${None}    ${timeout}=180s    ${interval}=5s
    ${scaled_timeout}=    Scaled Timeout    ${timeout}
    ${node}=    Wait Until Keyword Succeeds    ${scaled_timeout}    ${interval}
    ...    Lookup Definition By Fqn    ${project_id}    ${fqn}    ${definition_type}
    RETURN    ${node}

Lookup Definition By Fqn
    [Arguments]    ${project_id}    ${fqn}    ${definition_type}=${None}
    ${filters}=    Create Dictionary    project_id=${project_id}    fqn=${fqn}
    IF    $definition_type is not None
        Set To Dictionary    ${filters}    definition_type=${definition_type}
    END
    ${resp}=    Orbit Search Nodes With Filters    Definition    ${filters}    limit=5
    Should Be True    ${resp["row_count"]} >= 1
    ...    Definition fqn=${fqn} type=${definition_type} not found in project ${project_id}
    RETURN    ${resp["result"]["nodes"][0]}

# NOTE: the ontology has a CALLS edge (Definition→Definition, Definition→ImportedSymbol)
# and the indexer populates it in v1_gl_edge — but `config/ontology/schema.yaml` does not
# register it under `edges:`, so the Orbit API rejects "type": "CALLS" with a 400. The
# closest assertion we can make through the API today is DEFINES, which models containment
# (Module→Class, Class→Method, File→top-level-Definition).

Defines Edge Exists Between Definitions
    [Documentation]    Assert a DEFINES edge from Definition fqn=${parent_fqn} to Definition fqn=${child_fqn}
    ...                under ${project_id}. Models structural containment (e.g. Class defines Method).
    [Arguments]    ${project_id}    ${parent_fqn}    ${child_fqn}    ${timeout}=180s    ${interval}=5s
    ${scaled_timeout}=    Scaled Timeout    ${timeout}
    Wait Until Keyword Succeeds    ${scaled_timeout}    ${interval}
    ...    Assert Defines Edge Between Definitions    ${project_id}    ${parent_fqn}    ${child_fqn}

Assert Defines Edge Between Definitions
    [Arguments]    ${project_id}    ${parent_fqn}    ${child_fqn}
    ${src_filters}=    Create Dictionary    project_id=${project_id}    fqn=${parent_fqn}
    ${dst_filters}=    Create Dictionary    project_id=${project_id}    fqn=${child_fqn}
    ${resp}=    Orbit Traversal Two Hop    Definition    ${src_filters}
    ...    DEFINES    Definition    ${dst_filters}
    Should Be True    ${resp["row_count"]} >= 1
    ...    No DEFINES edge from ${parent_fqn} to ${child_fqn} in project ${project_id}

Defines Edge Exists Between File And Definition
    [Documentation]    Assert a DEFINES edge from File path=${file_path} to Definition fqn=${def_fqn}
    ...                under ${project_id}. Models the file→top-level-definition relationship.
    [Arguments]    ${project_id}    ${file_path}    ${def_fqn}    ${timeout}=180s    ${interval}=5s
    ${scaled_timeout}=    Scaled Timeout    ${timeout}
    Wait Until Keyword Succeeds    ${scaled_timeout}    ${interval}
    ...    Assert Defines Edge Between File And Definition    ${project_id}    ${file_path}    ${def_fqn}

Assert Defines Edge Between File And Definition
    [Arguments]    ${project_id}    ${file_path}    ${def_fqn}
    ${src_filters}=    Create Dictionary    project_id=${project_id}    path=${file_path}
    ${dst_filters}=    Create Dictionary    project_id=${project_id}    fqn=${def_fqn}
    ${resp}=    Orbit Traversal Two Hop    File    ${src_filters}
    ...    DEFINES    Definition    ${dst_filters}
    Should Be True    ${resp["row_count"]} >= 1
    ...    No DEFINES edge from File ${file_path} to Definition ${def_fqn} in project ${project_id}

Imports Edge Exists Between File And Imported Symbol
    [Documentation]    Assert an IMPORTS edge from File path=${file_path} to ImportedSymbol matching
    ...                (import_path=${import_path}, identifier_name=${identifier_name}) under ${project_id}.
    [Arguments]    ${project_id}    ${file_path}    ${import_path}    ${identifier_name}
    ...    ${timeout}=180s    ${interval}=5s
    ${scaled_timeout}=    Scaled Timeout    ${timeout}
    Wait Until Keyword Succeeds    ${scaled_timeout}    ${interval}
    ...    Assert Imports Edge Between File And Imported Symbol
    ...    ${project_id}    ${file_path}    ${import_path}    ${identifier_name}

Assert Imports Edge Between File And Imported Symbol
    [Arguments]    ${project_id}    ${file_path}    ${import_path}    ${identifier_name}
    ${src_filters}=    Create Dictionary    project_id=${project_id}    path=${file_path}
    ${dst_filters}=    Create Dictionary    project_id=${project_id}
    ...    import_path=${import_path}    identifier_name=${identifier_name}
    ${resp}=    Orbit Traversal Two Hop    File    ${src_filters}
    ...    IMPORTS    ImportedSymbol    ${dst_filters}
    Should Be True    ${resp["row_count"]} >= 1
    ...    No IMPORTS edge from File ${file_path} to ${import_path}.${identifier_name} in project ${project_id}
