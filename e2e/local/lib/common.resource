*** Settings ***
Library    RequestsLibrary
Library    Collections
Library    String
Library    DateTime
Library    OperatingSystem
Library    Process

Documentation    Shared keywords for the GetGraphStatus matrix suites.
...              Suites read:
...                GITLAB_URL           e.g. https://gdk.test:3443
...                GITLAB_PAT           admin-scoped PAT
...                NATS_URL             optional, for KV seed/inspect
...                STALENESS_FAST_SECS  optional, for F-series

*** Variables ***
${GITLAB_URL}             %{GITLAB_URL=https://gdk.test:3443}
${GITLAB_PAT}             %{GITLAB_PAT}
${NATS_URL}               %{NATS_URL=localhost:4222}
${NATS_BIN}               %{NATS_BIN=/home/arivera_gitlab_com/go/bin/nats}
${KV_BUCKET}              indexing_progress
${KNOWN_NS}               22
${VERIFY_SSL}             ${False}

*** Keywords ***
Auth Headers
    RETURN    ${{{"PRIVATE-TOKEN": "${GITLAB_PAT}", "Content-Type": "application/json"}}}

Bare PAT Header
    RETURN    ${{{"PRIVATE-TOKEN": "${GITLAB_PAT}"}}}

Random Suffix
    ${s}=    Generate Random String    6    [LOWER][NUMBERS]
    RETURN    ${s}

Create Group
    [Arguments]    ${name}    ${path}    ${parent_id}=${None}
    ${headers}=    Auth Headers
    ${body}=    Create Dictionary    name=${name}    path=${path}    visibility=public
    IF    $parent_id is not None
        Set To Dictionary    ${body}    parent_id=${parent_id}
    END
    ${resp}=    POST    ${GITLAB_URL}/api/v4/groups
    ...    headers=${headers}    json=${body}    expected_status=201    verify=${VERIFY_SSL}
    RETURN    ${resp.json()}

Create Project
    [Arguments]    ${name}    ${namespace_id}    ${initialize_with_readme}=${True}
    ${headers}=    Auth Headers
    ${body}=    Create Dictionary
    ...    name=${name}    namespace_id=${namespace_id}    visibility=public
    ...    initialize_with_readme=${initialize_with_readme}
    ${resp}=    POST    ${GITLAB_URL}/api/v4/projects
    ...    headers=${headers}    json=${body}    expected_status=201    verify=${VERIFY_SSL}
    RETURN    ${resp.json()}

Delete Group
    [Arguments]    ${group_id}
    ${headers}=    Bare PAT Header
    ${resp}=    DELETE    ${GITLAB_URL}/api/v4/groups/${group_id}
    ...    headers=${headers}    expected_status=any    verify=${VERIFY_SSL}
    RETURN    ${resp}

Enable Knowledge Graph
    [Arguments]    ${namespace_id}
    ${headers}=    Bare PAT Header
    Wait Until Keyword Succeeds    30s    3s    Enable KG Once    ${namespace_id}    ${headers}

Enable KG Once
    [Arguments]    ${namespace_id}    ${headers}
    ${resp}=    PUT    ${GITLAB_URL}/api/v4/admin/knowledge_graph/namespaces/${namespace_id}
    ...    headers=${headers}    expected_status=any    verify=${VERIFY_SSL}
    Should Be True    ${resp.status_code} == 200
    ...    Enable KG ${namespace_id} returned ${resp.status_code}: ${resp.text}

Disable Knowledge Graph
    [Arguments]    ${namespace_id}
    ${headers}=    Bare PAT Header
    ${resp}=    DELETE    ${GITLAB_URL}/api/v4/admin/knowledge_graph/namespaces/${namespace_id}
    ...    headers=${headers}    expected_status=any    verify=${VERIFY_SSL}
    RETURN    ${resp}

Graph Status By Namespace
    [Arguments]    ${namespace_id}    ${expected}=any
    ${headers}=    Auth Headers
    ${params}=    Create Dictionary    namespace_id=${namespace_id}
    ${resp}=    GET    url=${GITLAB_URL}/api/v4/orbit/graph_status
    ...    headers=${headers}    params=${params}
    ...    expected_status=${expected}    verify=${VERIFY_SSL}
    RETURN    ${resp}

Graph Status By Project
    [Arguments]    ${project_id}    ${expected}=any
    ${headers}=    Auth Headers
    ${params}=    Create Dictionary    project_id=${project_id}
    ${resp}=    GET    url=${GITLAB_URL}/api/v4/orbit/graph_status
    ...    headers=${headers}    params=${params}
    ...    expected_status=${expected}    verify=${VERIFY_SSL}
    RETURN    ${resp}

Graph Status By Full Path
    [Arguments]    ${full_path}    ${expected}=any
    ${headers}=    Auth Headers
    ${params}=    Create Dictionary    full_path=${full_path}
    ${resp}=    GET    url=${GITLAB_URL}/api/v4/orbit/graph_status
    ...    headers=${headers}    params=${params}
    ...    expected_status=${expected}    verify=${VERIFY_SSL}
    RETURN    ${resp}

Wait For State
    [Arguments]    ${namespace_id}    ${state}    ${timeout}=300s    ${poll}=5s
    Wait Until Keyword Succeeds    ${timeout}    ${poll}
    ...    Verify State Is    ${namespace_id}    ${state}

Verify State Is
    [Arguments]    ${namespace_id}    ${expected}
    ${resp}=    Graph Status By Namespace    ${namespace_id}    200
    Should Be Equal    ${resp.json()["state"]}    ${expected}
    ...    ns=${namespace_id} state=${resp.json()["state"]} expected=${expected}

Wait For Idle
    [Arguments]    ${namespace_id}    ${timeout}=300s
    Wait For State    ${namespace_id}    idle    ${timeout}

Assert Response Shape
    [Arguments]    ${body}
    FOR    ${key}    IN    state    initial_backfill_done    updated_at    stale    domains    edge_counts    sdlc    code
        Dictionary Should Contain Key    ${body}    ${key}
    END
    Should Be True    isinstance($body["domains"], list)
    Should Be True    isinstance($body["edge_counts"], dict)

Sum Entity Counts
    [Arguments]    ${domains}
    ${total}=    Set Variable    ${0}
    FOR    ${domain}    IN    @{domains}
        FOR    ${item}    IN    @{domain["items"]}
            ${total}=    Evaluate    ${total} + ${item["count"]}
        END
    END
    RETURN    ${total}

Count Items With Status
    [Arguments]    ${domains}    ${status}
    ${n}=    Set Variable    ${0}
    FOR    ${domain}    IN    @{domains}
        FOR    ${item}    IN    @{domain["items"]}
            IF    "${item["status"]}" == "${status}"
                ${n}=    Evaluate    ${n} + 1
            END
        END
    END
    RETURN    ${n}

Parse RFC3339
    [Arguments]    ${timestamp}
    ${ts}=    Evaluate
    ...    __import__("datetime").datetime.fromisoformat("${timestamp}".replace("Z","+00:00")).timestamp()
    RETURN    ${ts}

Assert Timestamp Non Decreasing
    [Arguments]    ${prev}    ${curr}    ${label}=timestamp
    IF    "${prev}" == "${EMPTY}"
        RETURN
    END
    ${p}=    Parse RFC3339    ${prev}
    ${c}=    Parse RFC3339    ${curr}
    Should Be True    ${c} >= ${p}
    ...    ${label} regressed: prev=${prev} curr=${curr}

Poll Graph Status
    [Arguments]    ${namespace_id}    ${n}=5    ${interval}=10
    ${snapshots}=    Create List
    FOR    ${i}    IN RANGE    ${n}
        ${resp}=    Graph Status By Namespace    ${namespace_id}    200
        Append To List    ${snapshots}    ${resp.json()}
        IF    ${i} < ${n} - 1
            Sleep    ${interval}s
        END
    END
    RETURN    ${snapshots}

NATS KV Get
    [Arguments]    ${key}
    ${result}=    Run Process    ${NATS_BIN}    kv    get    ${KV_BUCKET}    ${key}    --raw
    ...    env:NATS_URL=${NATS_URL}    stdout=STDOUT    stderr=STDERR
    RETURN    ${result}

NATS KV Delete
    [Arguments]    ${key}
    Run Process    ${NATS_BIN}    kv    del    -f    ${KV_BUCKET}    ${key}
    ...    env:NATS_URL=${NATS_URL}    stdout=STDOUT    stderr=STDERR

NATS KV Put
    [Arguments]    ${key}    ${value}
    ${tmp}=    Evaluate    __import__('tempfile').mkstemp()[1]
    Create File    ${tmp}    ${value}
    ${result}=    Run Process    ${NATS_BIN}    kv    put    ${KV_BUCKET}    ${key}
    ...    \@${tmp}    env:NATS_URL=${NATS_URL}    stdout=STDOUT    stderr=STDERR
    RETURN    ${result}

Ensure Known Indexed Namespace
    [Documentation]    Returns a namespace id guaranteed to already be at state=idle.
    ...                Uses ${KNOWN_NS} (22 / Commit451 on this VM) which is pre-seeded.
    ${resp}=    Graph Status By Namespace    ${KNOWN_NS}    200
    Should Be Equal    ${resp.json()["state"]}    idle
    ...    known namespace ${KNOWN_NS} expected idle, got ${resp.json()["state"]}
    RETURN    ${KNOWN_NS}
