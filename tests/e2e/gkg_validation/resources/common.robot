*** Settings ***
Library     RequestsLibrary
Library     Collections
Library     OperatingSystem
Library     Process
Library     String


*** Variables ***
${GDK_URL}                  https://gdk.test:3443
${API_BASE}                 ${GDK_URL}/api/v4
${CLICKHOUSE_URL}           http://127.0.0.1:8123
${GRAPH_DB}                 gitlab_clickhouse_main_development
${DATALAKE_DB}              gitlab_clickhouse_development
${GKG_GRPC}                 127.0.0.1:50054
${GKG_HTTP}                 http://127.0.0.1:4200
${NATS_URL}                 nats://127.0.0.1:4222
${GKG_REPO}                 %{HOME}/gitlab/orbit/knowledge-graph
${CDC_WAIT_SECS}            90
${INDEX_TIMEOUT_SECS}       120


*** Keywords ***
Load PAT
    ${token}=    Get File    %{HOME}/.gdk_token
    ${token}=    Strip String    ${token}
    Set Suite Variable    ${PAT}    ${token}

Create API Session
    Load PAT
    Create Session    gdk    ${API_BASE}    verify=${False}
    &{headers}=    Create Dictionary    PRIVATE-TOKEN=${PAT}    Content-Type=application/json
    Set Suite Variable    &{API_HEADERS}    &{headers}

GitLab API GET
    [Arguments]    ${path}
    ${resp}=    GET On Session    gdk    ${path}    headers=&{API_HEADERS}    expected_status=any
    RETURN    ${resp}

GitLab API POST
    [Arguments]    ${path}    ${data}
    ${resp}=    POST On Session    gdk    ${path}    json=${data}    headers=&{API_HEADERS}    expected_status=any
    RETURN    ${resp}

GitLab API PUT
    [Arguments]    ${path}    ${data}
    ${resp}=    PUT On Session    gdk    ${path}    json=${data}    headers=&{API_HEADERS}    expected_status=any
    RETURN    ${resp}

Orbit Query
    [Documentation]    Execute a query against POST /api/v4/orbit/query
    [Arguments]    ${query}
    ${data}=    Create Dictionary    query=${query}
    ${resp}=    POST On Session    gdk    /orbit/query    json=${data}    headers=&{API_HEADERS}    expected_status=any
    RETURN    ${resp}

ClickHouse Query
    [Arguments]    ${sql}    ${database}=${GRAPH_DB}
    ${result}=    Run Process
    ...    bash
    ...    -c
    ...    curl -s "${CLICKHOUSE_URL}/?database\=${database}" --data-binary @- <<< "${sql}"
    ...    timeout=10
    RETURN    ${result.stdout}

Wait For CDC
    [Documentation]    Wait for Siphon CDC replication and reload traversal path dictionary
    ${has_ns}=    Run Keyword And Return Status    Variable Should Exist    ${TOP_GROUP_ID}
    IF    ${has_ns}
        Wait Until Namespace In Datalake    ${TOP_GROUP_ID}
        Reload Traversal Path Dictionary
        Sleep    15
    ELSE
        Sleep    ${CDC_WAIT_SECS}
    END

Wait Until Namespace In Datalake
    [Arguments]    ${namespace_id}    ${timeout}=${CDC_WAIT_SECS}
    ${deadline}=    Evaluate    time.time() + ${timeout}    modules=time
    FOR    ${i}    IN RANGE    200
        ${now}=    Evaluate    time.time()    modules=time
        IF    ${now} > ${deadline}
            Log    WARNING: namespace ${namespace_id} not in datalake after ${timeout}s, continuing
            RETURN
        END
        ${result}=    ClickHouse Query
        ...    SELECT count() FROM siphon_namespaces FINAL WHERE id = ${namespace_id}
        ...    ${DATALAKE_DB}
        ${count}=    Strip String    ${result}
        IF    '${count}' != '0'
            Log    Namespace ${namespace_id} found in datalake after ${i} polls
            RETURN
        END
        Sleep    3
    END

Reload Traversal Path Dictionary
    ${result}=    ClickHouse Query    SYSTEM RELOAD DICTIONARY project_traversal_paths_dict    ${DATALAKE_DB}
    Log    Dictionary reloaded

Enable Namespace For KG
    [Arguments]    ${namespace_id}
    ${script}=    Catenate
    ...    ActiveRecord::Base.connection.execute(
    ...    "INSERT INTO knowledge_graph_enabled_namespaces (root_namespace_id, created_at, updated_at)
    ...    VALUES (${namespace_id}, NOW(), NOW()) ON CONFLICT DO NOTHING"
    ...    )
    ${result}=    Run Process
    ...    bash    -c
    ...    cd %{HOME}/gitlab/gdk/gitlab && bundle exec spring rails runner '${script}'
    ...    timeout=30
    Log    ${result.stdout}
    Should Be Equal As Integers    ${result.rc}    0

Create Top Level Group
    [Arguments]    ${name}    ${path}
    ${data}=    Create Dictionary    name=${name}    path=${path}    visibility=public
    ${resp}=    GitLab API POST    /groups    ${data}
    Should Be Equal As Integers    ${resp.status_code}    201    msg=Failed to create group: ${resp.text}
    RETURN    ${resp.json()['id']}

Create Subgroup
    [Arguments]    ${name}    ${path}    ${parent_id}
    ${data}=    Create Dictionary    name=${name}    path=${path}    parent_id=${parent_id}    visibility=public
    ${resp}=    GitLab API POST    /groups    ${data}
    Should Be Equal As Integers    ${resp.status_code}    201    msg=Failed to create subgroup: ${resp.text}
    RETURN    ${resp.json()['id']}

Create Project
    [Arguments]    ${name}    ${namespace_id}
    ${data}=    Create Dictionary
    ...    name=${name}
    ...    namespace_id=${namespace_id}
    ...    visibility=public
    ...    initialize_with_readme=${True}
    ${resp}=    GitLab API POST    /projects    ${data}
    Should Be Equal As Integers    ${resp.status_code}    201    msg=Failed to create project: ${resp.text}
    RETURN    ${resp.json()['id']}

Create Merge Request
    [Arguments]    ${project_id}    ${title}    ${source_branch}
    ${branch_data}=    Create Dictionary    branch=${source_branch}    ref=main
    POST On Session    gdk    /projects/${project_id}/repository/branches
    ...    json=${branch_data}    headers=&{API_HEADERS}    expected_status=any
    ${mr_data}=    Create Dictionary    title=${title}    source_branch=${source_branch}    target_branch=main
    ${resp}=    GitLab API POST    /projects/${project_id}/merge_requests    ${mr_data}
    Should Be Equal As Integers    ${resp.status_code}    201    msg=Failed to create MR: ${resp.text}
    RETURN    ${resp.json()['iid']}

Create Issue
    [Arguments]    ${project_id}    ${title}
    ${data}=    Create Dictionary    title=${title}
    ${resp}=    GitLab API POST    /projects/${project_id}/issues    ${data}
    Should Be Equal As Integers    ${resp.status_code}    201    msg=Failed to create issue: ${resp.text}
    RETURN    ${resp.json()['iid']}

Start GKG Services
    [Documentation]    Start webserver, dispatcher (fast cron), and indexer
    Stop GKG Services
    ${test_dir}=    Set Variable    ${GKG_REPO}/tests/e2e/gkg_validation
    ${web}=    Start Process    bash    -c
    ...    cd ${GKG_REPO} && set -a && source .env.local && set +a && target/release/gkg-server --mode\=webserver
    ...    stdout=/tmp/gkg-webserver.log    stderr=STDOUT
    ${disp}=    Start Process    bash    -c
    ...    cd ${GKG_REPO} && set -a && source .env.local && set +a && export GKG__SCHEDULE__TASKS__GLOBAL__CRON='* * * * * *' GKG__SCHEDULE__TASKS__NAMESPACE__CRON='* * * * * *' && target/release/gkg-server --mode\=dispatch-indexing
    ...    stdout=/tmp/gkg-dispatcher.log    stderr=STDOUT
    ${idx}=    Start Process    bash    -c
    ...    cd ${GKG_REPO} && set -a && source .env.local && set +a && target/release/gkg-server --mode\=indexer
    ...    stdout=/tmp/gkg-indexer.log    stderr=STDOUT
    Set Suite Variable    ${WEBSERVER_HANDLE}    ${web}
    Set Suite Variable    ${DISPATCHER_HANDLE}    ${disp}
    Set Suite Variable    ${INDEXER_HANDLE}    ${idx}
    Sleep    5

Stop GKG Services
    Run Process    bash    -c
    ...    pkill -9 -f "gkg-server --mode" 2>/dev/null; sleep 1
    ...    timeout=10

Wait For Indexing Complete
    [Documentation]    Poll the Orbit API until indexing is complete for a traversal path
    [Arguments]    ${traversal_path}    ${timeout_secs}=${INDEX_TIMEOUT_SECS}
    ${deadline}=    Evaluate    time.time() + ${timeout_secs}    modules=time
    FOR    ${i}    IN RANGE    200
        ${now}=    Evaluate    time.time()    modules=time
        IF    ${now} > ${deadline}
            Fail    Timed out waiting for indexing after ${timeout_secs}s
        END
        ${resp}=    GitLab API GET    /orbit/indexing_status?traversal_path=${traversal_path}
        IF    ${resp.status_code} == 200
            ${body}=    Set Variable    ${resp.json()}
            ${state}=    Set Variable    ${body['state']}
            ${done}=    Set Variable    ${body['initial_backfill_done']}
            Log    Poll ${i}: state=${state} backfill_done=${done}
            IF    '${state}' == 'idle' and ${done}    RETURN    ${resp}
        END
        Sleep    3
    END
    Fail    Indexing did not complete within ${timeout_secs}s
