*** Settings ***
Library    RequestsLibrary
Library    Collections
Library    OperatingSystem
Library    Process
Library    String

*** Variables ***
${GDK_URL}              https://gdk.test:3443
${API_BASE}             ${GDK_URL}/api/v4
${CLICKHOUSE_URL}       http://127.0.0.1:8123
${GRAPH_DB}             gitlab_clickhouse_main_development
${DATALAKE_DB}          gitlab_clickhouse_development
${GKG_GRPC}             127.0.0.1:50054
${GKG_HTTP}             http://127.0.0.1:4200
${NATS_URL}             nats://127.0.0.1:4222
${GKG_REPO}             %{HOME}/gitlab/orbit/knowledge-graph
${CDC_WAIT_SECS}        60
${INDEX_TIMEOUT_SECS}   60

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

ClickHouse Query
    [Arguments]    ${sql}    ${database}=${GRAPH_DB}
    ${result}=    Run Process    bash    -c    curl -s "http://127.0.0.1:8123/?database\=${database}" --data-binary @- <<< "${sql}"    timeout=10
    RETURN    ${result.stdout}

Wait For CDC
    [Documentation]    Wait for Siphon CDC replication, reload the traversal path
    ...               dictionary so MRs/issues get the correct traversal_path
    ...               through the materialized views, then touch MRs to re-replicate.
    ${has_ns}=    Run Keyword And Return Status    Variable Should Exist    ${TOP_GROUP_ID}
    IF    ${has_ns}
        Wait Until Namespace In Datalake    ${TOP_GROUP_ID}
        Reload Traversal Path Dictionary
        Touch MRs To Re-Replicate
        Sleep    15
    ELSE
        Sleep    ${CDC_WAIT_SECS}
    END

Wait Until Namespace In Datalake
    [Arguments]    ${namespace_id}    ${timeout}=${CDC_WAIT_SECS}
    ${deadline}=    Evaluate    time.time() + ${timeout}    modules=time
    FOR    ${i}    IN RANGE    100
        ${now}=    Evaluate    time.time()    modules=time
        IF    ${now} > ${deadline}
            Log    WARNING: namespace ${namespace_id} not in datalake after ${timeout}s, continuing anyway
            RETURN
        END
        ${result}=    ClickHouse Query    SELECT count() FROM siphon_namespaces FINAL WHERE id = ${namespace_id}    ${DATALAKE_DB}
        ${count}=    Strip String    ${result}
        IF    '${count}' != '0'
            Log    Namespace ${namespace_id} found in datalake after ${i} polls
            RETURN
        END
        Sleep    3
    END

Reload Traversal Path Dictionary
    [Documentation]    Reload the ClickHouse dictionary so MV inserts get correct traversal_paths
    ${result}=    ClickHouse Query    SYSTEM RELOAD DICTIONARY project_traversal_paths_dict    ${DATALAKE_DB}
    Log    Dictionary reloaded

Touch MRs To Re-Replicate
    [Documentation]    Touch MRs in Rails to trigger Siphon re-replication through the MV
    ${has_p1}=    Run Keyword And Return Status    Variable Should Exist    ${PROJECT1_ID}
    IF    not ${has_p1}    RETURN
    ${script}=    Catenate    SEPARATOR=;
    ...    [${PROJECT1_ID}, ${PROJECT2_ID}].each do |pid|
    ...      MergeRequest.where(source_project_id: pid).update_all(updated_at: Time.current)
    ...    end
    ...    Issue.where(project_id: [${PROJECT1_ID}, ${PROJECT2_ID}]).update_all(updated_at: Time.current)
    ${result}=    Run Process    bash    -c
    ...    cd %{HOME}/gitlab/gdk/gitlab && bundle exec spring rails runner '${script}'
    ...    timeout=30
    Log    Touched MRs and issues for re-replication

Start Indexer Services
    [Documentation]    Start dispatcher (every-second cron) and indexer as background processes
    Run Process    bash    -c    pkill -9 -f "gkg-server --mode.dispatch" 2>/dev/null; pkill -9 -f "gkg-server --mode.indexer" 2>/dev/null; sleep 1    timeout=10
    ${test_dir}=    Set Variable    ${GKG_REPO}/tests/e2e/indexing_progress
    ${disp}=    Start Process    ${test_dir}/run-dispatcher.sh
    ...    stdout=/tmp/gkg-dispatcher.log    stderr=STDOUT
    ${idx}=    Start Process    ${test_dir}/run-indexer.sh
    ...    stdout=/tmp/gkg-indexer.log    stderr=STDOUT
    Set Suite Variable    ${DISPATCHER_HANDLE}    ${disp}
    Set Suite Variable    ${INDEXER_HANDLE}    ${idx}

Stop Indexer Services
    [Documentation]    Stop dispatcher and indexer (leaves webserver running)
    Run Process    bash    -c    pkill -9 -f "gkg-server --mode.dispatch" 2>/dev/null; pkill -9 -f "gkg-server --mode.indexer" 2>/dev/null    timeout=10

Wait For Indexing Complete
    [Arguments]    ${traversal_path}    ${timeout_secs}=${INDEX_TIMEOUT_SECS}
    [Documentation]    Poll GetIndexingStatus until state=idle and initial_backfill_done=true
    ${deadline}=    Evaluate    time.time() + ${timeout_secs}    modules=time
    FOR    ${i}    IN RANGE    200
        ${now}=    Evaluate    time.time()    modules=time
        IF    ${now} > ${deadline}
            Fail    Timed out waiting for indexing to complete after ${timeout_secs}s
        END
        ${resp}=    Get Indexing Status    ${traversal_path}
        IF    ${resp.status_code} == 200
            ${body}=    Set Variable    ${resp.json()}
            ${state}=    Set Variable    ${body['state']}
            ${done}=    Set Variable    ${body['initial_backfill_done']}
            Log    Poll ${i}: state=${state} backfill_done=${done}
            IF    '${state}' == 'idle' and ${done}
                RETURN    ${resp}
            END
        END
        Sleep    3
    END
    Fail    Indexing did not complete within ${timeout_secs}s

Get Indexing Status
    [Arguments]    ${traversal_path}    ${exact_counts}=${False}
    ${params}=    Create Dictionary    traversal_path=${traversal_path}    exact_counts=${exact_counts}
    ${resp}=    GET On Session    gdk    /orbit/indexing_status    params=${params}    headers=&{API_HEADERS}    expected_status=any
    RETURN    ${resp}

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
    ${group_id}=    Set Variable    ${resp.json()['id']}
    RETURN    ${group_id}

Create Subgroup
    [Arguments]    ${name}    ${path}    ${parent_id}
    ${data}=    Create Dictionary    name=${name}    path=${path}    parent_id=${parent_id}    visibility=public
    ${resp}=    GitLab API POST    /groups    ${data}
    Should Be Equal As Integers    ${resp.status_code}    201    msg=Failed to create subgroup: ${resp.text}
    RETURN    ${resp.json()['id']}

Create Project
    [Arguments]    ${name}    ${namespace_id}
    ${data}=    Create Dictionary    name=${name}    namespace_id=${namespace_id}    visibility=public    initialize_with_readme=${True}
    ${resp}=    GitLab API POST    /projects    ${data}
    Should Be Equal As Integers    ${resp.status_code}    201    msg=Failed to create project: ${resp.text}
    RETURN    ${resp.json()['id']}

Create Merge Request
    [Arguments]    ${project_id}    ${title}    ${source_branch}
    # Create a branch first
    ${branch_data}=    Create Dictionary    branch=${source_branch}    ref=main
    ${branch_resp}=    POST On Session    gdk    /projects/${project_id}/repository/branches
    ...    json=${branch_data}    headers=&{API_HEADERS}    expected_status=any
    # Create the MR
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

Get Domain Item Count
    [Arguments]    ${status_response}    ${domain_name}    ${entity_name}
    [Documentation]    Extract a specific entity count from the indexing status response
    ${domains}=    Set Variable    ${status_response.json()['domains']}
    FOR    ${domain}    IN    @{domains}
        IF    '${domain["name"]}' == '${domain_name}'
            FOR    ${item}    IN    @{domain["items"]}
                IF    '${item["name"]}' == '${entity_name}'
                    RETURN    ${item["count"]}
                END
            END
        END
    END
    RETURN    ${0}
