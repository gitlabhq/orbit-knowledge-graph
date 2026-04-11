*** Settings ***
Documentation     End-to-end tests for the GetIndexingStatus feature.
...               Covers initial backfill, incremental updates, hierarchy rollup,
...               state transitions, and count accuracy.
...               Uses the status endpoint itself to poll for completion.
Resource          resources/common.robot
Suite Setup       Suite Init
Suite Teardown    Suite Cleanup

*** Keywords ***
Suite Init
    Create API Session
    ${ts}=    Evaluate    str(int(time.time()))[-6:]    modules=time
    Set Suite Variable    ${RUN_ID}    ${ts}
    ${result}=    Run Process    bash    -c
    ...    cd $HOME/gitlab/gdk/gitlab && bundle exec spring rails runner 'Feature.enable(:knowledge_graph); puts Feature.enabled?(:knowledge_graph)'
    ...    timeout=30
    Should Contain    ${result.stdout}    true

Suite Cleanup
    Stop Indexer Services
    Log    E2E indexing progress tests completed

Seed Test Data
    [Documentation]    Create test entities via GitLab API and enable namespace
    ${name}=    Set Variable    idx-e2e-${RUN_ID}
    ${group_id}=    Create Top Level Group    ${name}    ${name}
    Set Suite Variable    ${TOP_GROUP_ID}    ${group_id}

    ${sub_name}=    Set Variable    sub-${RUN_ID}
    ${sub_id}=    Create Subgroup    ${sub_name}    ${sub_name}    ${group_id}
    Set Suite Variable    ${SUB_GROUP_ID}    ${sub_id}

    ${proj1_id}=    Create Project    app-one-${RUN_ID}    ${group_id}
    ${proj2_id}=    Create Project    app-two-${RUN_ID}    ${group_id}
    Set Suite Variable    ${PROJECT1_ID}    ${proj1_id}
    Set Suite Variable    ${PROJECT2_ID}    ${proj2_id}

    Create Merge Request    ${proj1_id}    Fix login    fix/login-${RUN_ID}
    Create Merge Request    ${proj1_id}    Add tests    feat/tests-${RUN_ID}
    Create Merge Request    ${proj2_id}    Update deps    chore/deps-${RUN_ID}

    Create Issue    ${proj1_id}    Track performance
    Create Issue    ${proj2_id}    Security audit

    Enable Namespace For KG    ${group_id}

    Set Suite Variable    ${ORG_ID}    1
    Set Suite Variable    ${TRAVERSAL_PATH}    1/${group_id}/

*** Test Cases ***
Test 1: Initial Backfill Produces Correct Counts
    [Documentation]    Seed data, start indexer services, poll status endpoint until
    ...               state=idle and initial_backfill_done=true, then verify counts.
    [Tags]    backfill    critical
    Seed Test Data
    Wait For CDC
    Start Indexer Services

    ${resp}=    Wait For Indexing Complete    ${TRAVERSAL_PATH}
    ${body}=    Set Variable    ${resp.json()}

    Should Be Equal    ${body['state']}    idle
    Should Be True    ${body['initial_backfill_done']}
    Should Not Be True    ${body['stale']}

    ${group_count}=    Get Domain Item Count    ${resp}    core    Group
    Should Be True    ${group_count} >= 2    msg=Expected >= 2 groups, got ${group_count}

    ${project_count}=    Get Domain Item Count    ${resp}    core    Project
    Should Be True    ${project_count} >= 2    msg=Expected >= 2 projects, got ${project_count}

    ${mr_count}=    Get Domain Item Count    ${resp}    code_review    MergeRequest
    Should Be True    ${mr_count} >= 3    msg=Expected >= 3 MRs, got ${mr_count}

    ${edge_counts}=    Set Variable    ${body['edge_counts']}
    ${edge_keys}=    Get Dictionary Keys    ${edge_counts}
    Should Not Be Empty    ${edge_keys}    msg=Expected edge counts

    Log    Backfill OK: groups=${group_count} projects=${project_count} MRs=${mr_count} edges=${edge_keys}

Test 2: Hierarchy Rollup
    [Documentation]    Child group counts should be a subset of parent group counts
    [Tags]    hierarchy
    [Setup]    Variable Should Exist    ${SUB_GROUP_ID}

    ${sub_proj}=    Create Project    sub-app-${RUN_ID}    ${SUB_GROUP_ID}
    Wait For CDC
    Sleep    15

    ${parent_resp}=    Get Indexing Status    ${TRAVERSAL_PATH}
    Should Be Equal As Integers    ${parent_resp.status_code}    200

    ${child_tp}=    Set Variable    ${ORG_ID}/${SUB_GROUP_ID}/
    ${child_resp}=    Get Indexing Status    ${child_tp}
    Should Be Equal As Integers    ${child_resp.status_code}    200

    ${parent_projects}=    Get Domain Item Count    ${parent_resp}    core    Project
    ${child_projects}=    Get Domain Item Count    ${child_resp}    core    Project

    Should Be True    ${parent_projects} > ${child_projects}
    ...    msg=Parent (${parent_projects}) should include more projects than child (${child_projects})

    Log    Rollup OK: parent=${parent_projects} child=${child_projects}

Test 3: Incremental SDLC Updates
    [Documentation]    New entities after backfill should increment counts
    [Tags]    incremental
    [Setup]    Variable Should Exist    ${PROJECT1_ID}

    ${before}=    Get Indexing Status    ${TRAVERSAL_PATH}
    Should Be Equal As Integers    ${before.status_code}    200
    ${before_mr}=    Get Domain Item Count    ${before}    code_review    MergeRequest
    ${before_ts}=    Set Variable    ${before.json()['updated_at']}

    Create Merge Request    ${PROJECT1_ID}    Hotfix auth    hotfix/auth-${RUN_ID}
    Create Merge Request    ${PROJECT2_ID}    Refactor DB    refactor/db-${RUN_ID}
    Create Issue    ${PROJECT1_ID}    New feature request

    Wait For CDC
    Sleep    15

    ${after}=    Get Indexing Status    ${TRAVERSAL_PATH}
    Should Be Equal As Integers    ${after.status_code}    200
    ${after_mr}=    Get Domain Item Count    ${after}    code_review    MergeRequest
    ${after_ts}=    Set Variable    ${after.json()['updated_at']}

    ${expected}=    Evaluate    ${before_mr} + 2
    Should Be True    ${after_mr} >= ${expected}
    ...    msg=MR count should have increased from ${before_mr} to >= ${expected}, got ${after_mr}

    Should Not Be Equal    ${after_ts}    ${before_ts}    msg=updated_at should have changed

    Log    Incremental OK: MRs ${before_mr} -> ${after_mr}

Test 4: Empty Namespace Returns Pending State
    [Documentation]    A namespace with no indexed data should report pending
    [Tags]    state
    ${empty_name}=    Set Variable    idx-empty-${RUN_ID}
    ${empty_id}=    Create Top Level Group    ${empty_name}    ${empty_name}
    Enable Namespace For KG    ${empty_id}

    ${tp}=    Set Variable    ${ORG_ID}/${empty_id}/
    ${resp}=    Get Indexing Status    ${tp}
    Should Be Equal As Integers    ${resp.status_code}    200

    ${body}=    Set Variable    ${resp.json()}
    Should Be Equal    ${body['state']}    pending
    Should Not Be True    ${body['initial_backfill_done']}
    Should Be True    ${body['stale']}

    Log    Empty namespace correctly returns pending

Test 5: Timestamp Format Is ISO 8601
    [Tags]    format
    [Setup]    Variable Should Exist    ${TRAVERSAL_PATH}

    ${resp}=    Get Indexing Status    ${TRAVERSAL_PATH}
    Should Be Equal As Integers    ${resp.status_code}    200

    ${ts}=    Set Variable    ${resp.json()['updated_at']}
    Should Not Be Empty    ${ts}
    Should Match Regexp    ${ts}    ^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}

Test 6: Edge Counts Are Non-Empty
    [Tags]    edges
    [Setup]    Variable Should Exist    ${TRAVERSAL_PATH}

    ${resp}=    Get Indexing Status    ${TRAVERSAL_PATH}
    Should Be Equal As Integers    ${resp.status_code}    200

    ${edges}=    Set Variable    ${resp.json()['edge_counts']}
    ${keys}=    Get Dictionary Keys    ${edges}
    Should Not Be Empty    ${keys}    msg=Expected at least one edge type

Test 7: SDLC Progress Metadata Is Populated
    [Tags]    metadata
    [Setup]    Variable Should Exist    ${TRAVERSAL_PATH}

    ${resp}=    Get Indexing Status    ${TRAVERSAL_PATH}
    Should Be Equal As Integers    ${resp.status_code}    200

    ${sdlc}=    Set Variable    ${resp.json()['sdlc']}
    Should Not Be Equal    ${sdlc}    ${None}

    ${completed}=    Set Variable    ${sdlc['last_completed_at']}
    Should Not Be Empty    ${completed}

    ${duration}=    Set Variable    ${sdlc['last_duration_ms']}
    Should Be True    ${duration} > 0

Test 8: Count Accuracy vs ClickHouse FINAL
    [Tags]    accuracy
    [Setup]    Variable Should Exist    ${TRAVERSAL_PATH}

    ${kv_resp}=    Get Indexing Status    ${TRAVERSAL_PATH}
    Should Be Equal As Integers    ${kv_resp.status_code}    200
    ${kv_projects}=    Get Domain Item Count    ${kv_resp}    core    Project

    ${ch_result}=    ClickHouse Query
    ...    SELECT count() FROM gl_project FINAL WHERE startsWith(traversal_path, '${TRAVERSAL_PATH}') AND _deleted = 0
    ${ch_count}=    Strip String    ${ch_result}
    ${ch_count}=    Convert To Integer    ${ch_count}

    ${lower}=    Evaluate    ${ch_count} * 0.95
    ${upper}=    Evaluate    ${ch_count} * 1.05 + 1
    Should Be True    ${kv_projects} >= ${lower} and ${kv_projects} <= ${upper}
    ...    msg=KV count ${kv_projects} should be within 5%% of FINAL count ${ch_count}

    Log    Accuracy OK: KV=${kv_projects} FINAL=${ch_count}
