*** Settings ***
Documentation       Phase 3: Verify mutations propagate through the full pipeline.
...                 Tests that changes in Rails flow through Siphon CDC to ClickHouse
...                 and appear in the graph after re-indexing.

Resource            resources/common.robot

Suite Setup         Suite Init


*** Test Cases ***
Group Rename Propagates To Graph
    [Documentation]    Rename a group via API, verify the change appears in graph tables
    [Tags]    mutation    critical
    [Setup]    Variable Should Exist    ${TOP_GROUP_ID}

    # Get current group name
    ${before}=    GitLab API GET    /groups/${TOP_GROUP_ID}
    Should Be Equal As Integers    ${before.status_code}    200
    ${old_name}=    Set Variable    ${before.json()['name']}

    # Rename
    ${new_name}=    Set Variable    renamed-${RUN_ID}
    ${data}=    Create Dictionary    name=${new_name}
    ${resp}=    GitLab API PUT    /groups/${TOP_GROUP_ID}    ${data}
    Should Be Equal As Integers    ${resp.status_code}    200    msg=Failed to rename group: ${resp.text}

    # Wait for CDC to pick up the change
    Sleep    20

    # Check datalake
    ${result}=    ClickHouse Query
    ...    SELECT name FROM siphon_namespaces FINAL WHERE id = ${TOP_GROUP_ID}
    ...    ${DATALAKE_DB}
    ${dl_name}=    Strip String    ${result}
    Should Be Equal    ${dl_name}    ${new_name}
    ...    msg=Datalake should have new name '${new_name}', got '${dl_name}'

    # Wait for indexer to pick up the change
    Sleep    15

    # Check graph table
    ${graph_result}=    ClickHouse Query
    ...    SELECT name FROM gl_group FINAL WHERE id = ${TOP_GROUP_ID} AND _deleted = 0
    ${graph_name}=    Strip String    ${graph_result}
    Should Be Equal    ${graph_name}    ${new_name}
    ...    msg=Graph should have new name '${new_name}', got '${graph_name}'

    # Rename back
    ${restore}=    Create Dictionary    name=${old_name}
    GitLab API PUT    /groups/${TOP_GROUP_ID}    ${restore}

    Log    Mutation OK: group renamed ${old_name} -> ${new_name} -> ${old_name}

New Project Appears In Graph After Creation
    [Documentation]    Create a new project and verify it appears in graph tables
    [Tags]    mutation    critical
    [Setup]    Variable Should Exist    ${TOP_GROUP_ID}

    # Count projects before
    ${before}=    ClickHouse Query
    ...    SELECT count() FROM gl_project FINAL WHERE startsWith(traversal_path, '${TRAVERSAL_PATH}') AND _deleted = 0

    ${before_count}=    Strip String    ${before}
    ${before_count}=    Convert To Integer    ${before_count}

    # Create new project
    ${proj_name}=    Set Variable    mutation-test-${RUN_ID}
    ${proj_id}=    Create Project    ${proj_name}    ${TOP_GROUP_ID}

    # Wait for CDC + indexing
    Wait For CDC
    Sleep    20

    # Count projects after
    ${after}=    ClickHouse Query
    ...    SELECT count() FROM gl_project FINAL WHERE startsWith(traversal_path, '${TRAVERSAL_PATH}') AND _deleted = 0
    ${after_count}=    Strip String    ${after}
    ${after_count}=    Convert To Integer    ${after_count}

    Should Be True    ${after_count} > ${before_count}
    ...    msg=Project count should have increased from ${before_count}, got ${after_count}

    Log    Mutation OK: projects ${before_count} -> ${after_count}


*** Keywords ***
Suite Init
    Create API Session
    ${ts}=    Evaluate    str(int(time.time()))[-6:]    modules=time
    Set Suite Variable    ${RUN_ID}    ${ts}

    ${has_tp}=    Run Keyword And Return Status    Variable Should Exist    ${TOP_GROUP_ID}
    IF    not ${has_tp}
        ${result}=    Run Process    bash    -c
        ...    curl -sk "${GDK_URL}/api/v4/groups?search=gkg-e2e&private_token=$(cat %{HOME}/.gdk_token)" 2>/dev/null | python3 -c "import sys,json; groups=json.load(sys.stdin); print(groups[0]['id'] if groups else '')"
        ...    timeout=15
        ${group_id}=    Strip String    ${result.stdout}
        Should Not Be Empty    ${group_id}    msg=No gkg-e2e group found. Run 01_setup_and_index first.
        Set Suite Variable    ${TOP_GROUP_ID}    ${group_id}
        Set Suite Variable    ${ORG_ID}    1
        Set Suite Variable    ${TRAVERSAL_PATH}    1/${group_id}/
    END
