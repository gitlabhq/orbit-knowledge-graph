*** Settings ***
Documentation    C-series: code indexing pipeline. Creates a project, pushes
...              a commit via the GitLab API (creates file in default branch),
...              waits for code indexing to land, then verifies counts.
Resource         ../lib/common.resource
Suite Setup      Prepare Code Fixtures
Suite Teardown   Teardown Code Fixtures

*** Variables ***
${WAIT_IDLE_SECS}       %{WAIT_IDLE_SECS=300}
${WAIT_CODE_SECS}       %{WAIT_CODE_SECS=180}
${TOP_ID}               ${None}
${PROJECT_ID}           ${None}
${PROJECT_PATH}         ${None}

*** Keywords ***
Prepare Code Fixtures
    ${suffix}=    Random Suffix
    Set Suite Variable    ${SUFFIX}    ${suffix}
    ${top}=    Create Group    c-top-${suffix}    c-top-${suffix}
    ${proj}=    Create Project    c-${suffix}    ${top["id"]}
    Set Suite Variable    ${TOP_ID}        ${top["id"]}
    Set Suite Variable    ${PROJECT_ID}    ${proj["id"]}
    Set Suite Variable    ${PROJECT_PATH}  ${proj["path_with_namespace"]}
    Enable Knowledge Graph    ${TOP_ID}
    Wait For Idle    ${TOP_ID}    ${WAIT_IDLE_SECS}s

Teardown Code Fixtures
    IF    $TOP_ID is not None
        Delete Group    ${TOP_ID}
    END

Commit File Via API
    [Arguments]    ${project_id}    ${branch}    ${path}    ${content}    ${message}
    ${headers}=    Auth Headers
    ${body}=    Create Dictionary    branch=${branch}    content=${content}    commit_message=${message}
    ${resp}=    POST
    ...    ${GITLAB_URL}/api/v4/projects/${project_id}/repository/files/${path}
    ...    headers=${headers}    json=${body}    expected_status=any    verify=${VERIFY_SSL}
    Should Be True    ${resp.status_code} in [200, 201]
    ...    commit failed ${resp.status_code}: ${resp.text}

Wait For Code Entities
    [Arguments]    ${project_id}    ${entity}=File    ${timeout}=${WAIT_CODE_SECS}s
    Wait Until Keyword Succeeds    ${timeout}    10s
    ...    Verify Project Has Code Entity    ${project_id}    ${entity}

Verify Project Has Code Entity
    [Arguments]    ${project_id}    ${entity}
    ${resp}=    Graph Status By Project    ${project_id}    200
    ${body}=    Set Variable    ${resp.json()}
    FOR    ${domain}    IN    @{body["domains"]}
        IF    "${domain["name"]}" == "source_code"
            FOR    ${item}    IN    @{domain["items"]}
                IF    "${item["name"]}" == "${entity}" and ${item["count"]} >= 1
                    RETURN
                END
            END
        END
    END
    Fail    project ${project_id}: ${entity} count still 0

Get Source Code Count
    [Arguments]    ${project_id}    ${entity}
    ${resp}=    Graph Status By Project    ${project_id}    200
    FOR    ${domain}    IN    @{resp.json()["domains"]}
        IF    "${domain["name"]}" == "source_code"
            FOR    ${item}    IN    @{domain["items"]}
                IF    "${item["name"]}" == "${entity}"
                    RETURN    ${item["count"]}
                END
            END
        END
    END
    RETURN    ${0}

*** Test Cases ***
C1 First Commit Produces Code Entities
    [Documentation]    Commit a Ruby file via the GitLab Files API, wait for
    ...                the code indexing pipeline to land, assert that the
    ...                source_code.File entity count advances from 0 to >= 1.
    ${content}=    Catenate    SEPARATOR=\n
    ...    class Hello
    ...    ${SPACE*2}def greet
    ...    ${SPACE*4}puts "hi"
    ...    ${SPACE*2}end
    ...    end
    ...    ${EMPTY}
    ${before}=    Get Source Code Count    ${PROJECT_ID}    File
    Log    File count before commit: ${before}
    Commit File Via API    ${PROJECT_ID}    main    hello%2Erb    ${content}    c1-first-commit
    Wait For Code Entities    ${PROJECT_ID}    File
    ${after}=    Get Source Code Count    ${PROJECT_ID}    File
    Should Be True    ${after} >= 1    expected File >= 1, got ${after}
    Log    File count after commit: ${after}

    # Definition entities should also appear for a Ruby file with a class + method.
    Wait For Code Entities    ${PROJECT_ID}    Definition

C2 Projects Indexed Advances After First Commit
    [Documentation]    After code pipeline runs once, code.projects_indexed >= 1.
    ${resp}=    Graph Status By Project    ${PROJECT_ID}    200
    Should Be True    ${resp.json()["code"]["projects_indexed"]} >= 1
    ...    projects_indexed=${resp.json()["code"]["projects_indexed"]} after C1

C3 Projects Total Tracks Namespace Projects
    [Documentation]    Create one more project; projects_total must advance.
    ${before}=    Graph Status By Namespace    ${TOP_ID}    200
    ${t_before}=    Set Variable    ${before.json()["code"]["projects_total"]}
    ${suffix}=    Random Suffix
    Create Project    c3-${suffix}    ${TOP_ID}
    Wait Until Keyword Succeeds    ${WAIT_IDLE_SECS}s    10s
    ...    Projects Total At Least    ${TOP_ID}    ${t_before + 1}

C4 Code Indexing Produces Edge Counts
    [Documentation]    After first code index, edge_counts for the project
    ...                scope should include at least one code relation
    ...                (e.g. CONTAINS).
    ${resp}=    Graph Status By Project    ${PROJECT_ID}    200
    ${edges}=    Set Variable    ${resp.json()["edge_counts"]}
    ${n}=    Get Length    ${edges}
    Should Be True    ${n} >= 1    expected >=1 edge kind, got ${edges}

C5 Subsequent Commit Refreshes Last Indexed At
    ${before}=    Graph Status By Project    ${PROJECT_ID}    200
    ${t_before}=    Set Variable    ${before.json()["code"]["last_indexed_at"]}
    ${content}=    Set Variable    puts "world"\n
    Commit File Via API    ${PROJECT_ID}    main    world%2Erb    ${content}    c5-second-commit
    Wait Until Keyword Succeeds    ${WAIT_CODE_SECS}s    10s
    ...    Last Indexed At Advanced    ${PROJECT_ID}    ${t_before}

*** Keywords ***
Projects Total At Least
    [Arguments]    ${ns_id}    ${n}
    ${r}=    Graph Status By Namespace    ${ns_id}    200
    Should Be True    ${r.json()["code"]["projects_total"]} >= ${n}
    ...    projects_total=${r.json()["code"]["projects_total"]} (want >= ${n})

Last Indexed At Advanced
    [Arguments]    ${project_id}    ${prev}
    ${r}=    Graph Status By Project    ${project_id}    200
    ${cur}=    Set Variable    ${r.json()["code"]["last_indexed_at"]}
    Should Not Be Empty    ${cur}
    IF    "${prev}" == ""
        RETURN
    END
    Assert Timestamp Non Decreasing    ${prev}    ${cur}    code.last_indexed_at
    Should Not Be Equal    ${prev}    ${cur}
    ...    last_indexed_at did not advance after commit (prev=${prev} cur=${cur})
