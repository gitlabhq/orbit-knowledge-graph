*** Settings ***
Documentation       Opt-in end-to-end coverage for Rails-published role-tagged
...                 traversal paths. These tests require a compatible Rails
...                 monolith and real PG -> Siphon -> NATS -> ClickHouse -> GKG
...                 indexing, so the default Robot runner excludes their tag.

Library             Collections
Library             OperatingSystem
Library             Process
Resource            gitlab.resource
Resource            orbit.resource

Suite Setup         Run Keywords    Require Role Scoped Authz E2E    AND    Bootstrap E2E Credentials    AND    Seed Role Scoped Authz Fixtures


*** Variables ***
${ROLE_SCOPED_AUTHZ_E2E}            %{ROLE_SCOPED_AUTHZ_E2E=false}
${RAILS_RUNNER_CMD}                 %{RAILS_RUNNER_CMD=}
${ROLE_SCOPED_AUTHZ_SEED}           %{ROLE_SCOPED_AUTHZ_SEED=/fixtures/rails/role_scoped_authz_seed.rb}
${ROLE_SCOPED_AUTHZ_TOUCH}          %{ROLE_SCOPED_AUTHZ_TOUCH=/fixtures/rails/role_scoped_authz_touch.rb}


*** Test Cases ***
Reporter Path Cannot Infer Vulnerability Aggregates
    [Documentation]    The same user has Reporter on one KG-enabled group and
    ...                Security Manager on another. Aggregating Vulnerability by
    ...                Project returns only the Security Manager path.
    [Tags]    requires_rails_kg_authz    authz    security
    Wait For Seeded Vulnerabilities Indexed

    ${resp}=    Query Vulnerability Counts As Victim
    Response Has Project Count    ${resp}    ${SECURITY_PROJECT_ID}    1
    Response Does Not Have Project    ${resp}    ${REPORTER_PROJECT_ID}

Reporter Path Cannot Search Vulnerability Directly
    [Documentation]    Direct Vulnerability search is also scoped by per-path
    ...                access_levels, so the Reporter-path row is absent while
    ...                the Security Manager-path row remains visible.
    [Tags]    requires_rails_kg_authz    authz    security
    Wait For Seeded Vulnerabilities Indexed

    ${reporter_filters}=    Create Dictionary    id=${REPORTER_VULNERABILITY_ID}
    ${reporter_query}=    Create Dictionary    query_type=search
    ...    node=${{{"id": "v", "entity": "Vulnerability", "filters": $reporter_filters}}}
    ${reporter_resp}=    Orbit Query With Token    ${reporter_query}    ${ROLE_AUTHZ_VICTIM_PAT}
    Should Be Equal As Integers    ${reporter_resp["row_count"]}    0

    ${security_filters}=    Create Dictionary    id=${SECURITY_VULNERABILITY_ID}
    ${security_query}=    Create Dictionary    query_type=search
    ...    node=${{{"id": "v", "entity": "Vulnerability", "filters": $security_filters}}}
    ${security_resp}=    Orbit Query With Token    ${security_query}    ${ROLE_AUTHZ_VICTIM_PAT}
    Should Be True    ${security_resp["row_count"]} >= 1


*** Keywords ***
Require Role Scoped Authz E2E
    Skip If    '${ROLE_SCOPED_AUTHZ_E2E}' != 'true'
    ...    Set ROLE_SCOPED_AUTHZ_E2E=true and RAILS_RUNNER_CMD to run this opt-in suite.
    Should Not Be Empty    ${RAILS_RUNNER_CMD}
    ...    RAILS_RUNNER_CMD must point to a compatible Rails runner command.

Seed Role Scoped Authz Fixtures
    ${seed_exists}=    Run Keyword And Return Status    File Should Exist    ${ROLE_SCOPED_AUTHZ_SEED}
    IF    not ${seed_exists}
        ${local_seed}=    Normalize Path    ${CURDIR}/../fixtures/rails/role_scoped_authz_seed.rb
        Set Suite Variable    ${ROLE_SCOPED_AUTHZ_SEED}    ${local_seed}
    END
    File Should Exist    ${ROLE_SCOPED_AUTHZ_SEED}

    ${result}=    Run Process    bash    -lc    ${RAILS_RUNNER_CMD} ${ROLE_SCOPED_AUTHZ_SEED}
    ...    stdout=PIPE    stderr=STDOUT
    Should Be Equal As Integers    ${result.rc}    0    ${result.stdout}
    ${fixture}=    Parse Fixture Json    ${result.stdout}

    Set Suite Variable    ${ROLE_AUTHZ_VICTIM_PAT}    ${fixture["token"]}
    Set Suite Variable    ${REPORTER_PROJECT_ID}    ${fixture["reporter_project_id"]}
    Set Suite Variable    ${SECURITY_PROJECT_ID}    ${fixture["security_project_id"]}
    Set Suite Variable    ${REPORTER_VULNERABILITY_ID}    ${fixture["reporter_vulnerability_id"]}
    Set Suite Variable    ${SECURITY_VULNERABILITY_ID}    ${fixture["security_vulnerability_id"]}

    Wait For Node Indexed    Project    ${REPORTER_PROJECT_ID}    timeout=300s
    Wait For Node Indexed    Project    ${SECURITY_PROJECT_ID}    timeout=300s
    Touch Seeded Vulnerabilities

Parse Fixture Json
    [Arguments]    ${output}
    ${line}=    Evaluate    next(line for line in $output.splitlines() if line.startswith("ROLE_SCOPED_AUTHZ_FIXTURE_JSON="))
    ${payload}=    Evaluate    $line.split("=", 1)[1]
    ${data}=    Evaluate    json.loads($payload)    modules=json
    RETURN    ${data}

Touch Seeded Vulnerabilities
    ${touch_exists}=    Run Keyword And Return Status    File Should Exist    ${ROLE_SCOPED_AUTHZ_TOUCH}
    IF    not ${touch_exists}
        ${local_touch}=    Normalize Path    ${CURDIR}/../fixtures/rails/role_scoped_authz_touch.rb
        Set Suite Variable    ${ROLE_SCOPED_AUTHZ_TOUCH}    ${local_touch}
    END
    File Should Exist    ${ROLE_SCOPED_AUTHZ_TOUCH}

    ${ids}=    Set Variable    ${REPORTER_VULNERABILITY_ID},${SECURITY_VULNERABILITY_ID}
    ${result}=    Run Process    bash    -lc    VULNERABILITY_IDS=${ids} ${RAILS_RUNNER_CMD} ${ROLE_SCOPED_AUTHZ_TOUCH}
    ...    stdout=PIPE    stderr=STDOUT
    Should Be Equal As Integers    ${result.rc}    0    ${result.stdout}

Wait For Seeded Vulnerabilities Indexed
    Wait For Node Indexed    Vulnerability    ${REPORTER_VULNERABILITY_ID}    timeout=300s
    Wait For Node Indexed    Vulnerability    ${SECURITY_VULNERABILITY_ID}    timeout=300s

Query Vulnerability Counts As Victim
    ${query}=    Evaluate    {"query_type": "aggregation", "nodes": [{"id": "p", "entity": "Project", "columns": ["name"]}, {"id": "v", "entity": "Vulnerability"}], "relationships": [{"type": "IN_PROJECT", "from": "v", "to": "p"}], "aggregations": [{"function": "count", "target": "v", "group_by": "p", "alias": "vuln_count"}], "limit": 20}
    ${resp}=    Orbit Query With Token    ${query}    ${ROLE_AUTHZ_VICTIM_PAT}
    RETURN    ${resp}

Response Has Project Count
    [Arguments]    ${resp}    ${project_id}    ${expected}
    ${found}=    Evaluate    [node for node in $resp["result"]["nodes"] if int(node["id"]) == int($project_id)]
    Should Not Be Empty    ${found}
    Should Be Equal As Integers    ${found[0]["vuln_count"]}    ${expected}

Response Does Not Have Project
    [Arguments]    ${resp}    ${project_id}
    ${found}=    Evaluate    any(int(node["id"]) == int($project_id) for node in $resp["result"]["nodes"])
    Should Not Be True    ${found}
