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

Suite Setup         Run Keywords    Require Role Scoped Authz E2E
...                 AND    Bootstrap E2E Credentials
...                 AND    Enable Role Scoped Authz Feature Flags
...                 AND    Load Existing Role Scoped Authz Fixture


*** Variables ***
${ROLE_SCOPED_AUTHZ_E2E}            %{ROLE_SCOPED_AUTHZ_E2E=false}
${GRAPH_CLICKHOUSE_URL}             %{GRAPH_CLICKHOUSE_URL=http://127.0.0.1:8123}
${GRAPH_CLICKHOUSE_DATABASE}        %{GRAPH_CLICKHOUSE_DATABASE=gkg-development}
${DATALAKE_CLICKHOUSE_DATABASE}     %{DATALAKE_CLICKHOUSE_DATABASE=gitlab_clickhouse_development}
${ROLE_AUTHZ_VICTIM_PAT}            %{ROLE_AUTHZ_VICTIM_PAT=}
${REPORTER_PROJECT_ID}              %{REPORTER_PROJECT_ID=}
${SECURITY_PROJECT_ID}              %{SECURITY_PROJECT_ID=}
${DEVELOPER_PROJECT_ID}             %{DEVELOPER_PROJECT_ID=}
${MAINTAINER_PROJECT_ID}            %{MAINTAINER_PROJECT_ID=}
${REPORTER_VULNERABILITY_ID}        %{REPORTER_VULNERABILITY_ID=}
${SECURITY_VULNERABILITY_ID}        %{SECURITY_VULNERABILITY_ID=}
${DEVELOPER_VULNERABILITY_ID}       %{DEVELOPER_VULNERABILITY_ID=}
${MAINTAINER_VULNERABILITY_ID}      %{MAINTAINER_VULNERABILITY_ID=}
${REPORTER_VULNERABILITY_TITLE}     %{REPORTER_VULNERABILITY_TITLE=}
${SECURITY_VULNERABILITY_TITLE}     %{SECURITY_VULNERABILITY_TITLE=}
${DEVELOPER_VULNERABILITY_TITLE}    %{DEVELOPER_VULNERABILITY_TITLE=}
${MAINTAINER_VULNERABILITY_TITLE}   %{MAINTAINER_VULNERABILITY_TITLE=}
${REPORTER_VULNERABILITY_CREATED_AT}     %{REPORTER_VULNERABILITY_CREATED_AT=}
${SECURITY_VULNERABILITY_CREATED_AT}     %{SECURITY_VULNERABILITY_CREATED_AT=}
${DEVELOPER_VULNERABILITY_CREATED_AT}    %{DEVELOPER_VULNERABILITY_CREATED_AT=}
${MAINTAINER_VULNERABILITY_CREATED_AT}   %{MAINTAINER_VULNERABILITY_CREATED_AT=}
${NESTED_REPORTER_PROJECT_ID}              %{NESTED_REPORTER_PROJECT_ID=}
${NESTED_DEVELOPER_PROJECT_ID}             %{NESTED_DEVELOPER_PROJECT_ID=}
${NESTED_REPORTER_VULNERABILITY_ID}        %{NESTED_REPORTER_VULNERABILITY_ID=}
${NESTED_DEVELOPER_VULNERABILITY_ID}       %{NESTED_DEVELOPER_VULNERABILITY_ID=}
${NESTED_REPORTER_VULNERABILITY_TITLE}     %{NESTED_REPORTER_VULNERABILITY_TITLE=}
${NESTED_DEVELOPER_VULNERABILITY_TITLE}    %{NESTED_DEVELOPER_VULNERABILITY_TITLE=}
${NESTED_REPORTER_VULNERABILITY_CREATED_AT}     %{NESTED_REPORTER_VULNERABILITY_CREATED_AT=}
${NESTED_DEVELOPER_VULNERABILITY_CREATED_AT}    %{NESTED_DEVELOPER_VULNERABILITY_CREATED_AT=}


*** Test Cases ***
Reporter Path Cannot Infer Vulnerability Aggregates
    [Documentation]    The same user has Reporter on one KG-enabled group and
    ...                a security-authorized role on another. Aggregating
    ...                Vulnerability by Project returns only the authorized path.
    [Tags]    requires_rails_kg_authz    authz    security

    ${resp}=    Query Vulnerability Counts As Victim
    Response Has Project Count    ${resp}    ${SECURITY_PROJECT_ID}    1
    Response Does Not Have Project    ${resp}    ${REPORTER_PROJECT_ID}
    Response Does Not Have Project    ${resp}    ${NESTED_REPORTER_PROJECT_ID}

Reporter Project Vulnerability Oracle Filters Are Neutralized
    [Documentation]    Replays the original issue's aggregation-oracle matrix
    ...                against the Reporter-only path: total count, enum
    ...                fields, ID exact/range predicates, title equality, and
    ...                timestamp predicates must all return no Project row.
    [Tags]    requires_rails_kg_authz    authz    security

    ${filters}=    Reporter Vulnerability Oracle Filters
    FOR    ${filter}    IN    @{filters}
        ${resp}=    Query Vulnerability Counts For Project As Victim    ${REPORTER_PROJECT_ID}    ${filter}
        Response Does Not Have Project    ${resp}    ${REPORTER_PROJECT_ID}
    END

Authorized Project Vulnerability Oracle Filters Remain Authorized
    [Documentation]    The same query shapes must still work for a path where
    ...                the caller has Security Manager or Developer access,
    ...                proving the fix narrows only below-threshold paths.
    [Tags]    requires_rails_kg_authz    authz    security

    ${filters}=    Security Manager Vulnerability Oracle Filters
    FOR    ${filter}    IN    @{filters}
        ${resp}=    Query Vulnerability Counts For Project As Victim    ${SECURITY_PROJECT_ID}    ${filter}
        Response Has Project Count    ${resp}    ${SECURITY_PROJECT_ID}    1
    END

Developer And Maintainer Vulnerability Paths Remain Authorized
    [Documentation]    Developer and Maintainer roles are above the security
    ...                threshold and must still be able to search and aggregate
    ...                vulnerability rows on their own traversal paths.
    [Tags]    requires_rails_kg_authz    authz    security

    Authorized Vulnerability Path Is Visible    ${DEVELOPER_PROJECT_ID}    ${DEVELOPER_VULNERABILITY_ID}
    Authorized Vulnerability Path Is Visible    ${MAINTAINER_PROJECT_ID}    ${MAINTAINER_VULNERABILITY_ID}

Nested Reporter Subgroup Vulnerability Oracles Are Neutralized
    [Documentation]    A Reporter-only subgroup project remains hidden even
    ...                when the vulnerable row is below a nested traversal path.
    [Tags]    requires_rails_kg_authz    authz    security

    ${filters}=    Nested Reporter Vulnerability Oracle Filters
    FOR    ${filter}    IN    @{filters}
        ${resp}=    Query Vulnerability Counts For Project As Victim    ${NESTED_REPORTER_PROJECT_ID}    ${filter}
        Response Does Not Have Project    ${resp}    ${NESTED_REPORTER_PROJECT_ID}
    END

    Direct Vulnerability Search Is Hidden    ${NESTED_REPORTER_VULNERABILITY_ID}

Nested Child Developer Override Remains Authorized
    [Documentation]    Developer access granted on a child subgroup under a
    ...                Reporter parent still authorizes the child vulnerability
    ...                path, proving nested access arrays are evaluated per path.
    [Tags]    requires_rails_kg_authz    authz    security

    Authorized Vulnerability Path Is Visible    ${NESTED_DEVELOPER_PROJECT_ID}    ${NESTED_DEVELOPER_VULNERABILITY_ID}

Reporter Path Cannot Search Vulnerability Directly
    [Documentation]    Direct Vulnerability search is also scoped by per-path
    ...                access_levels, so the Reporter-path row is absent.
    [Tags]    requires_rails_kg_authz    authz    security

    Direct Vulnerability Search Is Hidden    ${REPORTER_VULNERABILITY_ID}


*** Keywords ***
Require Role Scoped Authz E2E
    Skip If    '${ROLE_SCOPED_AUTHZ_E2E}' != 'true'
    ...    Set ROLE_SCOPED_AUTHZ_E2E=true and provide existing fixture IDs through environment variables.

Load Existing Role Scoped Authz Fixture
    Should Not Be Empty    ${ROLE_AUTHZ_VICTIM_PAT}
    ...    ROLE_AUTHZ_VICTIM_PAT must be a PAT for the fixture user.
    Should Not Be Empty    ${REPORTER_PROJECT_ID}
    Should Not Be Empty    ${SECURITY_PROJECT_ID}
    Should Not Be Empty    ${DEVELOPER_PROJECT_ID}
    Should Not Be Empty    ${MAINTAINER_PROJECT_ID}
    Should Not Be Empty    ${REPORTER_VULNERABILITY_ID}
    Should Not Be Empty    ${SECURITY_VULNERABILITY_ID}
    Should Not Be Empty    ${DEVELOPER_VULNERABILITY_ID}
    Should Not Be Empty    ${MAINTAINER_VULNERABILITY_ID}
    Should Not Be Empty    ${REPORTER_VULNERABILITY_TITLE}
    Should Not Be Empty    ${SECURITY_VULNERABILITY_TITLE}
    Should Not Be Empty    ${DEVELOPER_VULNERABILITY_TITLE}
    Should Not Be Empty    ${MAINTAINER_VULNERABILITY_TITLE}
    Should Not Be Empty    ${REPORTER_VULNERABILITY_CREATED_AT}
    Should Not Be Empty    ${SECURITY_VULNERABILITY_CREATED_AT}
    Should Not Be Empty    ${DEVELOPER_VULNERABILITY_CREATED_AT}
    Should Not Be Empty    ${MAINTAINER_VULNERABILITY_CREATED_AT}
    Should Not Be Empty    ${NESTED_REPORTER_PROJECT_ID}
    Should Not Be Empty    ${NESTED_DEVELOPER_PROJECT_ID}
    Should Not Be Empty    ${NESTED_REPORTER_VULNERABILITY_ID}
    Should Not Be Empty    ${NESTED_DEVELOPER_VULNERABILITY_ID}
    Should Not Be Empty    ${NESTED_REPORTER_VULNERABILITY_TITLE}
    Should Not Be Empty    ${NESTED_DEVELOPER_VULNERABILITY_TITLE}
    Should Not Be Empty    ${NESTED_REPORTER_VULNERABILITY_CREATED_AT}
    Should Not Be Empty    ${NESTED_DEVELOPER_VULNERABILITY_CREATED_AT}

Enable Role Scoped Authz Feature Flags
    Enable Feature Flag    knowledge_graph_infra
    Enable Feature Flag    knowledge_graph
    Wait Until Keyword Succeeds    30s    3s    Feature Flag Is Enabled    knowledge_graph_infra
    Wait Until Keyword Succeeds    30s    3s    Feature Flag Is Enabled    knowledge_graph

Graph Has Live Vulnerability
    [Arguments]    ${vulnerability_id}
    ${sql}=    Catenate
    ...    SELECT count() FROM (
    ...    SELECT id FROM v6_gl_vulnerability WHERE id = ${vulnerability_id} GROUP BY id
    ...    HAVING isNotNull(argMaxIfOrNull(id, _version, _deleted = false))
    ...    ) FORMAT TabSeparated
    ${resp}=    POST    url=${GRAPH_CLICKHOUSE_URL}/?database=${GRAPH_CLICKHOUSE_DATABASE}
    ...    data=${sql}    expected_status=200
    Should Be Equal As Integers    ${resp.text.strip()}    1

Graph Has Live Project
    [Arguments]    ${project_id}
    ${sql}=    Catenate
    ...    SELECT count() FROM (
    ...    SELECT id FROM v6_gl_project WHERE id = ${project_id} GROUP BY id
    ...    HAVING isNotNull(argMaxIfOrNull(id, _version, _deleted = false))
    ...    ) FORMAT TabSeparated
    ${resp}=    POST    url=${GRAPH_CLICKHOUSE_URL}/?database=${GRAPH_CLICKHOUSE_DATABASE}
    ...    data=${sql}    expected_status=200
    Should Be Equal As Integers    ${resp.text.strip()}    1

Datalake Has Project Route
    [Arguments]    ${project_id}
    ${sql}=    Catenate
    ...    SELECT count() FROM siphon_routes
    ...    WHERE source_id = ${project_id} AND source_type = 'Project' AND _siphon_deleted = false
    ...    FORMAT TabSeparated
    ${resp}=    POST    url=${GRAPH_CLICKHOUSE_URL}/?database=${DATALAKE_CLICKHOUSE_DATABASE}
    ...    data=${sql}    expected_status=200
    ${count}=    Convert To Integer    ${resp.text.strip()}
    Should Be True    ${count} >= 1

Query Vulnerability Counts As Victim
    ${query}=    Evaluate    {"query_type": "aggregation", "nodes": [{"id": "p", "entity": "Project", "columns": ["name"]}, {"id": "v", "entity": "Vulnerability"}], "relationships": [{"type": "IN_PROJECT", "from": "v", "to": "p"}], "aggregations": [{"function": "count", "target": "v", "group_by": "p", "alias": "vuln_count"}], "limit": 20}
    ${resp}=    Orbit Query With Token    ${query}    ${ROLE_AUTHZ_VICTIM_PAT}
    RETURN    ${resp}

Query Vulnerability Counts For Project As Victim
    [Arguments]    ${project_id}    ${vulnerability_filters}=${None}
    ${query}=    Evaluate    {"query_type": "aggregation", "nodes": [{"id": "p", "entity": "Project", "columns": ["name"], "node_ids": [int($project_id)]}, {"id": "v", "entity": "Vulnerability", "filters": $vulnerability_filters or {}}], "relationships": [{"type": "IN_PROJECT", "from": "v", "to": "p"}], "aggregations": [{"function": "count", "target": "v", "group_by": "p", "alias": "vuln_count"}], "limit": 10}
    ${resp}=    Orbit Query With Token    ${query}    ${ROLE_AUTHZ_VICTIM_PAT}
    RETURN    ${resp}

Reporter Vulnerability Oracle Filters
    ${created_at}=    Normalize ClickHouse Timestamp    ${REPORTER_VULNERABILITY_CREATED_AT}
    ${filters}=    Evaluate    [None, {"severity": "critical"}, {"state": "detected"}, {"report_type": "generic"}, {"id": int($REPORTER_VULNERABILITY_ID)}, {"id": {"op": "lte", "value": int($REPORTER_VULNERABILITY_ID)}}, {"title": $REPORTER_VULNERABILITY_TITLE}, {"created_at": {"op": "lte", "value": $created_at}}]
    RETURN    ${filters}

Nested Reporter Vulnerability Oracle Filters
    ${created_at}=    Normalize ClickHouse Timestamp    ${NESTED_REPORTER_VULNERABILITY_CREATED_AT}
    ${filters}=    Evaluate    [None, {"severity": "critical"}, {"state": "detected"}, {"report_type": "generic"}, {"id": int($NESTED_REPORTER_VULNERABILITY_ID)}, {"id": {"op": "lte", "value": int($NESTED_REPORTER_VULNERABILITY_ID)}}, {"title": $NESTED_REPORTER_VULNERABILITY_TITLE}, {"created_at": {"op": "lte", "value": $created_at}}]
    RETURN    ${filters}

Security Manager Vulnerability Oracle Filters
    ${created_at}=    Normalize ClickHouse Timestamp    ${SECURITY_VULNERABILITY_CREATED_AT}
    ${filters}=    Evaluate    [None, {"severity": "high"}, {"state": "detected"}, {"report_type": "generic"}, {"id": int($SECURITY_VULNERABILITY_ID)}, {"id": {"op": "lte", "value": int($SECURITY_VULNERABILITY_ID)}}, {"title": $SECURITY_VULNERABILITY_TITLE}, {"created_at": {"op": "gte", "value": $created_at}}]
    RETURN    ${filters}

Normalize ClickHouse Timestamp
    [Arguments]    ${timestamp}
    ${normalized}=    Evaluate    $timestamp.replace("T", " ").replace("Z", "")
    RETURN    ${normalized}

Authorized Vulnerability Path Is Visible
    [Arguments]    ${project_id}    ${vulnerability_id}
    ${resp}=    Query Vulnerability Counts For Project As Victim    ${project_id}    ${None}
    Response Has Project Count    ${resp}    ${project_id}    1

Direct Vulnerability Search Is Hidden
    [Arguments]    ${vulnerability_id}
    ${id}=    Convert To Integer    ${vulnerability_id}
    ${filters}=    Create Dictionary    id=${id}
    ${query}=    Create Dictionary    query_type=search
    ...    node=${{{"id": "v", "entity": "Vulnerability", "filters": $filters}}}
    ${resp}=    Orbit Query With Token    ${query}    ${ROLE_AUTHZ_VICTIM_PAT}
    Should Be Equal As Integers    ${resp["row_count"]}    0

Response Has Project Count
    [Arguments]    ${resp}    ${project_id}    ${expected}
    ${pid}=    Convert To Integer    ${project_id}
    ${found}=    Evaluate    [node for node in $resp["result"]["nodes"] if int(node["id"]) == ${pid}]
    Should Not Be Empty    ${found}
    Should Be True    ${found[0]["vuln_count"]} >= ${expected}

Response Does Not Have Project
    [Arguments]    ${resp}    ${project_id}
    ${pid}=    Convert To Integer    ${project_id}
    ${found}=    Evaluate    any(int(node["id"]) == ${pid} for node in $resp["result"]["nodes"])
    Should Not Be True    ${found}
