*** Settings ***
Documentation       End-to-end coverage for issue #347 — aggregation queries must enforce
...                 per-entity authorization on the target node, not only on the group_by
...                 node. Builds a single victim user with Reporter, Security Manager,
...                 Developer, Maintainer, and nested-subgroup memberships, plants one
...                 Vulnerability per project via the GraphQL `vulnerabilityCreate` mutation,
...                 and replays the original oracle matrix from each role's traversal path.

Library             Collections
Library             OperatingSystem
Library             Process
Resource            gitlab.resource
Resource            orbit.resource

Suite Setup         Build Role Scoped Authz Fixture


*** Test Cases ***
Reporter Path Cannot Infer Vulnerability Aggregates
    [Documentation]    The same user has Reporter on one KG-enabled group and Security Manager
    ...                on another. Aggregating Vulnerability by Project returns only the
    ...                authorized path.
    [Tags]    authz    security

    ${resp}=    Query Vulnerability Counts As Victim
    Response Has Project Count    ${resp}    ${SECURITY_PROJECT_ID}    1
    Response Does Not Have Project    ${resp}    ${REPORTER_PROJECT_ID}
    Response Does Not Have Project    ${resp}    ${NESTED_REPORTER_PROJECT_ID}

Reporter Project Vulnerability Oracle Filters Are Neutralized
    [Documentation]    Replays the original issue's aggregation-oracle matrix against the
    ...                Reporter-only path: total count, enum fields, ID exact/range predicates,
    ...                title equality, and timestamp predicates must all return no Project row.
    [Tags]    authz    security

    ${filters}=    Reporter Vulnerability Oracle Filters
    FOR    ${filter}    IN    @{filters}
        ${resp}=    Query Vulnerability Counts For Project As Victim    ${REPORTER_PROJECT_ID}    ${filter}
        Response Does Not Have Project    ${resp}    ${REPORTER_PROJECT_ID}
    END

Authorized Project Vulnerability Oracle Filters Remain Authorized
    [Documentation]    The same query shapes must still work for a path where the caller has
    ...                Security Manager access, proving the fix narrows only below-threshold paths.
    [Tags]    authz    security

    ${filters}=    Security Manager Vulnerability Oracle Filters
    FOR    ${filter}    IN    @{filters}
        ${resp}=    Query Vulnerability Counts For Project As Victim    ${SECURITY_PROJECT_ID}    ${filter}
        Response Has Project Count    ${resp}    ${SECURITY_PROJECT_ID}    1
    END

Developer And Maintainer Vulnerability Paths Remain Authorized
    [Documentation]    Developer and Maintainer roles are above the security threshold and must
    ...                still aggregate vulnerability rows on their own traversal paths.
    [Tags]    authz    security

    Authorized Vulnerability Path Is Visible    ${DEVELOPER_PROJECT_ID}
    Authorized Vulnerability Path Is Visible    ${MAINTAINER_PROJECT_ID}

Nested Reporter Subgroup Vulnerability Oracles Are Neutralized
    [Documentation]    A Reporter-only subgroup project remains hidden even when the vulnerable
    ...                row is below a nested traversal path.
    [Tags]    authz    security

    ${filters}=    Nested Reporter Vulnerability Oracle Filters
    FOR    ${filter}    IN    @{filters}
        ${resp}=    Query Vulnerability Counts For Project As Victim    ${NESTED_REPORTER_PROJECT_ID}    ${filter}
        Response Does Not Have Project    ${resp}    ${NESTED_REPORTER_PROJECT_ID}
    END

    Direct Vulnerability Search Is Hidden    ${NESTED_REPORTER_VULNERABILITY_ID}

Nested Child Developer Override Remains Authorized
    [Documentation]    Developer access granted on a child subgroup under a Reporter parent
    ...                still authorizes the child vulnerability path, proving nested access
    ...                arrays are evaluated per path.
    [Tags]    authz    security

    Authorized Vulnerability Path Is Visible    ${NESTED_DEVELOPER_PROJECT_ID}

Reporter Path Cannot Search Vulnerability Directly
    [Documentation]    Direct Vulnerability search is also scoped by per-path access_levels, so
    ...                the Reporter-path row is absent. Symmetry probe confirms the same shape
    ...                returns the Security-Manager-path row.
    [Tags]    authz    security

    Direct Vulnerability Search Is Hidden    ${REPORTER_VULNERABILITY_ID}
    Direct Vulnerability Search Is Visible    ${SECURITY_VULNERABILITY_ID}


*** Keywords ***
Build Role Scoped Authz Fixture
    [Documentation]    Idempotent suite-level seeding: bootstraps the e2e-bot, enables KG flags,
    ...                creates the role-scoped group/subgroup matrix, plants one Vulnerability per
    ...                project, mints a victim PAT, and waits for the indexer to surface every row.
    Bootstrap E2E Credentials
    Enable Role Scoped Authz Feature Flags
    ${suffix}=    Random Suffix
    Set Suite Variable    ${ROLE_AUTHZ_SUFFIX}    ${suffix}
    Provision Role Scoped Groups
    Provision Role Scoped Projects
    Provision Role Scoped Victim
    Provision Role Scoped Vulnerabilities
    Wait For Role Scoped Fixture Indexed

Enable Role Scoped Authz Feature Flags
    Enable Feature Flag    knowledge_graph_infra
    Enable Feature Flag    knowledge_graph
    Wait Until Keyword Succeeds    30s    3s    Feature Flag Is Enabled    knowledge_graph_infra
    Wait Until Keyword Succeeds    30s    3s    Feature Flag Is Enabled    knowledge_graph

Provision Role Scoped Groups
    ${reporter_group}=        Create Group       kg347-${ROLE_AUTHZ_SUFFIX}-reporter
    ${security_group}=        Create Group       kg347-${ROLE_AUTHZ_SUFFIX}-security
    ${developer_group}=       Create Group       kg347-${ROLE_AUTHZ_SUFFIX}-developer
    ${maintainer_group}=      Create Group       kg347-${ROLE_AUTHZ_SUFFIX}-maintainer
    ${nested_parent_group}=   Create Group       kg347-${ROLE_AUTHZ_SUFFIX}-nested-parent
    ${nested_reporter_group}=     Create Subgroup    ${nested_parent_group["id"]}
    ...    kg347-${ROLE_AUTHZ_SUFFIX}-nested-reporter
    ${nested_developer_group}=    Create Subgroup    ${nested_parent_group["id"]}
    ...    kg347-${ROLE_AUTHZ_SUFFIX}-nested-developer
    Enable Knowledge Graph    ${reporter_group["id"]}
    Enable Knowledge Graph    ${security_group["id"]}
    Enable Knowledge Graph    ${developer_group["id"]}
    Enable Knowledge Graph    ${maintainer_group["id"]}
    Enable Knowledge Graph    ${nested_parent_group["id"]}
    Set Suite Variable    ${REPORTER_GROUP}            ${reporter_group}
    Set Suite Variable    ${SECURITY_GROUP}            ${security_group}
    Set Suite Variable    ${DEVELOPER_GROUP}           ${developer_group}
    Set Suite Variable    ${MAINTAINER_GROUP}          ${maintainer_group}
    Set Suite Variable    ${NESTED_PARENT_GROUP}       ${nested_parent_group}
    Set Suite Variable    ${NESTED_REPORTER_GROUP}     ${nested_reporter_group}
    Set Suite Variable    ${NESTED_DEVELOPER_GROUP}    ${nested_developer_group}

Provision Role Scoped Projects
    ${reporter_project}=    Create Project
    ...    kg347-${ROLE_AUTHZ_SUFFIX}-reporter-project    ${REPORTER_GROUP["id"]}
    ${security_project}=    Create Project
    ...    kg347-${ROLE_AUTHZ_SUFFIX}-security-project    ${SECURITY_GROUP["id"]}
    ${developer_project}=    Create Project
    ...    kg347-${ROLE_AUTHZ_SUFFIX}-developer-project    ${DEVELOPER_GROUP["id"]}
    ${maintainer_project}=    Create Project
    ...    kg347-${ROLE_AUTHZ_SUFFIX}-maintainer-project    ${MAINTAINER_GROUP["id"]}
    ${nested_reporter_project}=    Create Project
    ...    kg347-${ROLE_AUTHZ_SUFFIX}-nested-reporter-project    ${NESTED_REPORTER_GROUP["id"]}
    ${nested_developer_project}=    Create Project
    ...    kg347-${ROLE_AUTHZ_SUFFIX}-nested-developer-project    ${NESTED_DEVELOPER_GROUP["id"]}
    Set Suite Variable    ${REPORTER_PROJECT_ID}             ${reporter_project["id"]}
    Set Suite Variable    ${SECURITY_PROJECT_ID}             ${security_project["id"]}
    Set Suite Variable    ${DEVELOPER_PROJECT_ID}            ${developer_project["id"]}
    Set Suite Variable    ${MAINTAINER_PROJECT_ID}           ${maintainer_project["id"]}
    Set Suite Variable    ${NESTED_REPORTER_PROJECT_ID}      ${nested_reporter_project["id"]}
    Set Suite Variable    ${NESTED_DEVELOPER_PROJECT_ID}     ${nested_developer_project["id"]}

Provision Role Scoped Victim
    [Documentation]    Mint a non-admin user, attach the role matrix that exercises the fix:
    ...                Reporter (below threshold) on one group, Security Manager (=25, the
    ...                threshold) on another, Developer/Maintainer (above threshold) on two more,
    ...                and a Reporter parent + Developer child to cover nested-subgroup paths.
    ${victim}=    Create User    kg347-${ROLE_AUTHZ_SUFFIX}-victim
    ${root_headers}=    Root Auth Headers
    ${victim_pat}=    Issue PAT For User    ${root_headers}    ${victim["id"]}
    Add Group Member    ${REPORTER_GROUP["id"]}             ${victim["id"]}    20
    Add Group Member    ${SECURITY_GROUP["id"]}             ${victim["id"]}    25
    Add Group Member    ${DEVELOPER_GROUP["id"]}            ${victim["id"]}    30
    Add Group Member    ${MAINTAINER_GROUP["id"]}           ${victim["id"]}    40
    Add Group Member    ${NESTED_PARENT_GROUP["id"]}        ${victim["id"]}    20
    Add Group Member    ${NESTED_DEVELOPER_GROUP["id"]}     ${victim["id"]}    30
    Set Suite Variable    ${ROLE_AUTHZ_VICTIM_PAT}    ${victim_pat}

Provision Role Scoped Vulnerabilities
    [Documentation]    `vulnerabilityCreate` always sets report_type=generic and stamps
    ...                created_at server-side; capture the returned timestamp for the oracle
    ...                filters. Reporter+nested-reporter paths get critical+sqli to make the
    ...                oracle distinguishable; the rest are high+xss.
    ${reporter_vuln}=    Create Vulnerability    ${REPORTER_PROJECT_ID}
    ...    kg347-${ROLE_AUTHZ_SUFFIX} reporter-only SQLi    severity=critical
    ${security_vuln}=    Create Vulnerability    ${SECURITY_PROJECT_ID}
    ...    kg347-${ROLE_AUTHZ_SUFFIX} security-manager XSS    severity=high
    ${developer_vuln}=    Create Vulnerability    ${DEVELOPER_PROJECT_ID}
    ...    kg347-${ROLE_AUTHZ_SUFFIX} developer XSS    severity=high
    ${maintainer_vuln}=    Create Vulnerability    ${MAINTAINER_PROJECT_ID}
    ...    kg347-${ROLE_AUTHZ_SUFFIX} maintainer XSS    severity=high
    ${nested_reporter_vuln}=    Create Vulnerability    ${NESTED_REPORTER_PROJECT_ID}
    ...    kg347-${ROLE_AUTHZ_SUFFIX} nested reporter-only SQLi    severity=critical
    ${nested_developer_vuln}=    Create Vulnerability    ${NESTED_DEVELOPER_PROJECT_ID}
    ...    kg347-${ROLE_AUTHZ_SUFFIX} nested developer XSS    severity=high
    Set Suite Variable    ${REPORTER_VULNERABILITY_ID}             ${reporter_vuln["id"]}
    Set Suite Variable    ${REPORTER_VULNERABILITY_TITLE}          ${reporter_vuln["title"]}
    Set Suite Variable    ${REPORTER_VULNERABILITY_CREATED_AT}     ${reporter_vuln["createdAt"]}
    Set Suite Variable    ${SECURITY_VULNERABILITY_ID}             ${security_vuln["id"]}
    Set Suite Variable    ${SECURITY_VULNERABILITY_TITLE}          ${security_vuln["title"]}
    Set Suite Variable    ${SECURITY_VULNERABILITY_CREATED_AT}     ${security_vuln["createdAt"]}
    Set Suite Variable    ${DEVELOPER_VULNERABILITY_ID}            ${developer_vuln["id"]}
    Set Suite Variable    ${MAINTAINER_VULNERABILITY_ID}           ${maintainer_vuln["id"]}
    Set Suite Variable    ${NESTED_REPORTER_VULNERABILITY_ID}      ${nested_reporter_vuln["id"]}
    Set Suite Variable    ${NESTED_REPORTER_VULNERABILITY_TITLE}   ${nested_reporter_vuln["title"]}
    Set Suite Variable    ${NESTED_REPORTER_VULNERABILITY_CREATED_AT}    ${nested_reporter_vuln["createdAt"]}
    Set Suite Variable    ${NESTED_DEVELOPER_VULNERABILITY_ID}     ${nested_developer_vuln["id"]}

Wait For Role Scoped Fixture Indexed
    [Documentation]    Block until every project and vulnerability the suite asserts on is
    ...                queryable via Orbit. Closes the race where deny tests would pass by
    ...                emptiness against an indexer that hadn't caught up yet.
    Start Indexing Budget    600
    Wait For Node Indexed Within Budget    Project    ${REPORTER_PROJECT_ID}
    Wait For Node Indexed Within Budget    Project    ${SECURITY_PROJECT_ID}
    Wait For Node Indexed Within Budget    Project    ${DEVELOPER_PROJECT_ID}
    Wait For Node Indexed Within Budget    Project    ${MAINTAINER_PROJECT_ID}
    Wait For Node Indexed Within Budget    Project    ${NESTED_REPORTER_PROJECT_ID}
    Wait For Node Indexed Within Budget    Project    ${NESTED_DEVELOPER_PROJECT_ID}
    Wait For Node Indexed Within Budget    Vulnerability    ${REPORTER_VULNERABILITY_ID}
    ...    ${REPORTER_VULNERABILITY_TITLE}    label_field=title
    Wait For Node Indexed Within Budget    Vulnerability    ${SECURITY_VULNERABILITY_ID}
    ...    ${SECURITY_VULNERABILITY_TITLE}    label_field=title
    Wait For Node Indexed Within Budget    Vulnerability    ${DEVELOPER_VULNERABILITY_ID}
    Wait For Node Indexed Within Budget    Vulnerability    ${MAINTAINER_VULNERABILITY_ID}
    Wait For Node Indexed Within Budget    Vulnerability    ${NESTED_REPORTER_VULNERABILITY_ID}
    ...    ${NESTED_REPORTER_VULNERABILITY_TITLE}    label_field=title
    Wait For Node Indexed Within Budget    Vulnerability    ${NESTED_DEVELOPER_VULNERABILITY_ID}

Query Vulnerability Counts As Victim
    ${query}=    Evaluate
    ...    {"query_type": "aggregation", "nodes": [{"id": "p", "entity": "Project", "columns": ["name"]}, {"id": "v", "entity": "Vulnerability"}], "relationships": [{"type": "IN_PROJECT", "from": "v", "to": "p"}], "aggregations": [{"function": "count", "target": "v", "group_by": "p", "alias": "vuln_count"}], "limit": 20}
    ${resp}=    Orbit Query With Token    ${query}    ${ROLE_AUTHZ_VICTIM_PAT}
    RETURN    ${resp}

Query Vulnerability Counts For Project As Victim
    [Arguments]    ${project_id}    ${vulnerability_filters}=${None}
    ${query}=    Evaluate
    ...    {"query_type": "aggregation", "nodes": [{"id": "p", "entity": "Project", "columns": ["name"], "node_ids": [int($project_id)]}, {"id": "v", "entity": "Vulnerability", "filters": $vulnerability_filters or {}}], "relationships": [{"type": "IN_PROJECT", "from": "v", "to": "p"}], "aggregations": [{"function": "count", "target": "v", "group_by": "p", "alias": "vuln_count"}], "limit": 10}
    ${resp}=    Orbit Query With Token    ${query}    ${ROLE_AUTHZ_VICTIM_PAT}
    RETURN    ${resp}

Reporter Vulnerability Oracle Filters
    ${created_at}=    Normalize ClickHouse Timestamp    ${REPORTER_VULNERABILITY_CREATED_AT}
    ${filters}=    Evaluate
    ...    [None, {"severity": "critical"}, {"state": "detected"}, {"report_type": "generic"}, {"id": int($REPORTER_VULNERABILITY_ID)}, {"id": {"op": "lte", "value": int($REPORTER_VULNERABILITY_ID)}}, {"title": $REPORTER_VULNERABILITY_TITLE}, {"created_at": {"op": "lte", "value": $created_at}}]
    RETURN    ${filters}

Nested Reporter Vulnerability Oracle Filters
    ${created_at}=    Normalize ClickHouse Timestamp    ${NESTED_REPORTER_VULNERABILITY_CREATED_AT}
    ${filters}=    Evaluate
    ...    [None, {"severity": "critical"}, {"state": "detected"}, {"report_type": "generic"}, {"id": int($NESTED_REPORTER_VULNERABILITY_ID)}, {"id": {"op": "lte", "value": int($NESTED_REPORTER_VULNERABILITY_ID)}}, {"title": $NESTED_REPORTER_VULNERABILITY_TITLE}, {"created_at": {"op": "lte", "value": $created_at}}]
    RETURN    ${filters}

Security Manager Vulnerability Oracle Filters
    ${created_at}=    Normalize ClickHouse Timestamp    ${SECURITY_VULNERABILITY_CREATED_AT}
    ${filters}=    Evaluate
    ...    [None, {"severity": "high"}, {"state": "detected"}, {"report_type": "generic"}, {"id": int($SECURITY_VULNERABILITY_ID)}, {"id": {"op": "lte", "value": int($SECURITY_VULNERABILITY_ID)}}, {"title": $SECURITY_VULNERABILITY_TITLE}, {"created_at": {"op": "gte", "value": $created_at}}]
    RETURN    ${filters}

Normalize ClickHouse Timestamp
    [Documentation]    GraphQL emits ISO-8601 with `T` and `Z`; ClickHouse DateTime literals
    ...                want a space and no zone suffix.
    [Arguments]    ${timestamp}
    ${normalized}=    Evaluate    $timestamp.replace("T", " ").replace("Z", "")
    RETURN    ${normalized}

Authorized Vulnerability Path Is Visible
    [Arguments]    ${project_id}
    ${resp}=    Query Vulnerability Counts For Project As Victim    ${project_id}    ${None}
    Response Has Project Count    ${resp}    ${project_id}    1

Direct Vulnerability Search Is Hidden
    [Arguments]    ${vulnerability_id}
    ${resp}=    Direct Vulnerability Search    ${vulnerability_id}
    Should Be Equal As Integers    ${resp["row_count"]}    0

Direct Vulnerability Search Is Visible
    [Arguments]    ${vulnerability_id}
    ${resp}=    Direct Vulnerability Search    ${vulnerability_id}
    Should Be True    ${resp["row_count"]} >= 1
    ...    Expected ${vulnerability_id} to be visible to victim, got row_count=${resp["row_count"]}

Direct Vulnerability Search
    [Arguments]    ${vulnerability_id}
    ${id}=    Convert To Integer    ${vulnerability_id}
    ${filters}=    Create Dictionary    id=${id}
    ${query}=    Create Dictionary    query_type=traversal
    ...    node=${{{"id": "v", "entity": "Vulnerability", "filters": $filters}}}
    ${resp}=    Orbit Query With Token    ${query}    ${ROLE_AUTHZ_VICTIM_PAT}
    RETURN    ${resp}

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
