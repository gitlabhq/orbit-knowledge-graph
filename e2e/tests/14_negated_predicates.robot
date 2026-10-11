*** Settings ***
Documentation       NOT predicates end to end through Rails. Seeds one top-level group with a public
...                 project (open, closed, and confidential issues, plus one vulnerability) and a
...                 private project (one open issue). The admin bot is the control. A Reporter, a
...                 Guest, and a Security Manager on the public project and a non-member each run
...                 the same NOT queries in raw, llm, and gql formats. No user may see an issue that
...                 the GitLab REST API hides from that user, the Reporter sees exactly the open
...                 issues that REST shows, NOT must not
...                 reopen the vulnerability aggregation oracle from issue #347, and the rejected
...                 forms (OR, administrator-only fields, traversal_path under NOT) must fail with
...                 the same message whether or not hidden data exists. MCP is not covered here
...                 because it needs an OAuth token (see 09_api_surface).

Resource            gitlab.resource
Resource            orbit.resource

Suite Setup         Run Keywords    Attach To Shared Fixture    AND    Seed Negated Predicate Fixture


*** Test Cases ***
Admin Control Sees Every Open Issue Through Each NOT Form
    [Tags]    negation
    FOR    ${query}    IN    @{NEG_OPEN_QUERIES}
        Within Budget    Open Issue Ids Match    ${query}    ${NEG_ADMIN_EXPECTED}    ${None}
        FOR    ${format}    IN    llm    gql
            ${text}=    Orbit Query Text    ${query}    ${format}
            FOR    ${issue}    IN    @{NEG_ADMIN_EXPECTED}
                Should Contain    ${text}    ${NEG_TITLES}[${issue}]    admin ${format} lacks issue ${issue}
            END
            Should Not Contain    ${text}    ${NEG_TITLES}[${NEG_ISSUE_CLOSED}]
        END
    END

Reporter Sees Exactly The Open Issues GitLab Shows Through Each NOT Form
    [Tags]    negation    authz    redaction
    ${token}=    Set Variable    ${NEG_TOKENS["reporter"]}
    ${expected}=    REST Visible Open Issue Ids    ${token}
    List Should Contain Value    ${expected}    ${NEG_ISSUE_OPEN}
    List Should Not Contain Value    ${expected}    ${NEG_ISSUE_PRIVATE}
    FOR    ${query}    IN    @{NEG_OPEN_QUERIES}
        Within Budget    Open Issue Ids Match    ${query}    ${expected}    ${token}
    END

No User Sees An Issue That GitLab Hides Through Any NOT Form
    [Tags]    negation    authz    redaction
    FOR    ${user}    IN    reporter    guest    security_manager    nonmember
        ${token}=    Set Variable    ${NEG_TOKENS["${user}"]}
        ${visible}=    REST Visible Open Issue Ids    ${token}
        FOR    ${query}    IN    @{NEG_OPEN_QUERIES}
            ${resp}=    Orbit Query With Token    ${query}    ${token}
            ${ids}=    Result Ids    ${resp}
            FOR    ${id}    IN    @{ids}
                List Should Contain Value    ${visible}    ${id}    ${user} saw issue ${id} that GitLab hides
            END
        END
    END

Hidden Issues Do Not Appear In LLM Or GQL Output
    [Tags]    negation    authz    redaction
    FOR    ${user}    IN    reporter    guest    nonmember
        ${token}=    Set Variable    ${NEG_TOKENS["${user}"]}
        ${visible}=    REST Visible Open Issue Ids    ${token}
        FOR    ${format}    IN    llm    gql
            ${text}=    Orbit Query Text    ${NEG_OPEN_QUERIES[0]}    ${format}    ${token}
            FOR    ${issue}    IN    @{NEG_ALL_ISSUES}
                IF    $issue not in $visible
                    Should Not Contain    ${text}    ${NEG_TITLES}[${issue}]
                    ...    ${user} saw hidden issue ${issue} in ${format}
                END
            END
        END
    END

Hidden And Missing Issues Return The Same Empty Result
    [Tags]    negation    authz
    ${hidden}=    Orbit Query With Token
    ...    MATCH (w:WorkItem) WHERE w.id IN [${NEG_ISSUE_PRIVATE}] AND NOT w.state = 'closed' RETURN w.id, w.title
    ...    ${NEG_TOKENS["nonmember"]}
    ${missing}=    Orbit Query With Token
    ...    MATCH (w:WorkItem) WHERE w.id IN [${NEG_MISSING_ID}] AND NOT w.state = 'closed' RETURN w.id, w.title
    ...    ${NEG_TOKENS["nonmember"]}
    Should Be Equal As Integers    ${hidden["row_count"]}    0
    Should Be Equal    ${hidden["result"]}    ${missing["result"]}

OR And XOR Reject With A Hint
    [Tags]    negation
    ${message}=    Orbit Query Rejected
    ...    MATCH (w:WorkItem) WHERE w.id = ${NEG_ISSUE_OPEN} OR w.id = ${NEG_ISSUE_CLOSED} RETURN w.id
    Should Contain    ${message}    OR and XOR are not supported
    ${message}=    Orbit Query Rejected
    ...    MATCH (w:WorkItem {id: ${NEG_ISSUE_OPEN}}) WHERE NOT (w.state = 'closed' XOR w.title = '${NEG_PRIVATE_TITLE}') RETURN w.id
    Should Contain    ${message}    OR and XOR are not supported
    Should Not Contain    ${message}    ${NEG_PRIVATE_TITLE}

Traversal Path Under NOT Rejects Without Echoing The Path
    [Tags]    negation    authz
    ${message}=    Orbit Query Rejected
    ...    MATCH (w:WorkItem {id: ${NEG_ISSUE_OPEN}}) WHERE NOT w.traversal_path STARTS WITH '${NEG_GROUP_ID}/' RETURN w.id
    Should Contain    ${message}    traversal_path cannot be used under NOT
    Should Not Contain    ${message}    ${NEG_GROUP_ID}/

Administrator Only Fields Under NOT Reject For Users And Work For The Admin
    [Tags]    negation    authz
    ${query}=    Set Variable
    ...    MATCH (u:User {id: ${NEG_REPORTER_ID}}) WHERE NOT (u.is_admin = true AND u.username = 'nobody') RETURN u.username
    FOR    ${user}    IN    reporter    nonmember
        ${message}=    Rejected With Token    ${query}    ${NEG_TOKENS["${user}"]}
        Should Contain    ${message}    administrator
    END
    ${resp}=    Orbit Query    ${query}
    Should Be Equal As Integers    ${resp["row_count"]}    1

NOT Does Not Reopen The Vulnerability Aggregation Oracle
    [Documentation]    A Reporter gets no Project row for every NOT form, and for the positive twin,
    ...                so a pair of counts never reveals the total. A Security Manager on the same
    ...                project gets the real counts, which proves the Reporter result is not vacuous.
    [Tags]    negation    authz    security
    FOR    ${predicate}    IN    @{NEG_ORACLE_PREDICATES}
        ${reporter}=    Vulnerability Count    ${predicate}    ${NEG_TOKENS["reporter"]}
        Should Be Equal As Integers    ${reporter}    0    Reporter counted ${predicate}
    END
    ${twin}=    Vulnerability Count    v.severity = 'low'    ${NEG_TOKENS["reporter"]}
    Should Be Equal As Integers    ${twin}    0
    Within Budget    Security Manager Counts Vulnerability

Victim Cannot See The Private Issue Through A NOT Filter On Explicit IDs
    [Tags]    negation    authz    redaction
    ${resp}=    Orbit Query With Token
    ...    MATCH (w:WorkItem) WHERE w.id IN [${NEG_ISSUE_OPEN}, ${NEG_ISSUE_PRIVATE}] AND NOT w.state = 'closed' RETURN w.id, w.title
    ...    ${NEG_TOKENS["reporter"]}
    ${ids}=    Result Ids    ${resp}
    List Should Contain Value    ${ids}    ${NEG_ISSUE_OPEN}
    List Should Not Contain Value    ${ids}    ${NEG_ISSUE_PRIVATE}


*** Keywords ***
Seed Negated Predicate Fixture
    ${suffix}=    Random Suffix
    Start Indexing Budget    600
    ${group}=    Create Group    e2e-neg-${suffix}
    Enable Orbit    ${group["id"]}
    ${public}=    Create Project    e2e-neg-pub-${suffix}    ${group["id"]}    readme=${True}
    ${private}=    Create Project    e2e-neg-priv-${suffix}    ${group["id"]}    visibility=private
    ${open}=    Create Issue    ${public["id"]}    e2e-neg-open-${suffix}
    ${closed}=    Create Issue    ${public["id"]}    e2e-neg-closed-${suffix}
    ${confidential}=    Create Issue    ${public["id"]}    e2e-neg-conf-${suffix}    confidential=${True}
    ${hidden}=    Create Issue    ${private["id"]}    e2e-neg-priv-issue-${suffix}
    Close Issue    ${public["id"]}    ${closed["iid"]}
    ${vulnerability}=    Create Vulnerability    ${public["id"]}    e2e-neg-${suffix} SQLi
    ...    severity=critical
    ${tokens}=    Create Dictionary
    FOR    ${user}    ${level}    IN    reporter    20    guest    10    security_manager    25
    ...    nonmember    ${None}
        ${account}=    Create User    e2e-neg-${suffix}-${user.replace("_", "-")}
        ${root_headers}=    Root Auth Headers
        ${token}=    Issue PAT For User    ${root_headers}    ${account["id"]}
        Add Group Member    ${SHARED_NAMESPACE_ID}    ${account["id"]}    20
        IF    $level is not None
            Add Project Member    ${public["id"]}    ${account["id"]}    ${level}
        END
        Set To Dictionary    ${tokens}    ${user}    ${token}
        IF    $user == "reporter"
            Set Suite Variable    ${NEG_REPORTER_ID}    ${account["id"]}
        END
    END
    Set Suite Variable    ${NEG_TOKENS}    ${tokens}
    Set Suite Variable    ${NEG_GROUP_ID}    ${group["id"]}
    Set Suite Variable    ${NEG_PUBLIC_PROJECT_ID}    ${public["id"]}
    Set Suite Variable    ${NEG_PRIVATE_PROJECT_ID}    ${private["id"]}
    Set Suite Variable    ${NEG_ISSUE_OPEN}    ${open["id"]}
    Set Suite Variable    ${NEG_ISSUE_CLOSED}    ${closed["id"]}
    Set Suite Variable    ${NEG_ISSUE_CONFIDENTIAL}    ${confidential["id"]}
    Set Suite Variable    ${NEG_ISSUE_PRIVATE}    ${hidden["id"]}
    Set Suite Variable    ${NEG_PRIVATE_TITLE}    e2e-neg-priv-issue-${suffix}
    Set Suite Variable    ${NEG_MISSING_ID}    ${{int($hidden["id"]) + 1000000}}
    Set Suite Variable    ${NEG_VULNERABILITY_ID}    ${vulnerability["id"]}
    ${all}=    Create List    ${open["id"]}    ${closed["id"]}    ${confidential["id"]}    ${hidden["id"]}
    Set Suite Variable    ${NEG_ALL_ISSUES}    ${all}
    ${titles}=    Create Dictionary
    FOR    ${issue}    IN    ${open}    ${closed}    ${confidential}    ${hidden}
        Set To Dictionary    ${titles}    ${issue["id"]}    ${issue["title"]}
    END
    Set Suite Variable    ${NEG_TITLES}    ${titles}
    ${admin_expected}=    Create List    ${open["id"]}    ${confidential["id"]}    ${hidden["id"]}
    Set Suite Variable    ${NEG_ADMIN_EXPECTED}    ${admin_expected}
    ${ids}=    Set Variable    ${open["id"]}, ${closed["id"]}, ${confidential["id"]}, ${hidden["id"]}
    ${queries}=    Create List
    ...    MATCH (w:WorkItem) WHERE w.id IN [${ids}] AND NOT w.state = 'closed' RETURN w.id, w.title
    ...    MATCH (w:WorkItem) WHERE w.id IN [${ids}] AND NOT w.title IN ['e2e-neg-closed-${suffix}'] RETURN w.id, w.title
    ...    MATCH (w:WorkItem) WHERE w.id IN [${ids}] AND NOT (w.state = 'closed' AND w.title STARTS WITH 'e2e-neg') RETURN w.id, w.title
    Set Suite Variable    ${NEG_OPEN_QUERIES}    ${queries}
    ${oracle}=    Create List    NOT v.severity = 'low'    NOT v.severity IN ['low', 'info']
    ...    NOT v.id < ${vulnerability["id"]}    NOT (v.id >= 1 AND v.id <= ${{int($vulnerability["id"]) - 1}})
    ...    NOT NOT v.severity = 'critical'
    Set Suite Variable    ${NEG_ORACLE_PREDICATES}    ${oracle}
    Wait For Node Indexed Within Budget    WorkItem    ${NEG_ISSUE_OPEN}    e2e-neg-open-${suffix}
    ...    label_field=title
    Wait For Node Indexed Within Budget    WorkItem    ${NEG_ISSUE_CONFIDENTIAL}    e2e-neg-conf-${suffix}
    ...    label_field=title
    Wait For Node Indexed Within Budget    WorkItem    ${NEG_ISSUE_PRIVATE}    ${NEG_PRIVATE_TITLE}
    ...    label_field=title
    Wait For Node Indexed Within Budget    Vulnerability    ${NEG_VULNERABILITY_ID}
    ...    ${vulnerability["title"]}    label_field=title
    Within Budget    Work Item Is Closed    ${NEG_ISSUE_CLOSED}

Within Budget
    [Arguments]    ${keyword}    @{arguments}
    ${budget}=    Remaining Budget
    Wait Until Keyword Succeeds    ${budget}    2s    ${keyword}    @{arguments}

Work Item Is Closed
    [Arguments]    ${issue_id}
    ${resp}=    Orbit Query    MATCH (w:WorkItem {id: ${issue_id}}) WHERE w.state = 'closed' RETURN w.id
    Should Be Equal As Integers    ${resp["row_count"]}    1    issue ${issue_id} is not closed in Orbit yet

Result Ids
    [Arguments]    ${resp}
    ${ids}=    Evaluate    sorted(int(n["id"]) for n in $resp["result"].get("nodes", []))
    RETURN    ${ids}

Open Issue Ids Match
    [Documentation]    Run ${query} (with ${token}, or the admin bot) and assert the returned
    ...                WorkItem ids equal ${expected}.
    [Arguments]    ${query}    ${expected}    ${token}
    IF    $token is None
        ${resp}=    Orbit Query    ${query}
    ELSE
        ${resp}=    Orbit Query With Token    ${query}    ${token}
    END
    ${ids}=    Result Ids    ${resp}
    ${wanted}=    Evaluate    sorted(int(i) for i in $expected)
    Lists Should Be Equal    ${ids}    ${wanted}    ${query}

REST Visible Open Issue Ids
    [Documentation]    Ids of the seeded open issues that GitLab REST shows to the owner of ${token}.
    [Arguments]    ${token}
    ${headers}=    Create Dictionary    PRIVATE-TOKEN=${token}
    ${params}=    Create Dictionary    state=opened    per_page=100
    ${visible}=    Create List
    FOR    ${project_id}    IN    ${NEG_PUBLIC_PROJECT_ID}    ${NEG_PRIVATE_PROJECT_ID}
        ${resp}=    GET    ${GITLAB_URL}/api/v4/projects/${project_id}/issues
        ...    headers=${headers}    params=${params}    expected_status=any
        IF    ${resp.status_code} == 200
            FOR    ${issue}    IN    @{resp.json()}
                IF    $issue["id"] in $NEG_ALL_ISSUES
                    Append To List    ${visible}    ${issue["id"]}
                END
            END
        ELSE
            Should Be Equal As Integers    ${resp.status_code}    404
        END
    END
    RETURN    ${visible}

Rejected With Token
    [Arguments]    ${query}    ${token}
    ${headers}=    Create Dictionary    PRIVATE-TOKEN=${token}    Content-Type=application/json
    ${body}=    Create Dictionary    query=${query}
    ${resp}=    POST    ${GITLAB_URL}/api/v4/orbit/query
    ...    headers=${headers}    json=${body}    expected_status=400
    RETURN    ${resp.json()["message"]}

Vulnerability Count
    [Documentation]    Vulnerability count for the public project under ${predicate}; 0 when the
    ...                response has no Project row.
    [Arguments]    ${predicate}    ${token}
    ${resp}=    Orbit Query With Token
    ...    MATCH (p:Project {id: ${NEG_PUBLIC_PROJECT_ID}})<-[:IN_PROJECT]-(v:Vulnerability) WHERE ${predicate} RETURN p{.name}, count(v) AS vuln_count LIMIT 10
    ...    ${token}
    ${rows}=    Set Variable    ${resp["result"].get("rows", [])}
    ${count}=    Evaluate    sum(int(row.get("vuln_count", 0)) for row in $rows)
    RETURN    ${count}

Security Manager Counts Vulnerability
    FOR    ${predicate}    IN    @{NEG_ORACLE_PREDICATES}
        ${count}=    Vulnerability Count    ${predicate}    ${NEG_TOKENS["security_manager"]}
        Should Be Equal As Integers    ${count}    1    Security Manager count for ${predicate}
    END
    ${twin}=    Vulnerability Count    v.severity = 'low'    ${NEG_TOKENS["security_manager"]}
    Should Be Equal As Integers    ${twin}    0
