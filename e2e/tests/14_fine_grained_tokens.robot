*** Settings ***
Documentation       Fine-grained tokens restrict Orbit to the caller's existing resource and feature grants.

Resource            gitlab.resource
Resource            orbit.resource
Resource            git.resource

Suite Setup         Run Keywords    Attach To Shared Fixture    AND    Build Token Fixture
Suite Teardown      Delete Token Fixture
Test Tags           authz    fine-grained-tokens


*** Variables ***
${TOKEN_CODE_FIXTURE}    /fixtures/ruby/weather-app


*** Test Cases ***
Classic Token Reads Both Member Projects
    Project Results Are    ${CLASSIC_TOKEN}    ${PROJECT_A}[id]    ${PROJECT_B}[id]
    Normal API Read Is    ${CLASSIC_TOKEN}    projects/${PROJECT_A}[id]    200
    Normal API Read Is    ${CLASSIC_TOKEN}    projects/${PROJECT_B}[id]    200

All Membership Grants Retain Both Member Projects
    Project Results Are    ${TOKENS}[all_memberships]    ${PROJECT_A}[id]    ${PROJECT_B}[id]

Resource Permission Does Not Replace The Orbit Gateway
    Normal API Read Is    ${TOKENS}[no_gateway]    projects/${PROJECT_A}[id]    200
    ${query}=    Project Query
    Orbit Query Status Is    ${TOKENS}[no_gateway]    ${query}    403

Orbit Requires An Enabled Namespace Membership
    Normal API Read Is    ${DISABLED_TOKEN}    projects/${DISABLED_PROJECT}[id]    200
    Normal API Read Is    ${DISABLED_TOKEN}    orbit/schema    200
    ${query}=    Node Query    Project    ${DISABLED_PROJECT}[id]
    Orbit Query Status Is    ${DISABLED_TOKEN}    ${query}    403
    ...    No Knowledge Graph enabled namespaces available

Selected Project Grant Hides The Other Member Project
    Project Results Are    ${TOKENS}[read_project]    ${PROJECT_A}[id]
    Normal API Read Is    ${TOKENS}[read_project]    projects/${PROJECT_A}[id]    200
    Normal API Read Is    ${TOKENS}[read_project]    projects/${PROJECT_B}[id]    403

Selected Group Grant Includes Descendants And Excludes Siblings
    Project Results Are    ${TOKENS}[group]    ${PROJECT_A}[id]
    Normal API Read Is    ${TOKENS}[group]    projects/${PROJECT_A}[id]    200
    Normal API Read Is    ${TOKENS}[group]    projects/${PROJECT_B}[id]    403

Feature Reads Require Their Existing Permissions
    [Template]    Feature Read Is Scoped
    WorkItem    ${ISSUE_A}[id]       read_work_item    projects/${PROJECT_A}[id]/issues/${ISSUE_A}[iid]
    Pipeline    ${PIPELINE}[id]      read_pipeline     projects/${PROJECT_A}[id]/pipelines/${PIPELINE}[id]
    Job         ${JOB}[id]           read_job          projects/${PROJECT_A}[id]/jobs/${JOB}[id]
    User        ${TOKEN_USER}[id]    read_user         users/${TOKEN_USER}[id]

Private Code Requires The Code Read Permission
    Normal API Read Is    ${CLASSIC_TOKEN}           projects/${PROJECT_A}[id]/repository/tree    200
    Normal API Read Is    ${TOKENS}[read_code]       projects/${PROJECT_A}[id]/repository/tree    403
    Normal API Read Is    ${TOKENS}[read_code]       projects/${PROJECT_B}[id]/repository/tree    403
    Normal API Read Is    ${TOKENS}[read_project]    projects/${PROJECT_A}[id]/repository/tree    403
    FOR    ${entity}    IN    File    Definition
        ${query}=    Project Code Query    ${entity}
        ${allowed}=    Orbit Query With Token    ${query}    ${TOKENS}[read_code]
        Should Be True    ${allowed}[row_count] > 0
        ${denied}=    Orbit Query With Token    ${query}    ${TOKENS}[read_project]
        Result IDs Are    ${denied}
        ${gateway}=    Orbit Query With Token    ${query}    ${TOKENS}[gateway]
        Result IDs Are    ${gateway}
        ${other_query}=    Project Code Query    ${entity}    ${PROJECT_B}[id]
        ${classic}=    Orbit Query With Token    ${other_query}    ${CLASSIC_TOKEN}
        Should Be True    ${classic}[row_count] > 0
        ${other}=    Orbit Query With Token    ${other_query}    ${TOKENS}[read_code]
        Result IDs Are    ${other}
    END

Permissions Stay Paired With Their Selected Boundaries
    ${query}=    Node Query    WorkItem    ${ISSUE_A}[id]    ${ISSUE_B}[id]
    ${result}=    Orbit Query With Token    ${query}    ${TOKENS}[mixed]
    Result IDs Are    ${result}    ${ISSUE_A}[id]
    Project Results Are    ${TOKENS}[mixed]    ${PROJECT_B}[id]

Counts Exclude Projects Outside The Token Scope
    ${query}=    Project Query
    Set To Dictionary    ${query}    query_type=aggregation    aggregations=${{[{"count": "n", "as": "count"}]}}
    ${classic}=    Orbit Query With Token    ${query}    ${CLASSIC_TOKEN}
    ${classic_count}=    Aggregation Value    ${classic}    count
    Should Be Equal As Integers    ${classic_count}    2
    ${selected}=    Orbit Query With Token    ${query}    ${TOKENS}[read_project]
    ${selected_count}=    Aggregation Value    ${selected}    count
    Should Be Equal As Integers    ${selected_count}    1
    ${gateway}=    Orbit Query With Token    ${query}    ${TOKENS}[gateway]
    ${gateway_count}=    Aggregation Value    ${gateway}    count
    Should Be Equal As Integers    ${gateway_count}    0

Aggregate Excludes A Denied Unreturned Project
    ${wi}=    Create Dictionary    id=wi    entity=WorkItem    node_ids=${{[int($ISSUE_A["id"])]}}
    ${project}=    Create Dictionary    id=p    entity=Project    node_ids=${{[int($PROJECT_A["id"])]}}
    ${edge}=    Create Dictionary    type=IN_PROJECT    from=wi    to=p
    ${query}=    Create Dictionary    query_type=aggregation    nodes=${{[$wi, $project]}}
    ...    relationships=${{[$edge]}}    aggregations=${{[{"count": "wi", "as": "count"}]}}
    FOR    ${token}    IN    ${CLASSIC_TOKEN}    ${TOKENS}[all_memberships]
        ${result}=    Orbit Query With Token    ${query}    ${token}
        ${count}=    Aggregation Value    ${result}    count
        Should Be Equal As Integers    ${count}    1
    END
    ${denied}=    Orbit Query With Token    ${query}    ${TOKENS}[read_work_item]
    ${count}=    Aggregation Value    ${denied}    count
    Should Be Equal As Integers    ${count}    0

Cursor Does Not Reveal Another Member Project
    ${query}=    Project Query
    Set To Dictionary    ${query}    cursor=${{{"page_size": 1}}}    order_by=n.id
    ${selected}=    Orbit Query With Token    ${query}    ${TOKENS}[read_project]
    Result IDs Are    ${selected}    ${PROJECT_A}[id]
    Should Be Equal    ${selected}[result][pagination][has_more]    ${False}
    Should Not Contain    ${selected}[result][pagination]    next_cursor
    ${classic}=    Orbit Query With Token    ${query}    ${CLASSIC_TOKEN}
    Should Be Equal    ${classic}[result][pagination][has_more]    ${True}
    ${after}=    Set Variable    ${classic}[result][pagination][next_cursor]
    Set To Dictionary    ${query}    cursor=${{{"page_size": 1, "after": $after}}}
    ${restricted_page}=    Orbit Query With Token    ${query}    ${TOKENS}[read_project]
    Result IDs Are    ${restricted_page}
    ${classic_page}=    Orbit Query With Token    ${query}    ${CLASSIC_TOKEN}
    Result IDs Are    ${classic_page}    ${PROJECT_B}[id]

Graph Status Uses Each Feature Permission
    [Template]    Graph Status Project Counts Are
    ${CLASSIC_TOKEN}           2    2
    ${TOKENS}[read_project]    1    0
    ${TOKENS}[read_code]       0    1
    ${TOKENS}[gateway]         0    0

Schema Metadata Needs Only The Orbit Gateway
    Normal API Read Is    ${TOKENS}[gateway]       orbit/schema    200
    Normal API Read Is    ${TOKENS}[no_gateway]    orbit/schema    403

MCP Query Uses The Same Resource Grants
    ${query}=    Project Query
    ${arguments}=    Create Dictionary    command_name=query_graph    parameters=${{{"query": $query}}}
    ${params}=    Create Dictionary    name=invoke_command    arguments=${arguments}
    ${body}=    Create Dictionary    jsonrpc=2.0    method=tools/call    id=token-query    params=${params}
    ${headers}=    GitLab Auth Headers    ${TOKENS}[read_project]
    ${resp}=    POST    ${GITLAB_URL}/api/v4/orbit/mcp
    ...    headers=${headers}    json=${body}    expected_status=200
    Should Be Equal    ${resp.json()["result"].get("isError", False)}    ${False}
    ${text}=    Set Variable    ${resp.json()["result"]["content"][0]["text"]}
    Should Contain        ${text}    ${PROJECT_A}[name]
    Should Not Contain    ${text}    ${PROJECT_B}[name]


*** Keywords ***
Build Token Fixture
    ${suffix}=    Random Suffix
    ${root}=    Create Group    fgpat-${suffix}    visibility=private
    Set Suite Variable    ${TOKEN_ROOT}    ${root}
    ${allowed}=    Create Subgroup    ${root}[id]    allowed-${suffix}    visibility=private
    ${nested}=    Create Subgroup    ${allowed}[id]    nested-${suffix}    visibility=private
    ${sibling}=    Create Subgroup    ${root}[id]    sibling-${suffix}    visibility=private
    ${a}=    Create Project    fgpat-${suffix}-a    ${nested}[id]     visibility=private
    ${b}=    Create Project    fgpat-${suffix}-b    ${sibling}[id]    visibility=private
    Set Suite Variable    ${PROJECT_A}    ${a}
    Set Suite Variable    ${PROJECT_B}    ${b}
    ${user}=    Create User    fgpat-${suffix}-reader
    Set Suite Variable    ${TOKEN_USER}    ${user}
    Enable Feature Flag    granular_personal_access_tokens    user=${user}[username]
    Add Group Member    ${root}[id]    ${user}[id]    30
    ${root_headers}=    Root Auth Headers
    ${classic}=    Issue PAT For User    ${root_headers}    ${user}[id]
    Set Suite Variable    ${CLASSIC_TOKEN}    ${classic}
    Push Fixture To Project    ${a}    ${TOKEN_CODE_FIXTURE}
    Push Fixture To Project    ${b}    ${TOKEN_CODE_FIXTURE}
    Enable Orbit    ${root}[id]
    Start Indexing Budget    600
    Wait For Node Indexed Within Budget    Project    ${a}[id]    ${a}[name]
    Wait For Node Indexed Within Budget    Project    ${b}[id]    ${b}[name]
    Seed Token Pipeline
    ${issue_a}=    Create Issue    ${a}[id]    fgpat-${suffix}-issue-a
    ${issue_b}=    Create Issue    ${b}[id]    fgpat-${suffix}-issue-b
    Set Suite Variable    ${ISSUE_A}    ${issue_a}
    Set Suite Variable    ${ISSUE_B}    ${issue_b}
    Provision Feature Tokens    ${allowed}[id]
    Provision Disabled Orbit Caller    ${suffix}
    Wait For Token Fixture Indexed

Seed Token Pipeline
    ${headers}=    GitLab Auth Headers
    ${config}=    Catenate    SEPARATOR=\n    token_test:    ${SPACE}${SPACE}script: echo token permission test
    ${action}=    Create Dictionary    action=create    file_path=.gitlab-ci.yml    content=${config}
    ${commit}=    Create Dictionary    branch=main    commit_message=Add token test pipeline    actions=${{[$action]}}
    POST    ${GITLAB_URL}/api/v4/projects/${PROJECT_A}[id]/repository/commits
    ...    headers=${headers}    json=${commit}    expected_status=201
    ${body}=    Create Dictionary    ref=main
    ${resp}=    POST    ${GITLAB_URL}/api/v4/projects/${PROJECT_A}[id]/pipeline
    ...    headers=${headers}    json=${body}    expected_status=201
    Set Suite Variable    ${PIPELINE}    ${resp.json()}
    ${job}=    Wait Until Keyword Succeeds    90s    3s    Token Pipeline Job
    Set Suite Variable    ${JOB}    ${job}

Token Pipeline Job
    ${headers}=    GitLab Auth Headers
    ${resp}=    GET    ${GITLAB_URL}/api/v4/projects/${PROJECT_A}[id]/pipelines/${PIPELINE}[id]/jobs
    ...    headers=${headers}    expected_status=200
    Should Not Be Empty    ${resp.json()}
    RETURN    ${resp.json()[0]}

Provision Feature Tokens
    [Arguments]    ${group_id}
    ${tokens}=    Create Dictionary
    Set Suite Variable    ${TOKENS}    ${tokens}
    ${gateway}=    Create Dictionary    access=user    permissions=${{["read_knowledge_graph", "execute_orbit_mcp_tool"]}}
    ${token}=    Issue Fine Grained PAT    ${CLASSIC_TOKEN}    ${{[$gateway]}}
    Set To Dictionary    ${TOKENS}    gateway=${token}
    ${scope}=    Create Dictionary    access=all_memberships    permissions=${{["read_project", "read_work_item"]}}
    ${token}=    Issue Fine Grained PAT    ${CLASSIC_TOKEN}    ${{[$gateway, $scope]}}
    Set To Dictionary    ${TOKENS}    all_memberships=${token}
    FOR    ${permission}    IN    read_project    read_work_item    read_pipeline    read_job    read_code
        ${scope}=    Project Token Scope    ${permission}    ${PROJECT_A}[id]
        ${token}=    Issue Fine Grained PAT    ${CLASSIC_TOKEN}    ${{[$gateway, $scope]}}
        Set To Dictionary    ${TOKENS}    ${permission}=${token}
    END
    ${scope}=    Create Dictionary    access=user    permissions=${{["read_user", "read_knowledge_graph", "execute_orbit_mcp_tool"]}}
    ${token}=    Issue Fine Grained PAT    ${CLASSIC_TOKEN}    ${{[$scope]}}
    Set To Dictionary    ${TOKENS}    read_user=${token}
    ${scope}=    Project Token Scope    read_project    ${PROJECT_A}[id]
    ${token}=    Issue Fine Grained PAT    ${CLASSIC_TOKEN}    ${{[$scope]}}
    Set To Dictionary    ${TOKENS}    no_gateway=${token}
    ${scope}=    Create Dictionary    access=selected_memberships    group_ids=${{[int($group_id)]}}
    ...    permissions=${{["read_project"]}}
    ${token}=    Issue Fine Grained PAT    ${CLASSIC_TOKEN}    ${{[$gateway, $scope]}}
    Set To Dictionary    ${TOKENS}    group=${token}
    ${a}=    Project Token Scope    read_work_item    ${PROJECT_A}[id]
    ${b}=    Project Token Scope    read_project      ${PROJECT_B}[id]
    ${token}=    Issue Fine Grained PAT    ${CLASSIC_TOKEN}    ${{[$gateway, $a, $b]}}
    Set To Dictionary    ${TOKENS}    mixed=${token}

Project Token Scope
    [Arguments]    ${permission}    ${project_id}
    ${scope}=    Create Dictionary    access=selected_memberships    project_ids=${{[int($project_id)]}}
    ...    permissions=${{[$permission]}}
    RETURN    ${scope}

Provision Disabled Orbit Caller
    [Arguments]    ${suffix}
    ${group}=    Create Group    fgpat-disabled-${suffix}    visibility=private
    Set Suite Variable    ${DISABLED_GROUP}    ${group}
    ${project}=    Create Project    fgpat-disabled-${suffix}    ${group}[id]    visibility=private
    Set Suite Variable    ${DISABLED_PROJECT}    ${project}
    ${user}=    Create User    fgpat-disabled-${suffix}-reader
    Set Suite Variable    ${DISABLED_USER}    ${user}
    Enable Feature Flag    granular_personal_access_tokens    user=${user}[username]
    Add Group Member    ${group}[id]    ${user}[id]    30
    ${headers}=    Root Auth Headers
    ${classic}=    Issue PAT For User    ${headers}    ${user}[id]
    ${gateway}=    Create Dictionary    access=user    permissions=${{["read_knowledge_graph"]}}
    ${scope}=    Project Token Scope    read_project    ${project}[id]
    ${token}=    Issue Fine Grained PAT    ${classic}    ${{[$gateway, $scope]}}
    Set Suite Variable    ${DISABLED_TOKEN}    ${token}

Wait For Token Fixture Indexed
    Wait For Node Indexed Within Budget    User        ${TOKEN_USER}[id]    ${TOKEN_USER}[username]    label_field=username
    Wait For Node Indexed Within Budget    WorkItem    ${ISSUE_A}[id]       ${ISSUE_A}[title]          label_field=title
    Wait For Node Indexed Within Budget    WorkItem    ${ISSUE_B}[id]       ${ISSUE_B}[title]          label_field=title
    Wait For Edge Indexed Within Budget    WorkItem    ${ISSUE_A}[id]    IN_PROJECT    Project    ${PROJECT_A}[id]
    Wait For Node Indexed Within Budget    Pipeline    ${PIPELINE}[id]
    Wait For Node Indexed Within Budget    Job         ${JOB}[id]
    FOR    ${project}    IN    ${PROJECT_A}    ${PROJECT_B}
        Definition Exists In Project    ${project}[id]    WeatherApp::Forecast    Class
    END
    Project Results Are    ${CLASSIC_TOKEN}    ${PROJECT_A}[id]    ${PROJECT_B}[id]

Node Query
    [Arguments]    ${entity}    @{ids}
    ${node}=    Create Dictionary    id=n    entity=${entity}    node_ids=${{[int(i) for i in $ids]}}
    ${query}=    Create Dictionary    query_type=traversal    nodes=${{[$node]}}
    RETURN    ${query}

Project Query
    ${query}=    Node Query    Project    ${PROJECT_A}[id]    ${PROJECT_B}[id]
    RETURN    ${query}

Project Code Query
    [Arguments]    ${entity}    ${project_id}=${PROJECT_A}[id]
    ${filters}=    Create Dictionary    project_id=${project_id}
    ${node}=    Create Dictionary    id=n    entity=${entity}    filters=${filters}
    ${query}=    Create Dictionary    query_type=traversal    nodes=${{[$node]}}
    RETURN    ${query}

Result IDs Are
    [Arguments]    ${result}    @{expected_ids}
    ${ids}=    Evaluate    [str(node["id"]) for node in $result["result"]["nodes"]]
    ${expected}=    Evaluate    [str(i) for i in $expected_ids]
    Lists Should Be Equal    ${ids}    ${expected}    ignore_order=True
    Should Be Equal As Integers    ${result}[row_count]    ${{len($expected_ids)}}

Project Results Are
    [Arguments]    ${token}    @{ids}
    ${query}=    Project Query
    ${result}=    Orbit Query With Token    ${query}    ${token}
    Result IDs Are    ${result}    @{ids}

Feature Read Is Scoped
    [Arguments]    ${entity}    ${node_id}    ${permission}    ${normal_path}
    Normal API Read Is    ${TOKENS}[${permission}]    ${normal_path}    200
    Normal API Read Is    ${TOKENS}[read_project]    ${normal_path}    403
    ${query}=    Node Query    ${entity}    ${node_id}
    ${allowed}=    Orbit Query With Token    ${query}    ${TOKENS}[${permission}]
    Result IDs Are    ${allowed}    ${node_id}
    ${denied}=    Orbit Query With Token    ${query}    ${TOKENS}[read_project]
    Result IDs Are    ${denied}

Normal API Read Is
    [Arguments]    ${token}    ${path}    ${status}
    ${headers}=    GitLab Auth Headers    ${token}
    GET    ${GITLAB_URL}/api/v4/${path}    headers=${headers}    expected_status=${status}

Orbit Query Status Is
    [Arguments]    ${token}    ${query}    ${status}    ${message}=${None}
    ${headers}=    GitLab Auth Headers    ${token}
    ${body}=    Create Dictionary    query=${query}
    ${resp}=    POST    ${GITLAB_URL}/api/v4/orbit/query
    ...    headers=${headers}    json=${body}    expected_status=${status}
    IF    $message is not None
        Should Contain    ${resp.json()["message"]}    ${message}
    END

Token Graph Status
    [Arguments]    ${token}
    ${headers}=    GitLab Auth Headers    ${token}
    ${params}=    Create Dictionary    namespace_id=${TOKEN_ROOT}[id]
    ${resp}=    GET    ${GITLAB_URL}/api/v4/orbit/graph_status
    ...    headers=${headers}    params=${params}    expected_status=200
    RETURN    ${resp.json()}

Graph Status Project Counts Are
    [Arguments]    ${token}    ${project_count}    ${code_project_count}
    ${status}=    Token Graph Status    ${token}
    ${count}=    Evaluate    sum(item["count"] for domain in $status["domains"] for item in domain["items"] if item["name"] == "Project")
    Should Be Equal As Integers    ${count}    ${project_count}
    Should Be Equal As Integers    ${status}[projects][total_known]    ${code_project_count}

Delete Token Fixture
    ${headers}=    Root Auth Headers
    FOR    ${name}    IN    TOKEN_ROOT    DISABLED_GROUP
        ${group}=    Get Variable Value    ${${name}}    ${None}
        IF    $group is not None
            IF    $GITLAB_HOSTED_PLAN
                Set Hosted Group Plan    ${group}    free
            END
            DELETE    ${GITLAB_URL}/api/v4/groups/${group}[id]    headers=${headers}    expected_status=202
        END
    END
    FOR    ${name}    IN    TOKEN_USER    DISABLED_USER
        ${user}=    Get Variable Value    ${${name}}    ${None}
        IF    $user is not None
            DELETE    ${GITLAB_URL}/api/v4/users/${user}[id]    headers=${headers}    expected_status=204
        END
    END
