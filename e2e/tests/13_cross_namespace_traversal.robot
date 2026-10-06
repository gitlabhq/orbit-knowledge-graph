*** Settings ***
Documentation       A project- or group-id-scoped query rewrites the scope filter to a tight
...                 startsWith(traversal_path, '<prefix>') on the scoped node only. This must not
...                 prune a related entity that lives under a DIFFERENT top-level namespace.
...                 Seeds two projects under two top-level groups, links their issues across
...                 namespaces (RELATED_TO, including a 3-hop chain) and opens a cross-project
...                 closing MR (CLOSES), then asserts the cross-namespace entity still appears across
...                 every query type: traversal (2-hop and 3-hop), variable-length neighbors,
...                 aggregation (3-hop), and path_finding, under both project scope and group scope.
...                 Discovered result nodes are polled within the shared budget because their
...                 per-resource authz is eventually consistent.

Resource            gitlab.resource
Resource            orbit.resource

Suite Setup         Run Keywords    Attach To Shared Fixture    AND    Seed Cross Namespace Fixture


*** Test Cases ***
Project Scoped Traversal Returns Its Own Issue And A Cross Namespace Related Issue
    [Tags]    cross-namespace
    Wait Until Result Node Ids Contain
    ...    MATCH (p:Project)<-[:IN_PROJECT]-(wi:WorkItem)-[:RELATED_TO]->(rel:WorkItem) WHERE p.id = ${XNS_PROJECT_ID_A} RETURN p, wi.id, rel.id, rel.title LIMIT 100
    ...    ${XNS_ISSUE_ID_A}    ${XNS_ISSUE_ID_B}

Project Scoped Neighbors Returns Cross Namespace Related Issue
    [Tags]    cross-namespace
    Wait Until Result Node Ids Contain    MATCH (wi:WorkItem {id: ${XNS_ISSUE_ID_A}})--(n) RETURN wi, n LIMIT 100
    ...    ${XNS_ISSUE_ID_B}

Multi Hop Neighbors Reach Cross Namespace Issue At Three Hops
    [Documentation]    The neighbors query type is 1-hop by schema, so a 3-hop neighborhood is
    ...                expressed as a variable-length (hops 1..3) RELATED_TO traversal. issue_c is
    ...                reachable from issue_a only via a 3-hop chain whose last hop crosses into a
    ...                different top-level namespace; the project-A tight prefix must not prune it.
    [Tags]    cross-namespace
    Wait Until Result Node Ids Contain
    ...    MATCH (p:Project)<-[:IN_PROJECT]-(a:WorkItem)-[:RELATED_TO*1..3]->(b:WorkItem) WHERE p.id = ${XNS_PROJECT_ID_A} RETURN p, a, b.id LIMIT 100
    ...    ${XNS_ISSUE_ID_C}

Project Scoped Multi Hop Traversal Reaches Cross Namespace Project
    [Tags]    cross-namespace
    Wait Until Result Node Ids Contain
    ...    MATCH (p:Project)<-[:IN_PROJECT]-(wi:WorkItem)-[:RELATED_TO]->(rel:WorkItem)-[:IN_PROJECT]->(p2:Project) WHERE p.id = ${XNS_PROJECT_ID_A} RETURN p, wi, rel.id, p2.id LIMIT 100
    ...    ${XNS_ISSUE_ID_B}    ${XNS_PROJECT_ID_B}

Project Scoped Multi Hop Aggregation Counts Cross Namespace Project
    [Tags]    cross-namespace
    Wait Until Aggregation At Least
    ...    MATCH (p:Project)<-[:IN_PROJECT]-(wi:WorkItem)-[:RELATED_TO]->(rel:WorkItem)-[:IN_PROJECT]->(p2:Project) WHERE p.id = ${XNS_PROJECT_ID_A} RETURN count(p2) AS xns_project_count
    ...    xns_project_count    1

Path Finding Within Scoped Project Returns The Path
    [Tags]    cross-namespace
    Wait Until Result Node Ids Contain
    ...    MATCH path = ANY SHORTEST (start:WorkItem {id: ${XNS_ISSUE_ID_A}})-[:IN_PROJECT*1..2]->(target:Project {id: ${XNS_PROJECT_ID_A}}) RETURN path
    ...    ${XNS_ISSUE_ID_A}    ${XNS_PROJECT_ID_A}

Group Scoped Multi Hop Traversal Returns Cross Namespace Related Issue
    [Tags]    cross-namespace
    Wait Until Result Node Ids Contain
    ...    MATCH (g:Group)-[:CONTAINS]->(p:Project)<-[:IN_PROJECT]-(wi:WorkItem)-[:RELATED_TO]->(rel:WorkItem) WHERE g.id = ${XNS_GROUP_ID_A} RETURN g, p, wi, rel.id LIMIT 100
    ...    ${XNS_ISSUE_ID_B}

Group Scoped Multi Hop Aggregation Counts Cross Namespace Related Issue
    [Tags]    cross-namespace
    Wait Until Aggregation At Least
    ...    MATCH (g:Group)-[:CONTAINS]->(p:Project)<-[:IN_PROJECT]-(wi:WorkItem)-[:RELATED_TO]->(rel:WorkItem) WHERE g.id = ${XNS_GROUP_ID_A} RETURN count(rel) AS related_count
    ...    related_count    1

Project Scoped Traversal Returns Cross Namespace Closed Issue
    [Tags]    cross-namespace
    [Setup]    Seed Cross Project Closing MR
    Wait Until Result Node Ids Contain
    ...    MATCH (p:Project)<-[:IN_PROJECT]-(mr:MergeRequest)-[:CLOSES]->(issue:WorkItem) WHERE p.id = ${XNS_PROJECT_ID_A} RETURN p, mr, issue.id LIMIT 100
    ...    ${XNS_ISSUE_ID_B}

Group Scoped Multi Hop Traversal Returns Cross Namespace Closed Issue
    [Tags]    cross-namespace
    [Setup]    Seed Cross Project Closing MR
    Wait Until Result Node Ids Contain
    ...    MATCH (g:Group)-[:CONTAINS]->(p:Project)<-[:IN_PROJECT]-(mr:MergeRequest)-[:CLOSES]->(issue:WorkItem) WHERE g.id = ${XNS_GROUP_ID_A} RETURN g, p, mr, issue.id LIMIT 100
    ...    ${XNS_ISSUE_ID_B}


*** Keywords ***
Seed Cross Namespace Fixture
    ${suffix}=    Random Suffix
    Start Indexing Budget    300
    ${group_a}=    Create Group    e2e-xns-a-${suffix}
    ${group_b}=    Create Group    e2e-xns-b-${suffix}
    Enable Orbit    ${group_a["id"]}
    Enable Orbit    ${group_b["id"]}
    ${project_a}=    Create Project    e2e-xns-prj-a-${suffix}    ${group_a["id"]}
    ${project_b}=    Create Project    e2e-xns-prj-b-${suffix}    ${group_b["id"]}
    ${issue_a}=    Create Issue    ${project_a["id"]}    e2e-xns-issue-a-${suffix}
    ${issue_b}=    Create Issue    ${project_b["id"]}    e2e-xns-issue-b-${suffix}
    ${issue_m1}=    Create Issue    ${project_a["id"]}    e2e-xns-issue-m1-${suffix}
    ${issue_m2}=    Create Issue    ${project_a["id"]}    e2e-xns-issue-m2-${suffix}
    ${issue_c}=    Create Issue    ${project_b["id"]}    e2e-xns-issue-c-${suffix}
    Link Issues    ${project_a["id"]}    ${issue_a["iid"]}    ${project_b["id"]}    ${issue_b["iid"]}
    Link Issues    ${project_a["id"]}    ${issue_a["iid"]}    ${project_a["id"]}    ${issue_m1["iid"]}
    Link Issues    ${project_a["id"]}    ${issue_m1["iid"]}    ${project_a["id"]}    ${issue_m2["iid"]}
    Link Issues    ${project_a["id"]}    ${issue_m2["iid"]}    ${project_b["id"]}    ${issue_c["iid"]}
    # The CLOSES edge (slowest path) indexes while tests 1-7 run; merging
    # closes issue B, which is safe — tests 1-7 assert ids, never state.
    ${mr_a}=    Open Closing Merge Request    ${project_a["id"]}
    ...    ${project_b["path_with_namespace"]}    ${issue_b["iid"]}
    Set Suite Variable    ${XNS_MR_ID_A}    ${mr_a["id"]}
    Set Suite Variable    ${XNS_GROUP_ID_A}    ${group_a["id"]}
    Set Suite Variable    ${XNS_PROJECT_ID_A}    ${project_a["id"]}
    Set Suite Variable    ${XNS_PROJECT_ID_B}    ${project_b["id"]}
    Set Suite Variable    ${XNS_ISSUE_ID_A}    ${issue_a["id"]}
    Set Suite Variable    ${XNS_ISSUE_ID_B}    ${issue_b["id"]}
    Set Suite Variable    ${XNS_ISSUE_ID_C}    ${issue_c["id"]}
    Set Suite Variable    ${XNS_PROJECT_B_FULL_PATH}    ${project_b["path_with_namespace"]}
    Set Suite Variable    ${XNS_ISSUE_B_IID}    ${issue_b["iid"]}
    Wait For Node Indexed Within Budget    Project    ${project_a["id"]}    e2e-xns-prj-a-${suffix}
    Wait For Node Indexed Within Budget    WorkItem    ${issue_b["id"]}
    ...    e2e-xns-issue-b-${suffix}    label_field=title
    Wait For Edge Indexed Within Budget    Group    ${XNS_GROUP_ID_A}    CONTAINS
    ...    Project    ${XNS_PROJECT_ID_A}
    Wait For Edge Indexed Within Budget    WorkItem    ${XNS_ISSUE_ID_A}    IN_PROJECT
    ...    Project    ${XNS_PROJECT_ID_A}
    Wait For Edge Indexed Within Budget    WorkItem    ${XNS_ISSUE_ID_A}    RELATED_TO
    ...    WorkItem    ${XNS_ISSUE_ID_B}
    Wait For Edge Indexed Within Budget    WorkItem    ${XNS_ISSUE_ID_A}    RELATED_TO
    ...    WorkItem    ${issue_m1["id"]}
    Wait For Edge Indexed Within Budget    WorkItem    ${issue_m1["id"]}    RELATED_TO
    ...    WorkItem    ${issue_m2["id"]}
    Wait For Edge Indexed Within Budget    WorkItem    ${issue_m2["id"]}    RELATED_TO
    ...    WorkItem    ${XNS_ISSUE_ID_C}

Seed Cross Project Closing MR
    [Documentation]    The MR itself is opened by Seed Cross Namespace Fixture; this setup only
    ...                waits for the MergeRequest CLOSES WorkItem edge, which has usually caught
    ...                up while tests 1-7 ran. Shared by both closing test cases.
    Start Indexing Budget    400
    Wait For Edge Indexed Within Budget    MergeRequest    ${XNS_MR_ID_A}    CLOSES
    ...    WorkItem    ${XNS_ISSUE_ID_B}
