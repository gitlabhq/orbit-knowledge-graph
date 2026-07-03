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
    ${rel}=    Create Dictionary    id=rel    entity=WorkItem    columns=${{["id", "title"]}}
    ${scope}=    Create Dictionary    op=eq    value=${{int($XNS_PROJECT_ID_A)}}
    ${filters}=    Create Dictionary    id=${scope}
    ${proj}=    Create Dictionary    id=p    entity=Project    filters=${filters}
    ${wi}=    Create Dictionary    id=wi    entity=WorkItem    columns=${{["id"]}}
    ${in_project}=    Create Dictionary    type=IN_PROJECT    from=wi    to=p
    ${related}=    Create Dictionary    type=RELATED_TO    from=wi    to=rel
    ${query}=    Create Dictionary    query_type=traversal
    ...    nodes=${{[$proj, $wi, $rel]}}    relationships=${{[$in_project, $related]}}    limit=${100}
    Wait Until Result Node Ids Contain    ${query}    ${XNS_ISSUE_ID_A}    ${XNS_ISSUE_ID_B}

Project Scoped Neighbors Returns Cross Namespace Related Issue
    [Tags]    cross-namespace
    ${center}=    Create Dictionary    id=wi    entity=WorkItem    node_ids=${{[int($XNS_ISSUE_ID_A)]}}
    ${dir}=    Create Dictionary    node=wi    direction=both
    ${query}=    Create Dictionary    query_type=neighbors    node=${center}    neighbors=${dir}    limit=${100}
    Wait Until Result Node Ids Contain    ${query}    ${XNS_ISSUE_ID_B}

Multi Hop Neighbors Reach Cross Namespace Issue At Three Hops
    [Documentation]    The neighbors query type is 1-hop by schema, so a 3-hop neighborhood is
    ...                expressed as a variable-length (max_hops=3) RELATED_TO traversal. issue_c is
    ...                reachable from issue_a only via a 3-hop chain whose last hop crosses into a
    ...                different top-level namespace; the project-A tight prefix must not prune it.
    [Tags]    cross-namespace
    ${scope}=    Create Dictionary    op=eq    value=${{int($XNS_PROJECT_ID_A)}}
    ${filters}=    Create Dictionary    id=${scope}
    ${p}=    Create Dictionary    id=p    entity=Project    filters=${filters}
    ${a}=    Create Dictionary    id=a    entity=WorkItem
    ${b}=    Create Dictionary    id=b    entity=WorkItem    columns=${{["id"]}}
    ${r1}=    Create Dictionary    type=IN_PROJECT    from=a    to=p
    ${r2}=    Create Dictionary    type=RELATED_TO    from=a    to=b    max_hops=${3}
    ${query}=    Create Dictionary    query_type=traversal
    ...    nodes=${{[$p, $a, $b]}}    relationships=${{[$r1, $r2]}}    limit=${100}
    Wait Until Result Node Ids Contain    ${query}    ${XNS_ISSUE_ID_C}

Project Scoped Multi Hop Traversal Reaches Cross Namespace Project
    [Tags]    cross-namespace
    ${scope}=    Create Dictionary    op=eq    value=${{int($XNS_PROJECT_ID_A)}}
    ${filters}=    Create Dictionary    id=${scope}
    ${p}=    Create Dictionary    id=p    entity=Project    filters=${filters}
    ${wi}=    Create Dictionary    id=wi    entity=WorkItem
    ${rel}=    Create Dictionary    id=rel    entity=WorkItem    columns=${{["id"]}}
    ${p2}=    Create Dictionary    id=p2    entity=Project    columns=${{["id"]}}
    ${r1}=    Create Dictionary    type=IN_PROJECT    from=wi    to=p
    ${r2}=    Create Dictionary    type=RELATED_TO    from=wi    to=rel
    ${r3}=    Create Dictionary    type=IN_PROJECT    from=rel    to=p2
    ${query}=    Create Dictionary    query_type=traversal
    ...    nodes=${{[$p, $wi, $rel, $p2]}}    relationships=${{[$r1, $r2, $r3]}}    limit=${100}
    Wait Until Result Node Ids Contain    ${query}    ${XNS_ISSUE_ID_B}    ${XNS_PROJECT_ID_B}

Project Scoped Multi Hop Aggregation Counts Cross Namespace Project
    [Tags]    cross-namespace
    ${scope}=    Create Dictionary    op=eq    value=${{int($XNS_PROJECT_ID_A)}}
    ${filters}=    Create Dictionary    id=${scope}
    ${p}=    Create Dictionary    id=p    entity=Project    filters=${filters}
    ${wi}=    Create Dictionary    id=wi    entity=WorkItem
    ${rel}=    Create Dictionary    id=rel    entity=WorkItem
    ${p2}=    Create Dictionary    id=p2    entity=Project
    ${r1}=    Create Dictionary    type=IN_PROJECT    from=wi    to=p
    ${r2}=    Create Dictionary    type=RELATED_TO    from=wi    to=rel
    ${r3}=    Create Dictionary    type=IN_PROJECT    from=rel    to=p2
    ${agg}=    Create Dictionary    function=count    target=p2    alias=xns_project_count
    ${query}=    Create Dictionary    query_type=aggregation
    ...    nodes=${{[$p, $wi, $rel, $p2]}}    relationships=${{[$r1, $r2, $r3]}}    aggregations=${{[$agg]}}
    Wait Until Aggregation At Least    ${query}    xns_project_count    1

Path Finding Within Scoped Project Returns The Path
    [Tags]    cross-namespace
    ${start}=    Create Dictionary    id=start    entity=WorkItem    node_ids=${{[int($XNS_ISSUE_ID_A)]}}
    ${end}=    Create Dictionary    id=end    entity=Project    node_ids=${{[int($XNS_PROJECT_ID_A)]}}
    ${path}=    Create Dictionary    type=shortest    from=start    to=end    max_depth=${2}    rel_types=${{["IN_PROJECT"]}}
    ${query}=    Create Dictionary    query_type=path_finding    nodes=${{[$start, $end]}}    path=${path}
    Wait Until Result Node Ids Contain    ${query}    ${XNS_ISSUE_ID_A}    ${XNS_PROJECT_ID_A}

Group Scoped Multi Hop Traversal Returns Cross Namespace Related Issue
    [Tags]    cross-namespace
    ${scope}=    Create Dictionary    op=eq    value=${{int($XNS_GROUP_ID_A)}}
    ${filters}=    Create Dictionary    id=${scope}
    ${g}=    Create Dictionary    id=g    entity=Group    filters=${filters}
    ${p}=    Create Dictionary    id=p    entity=Project
    ${wi}=    Create Dictionary    id=wi    entity=WorkItem
    ${rel}=    Create Dictionary    id=rel    entity=WorkItem    columns=${{["id"]}}
    ${r1}=    Create Dictionary    type=CONTAINS    from=g    to=p
    ${r2}=    Create Dictionary    type=IN_PROJECT    from=wi    to=p
    ${r3}=    Create Dictionary    type=RELATED_TO    from=wi    to=rel
    ${query}=    Create Dictionary    query_type=traversal
    ...    nodes=${{[$g, $p, $wi, $rel]}}    relationships=${{[$r1, $r2, $r3]}}    limit=${100}
    Wait Until Result Node Ids Contain    ${query}    ${XNS_ISSUE_ID_B}

Group Scoped Multi Hop Aggregation Counts Cross Namespace Related Issue
    [Tags]    cross-namespace
    ${scope}=    Create Dictionary    op=eq    value=${{int($XNS_GROUP_ID_A)}}
    ${filters}=    Create Dictionary    id=${scope}
    ${g}=    Create Dictionary    id=g    entity=Group    filters=${filters}
    ${p}=    Create Dictionary    id=p    entity=Project
    ${wi}=    Create Dictionary    id=wi    entity=WorkItem
    ${rel}=    Create Dictionary    id=rel    entity=WorkItem
    ${r1}=    Create Dictionary    type=CONTAINS    from=g    to=p
    ${r2}=    Create Dictionary    type=IN_PROJECT    from=wi    to=p
    ${r3}=    Create Dictionary    type=RELATED_TO    from=wi    to=rel
    ${agg}=    Create Dictionary    function=count    target=rel    alias=related_count
    ${query}=    Create Dictionary    query_type=aggregation
    ...    nodes=${{[$g, $p, $wi, $rel]}}    relationships=${{[$r1, $r2, $r3]}}    aggregations=${{[$agg]}}
    Wait Until Aggregation At Least    ${query}    related_count    1

Project Scoped Traversal Returns Cross Namespace Closed Issue
    [Tags]    cross-namespace
    [Setup]    Seed Cross Project Closing MR
    ${scope}=    Create Dictionary    op=eq    value=${{int($XNS_PROJECT_ID_A)}}
    ${filters}=    Create Dictionary    id=${scope}
    ${p}=    Create Dictionary    id=p    entity=Project    filters=${filters}
    ${mr}=    Create Dictionary    id=mr    entity=MergeRequest
    ${issue}=    Create Dictionary    id=issue    entity=WorkItem    columns=${{["id"]}}
    ${r1}=    Create Dictionary    type=IN_PROJECT    from=mr    to=p
    ${r2}=    Create Dictionary    type=CLOSES    from=mr    to=issue
    ${query}=    Create Dictionary    query_type=traversal
    ...    nodes=${{[$p, $mr, $issue]}}    relationships=${{[$r1, $r2]}}    limit=${100}
    Wait Until Result Node Ids Contain    ${query}    ${XNS_ISSUE_ID_B}

Group Scoped Multi Hop Traversal Returns Cross Namespace Closed Issue
    [Tags]    cross-namespace
    [Setup]    Seed Cross Project Closing MR
    ${scope}=    Create Dictionary    op=eq    value=${{int($XNS_GROUP_ID_A)}}
    ${filters}=    Create Dictionary    id=${scope}
    ${g}=    Create Dictionary    id=g    entity=Group    filters=${filters}
    ${p}=    Create Dictionary    id=p    entity=Project
    ${mr}=    Create Dictionary    id=mr    entity=MergeRequest
    ${issue}=    Create Dictionary    id=issue    entity=WorkItem    columns=${{["id"]}}
    ${r1}=    Create Dictionary    type=CONTAINS    from=g    to=p
    ${r2}=    Create Dictionary    type=IN_PROJECT    from=mr    to=p
    ${r3}=    Create Dictionary    type=CLOSES    from=mr    to=issue
    ${query}=    Create Dictionary    query_type=traversal
    ...    nodes=${{[$g, $p, $mr, $issue]}}    relationships=${{[$r1, $r2, $r3]}}    limit=${100}
    Wait Until Result Node Ids Contain    ${query}    ${XNS_ISSUE_ID_B}


*** Keywords ***
Seed Cross Namespace Fixture
    ${suffix}=    Random Suffix
    Start Indexing Budget    300
    ${group_a}=    Create Group    e2e-xns-a-${suffix}
    ${group_b}=    Create Group    e2e-xns-b-${suffix}
    Enable Knowledge Graph    ${group_a["id"]}
    Enable Knowledge Graph    ${group_b["id"]}
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
    # Open the closing MR here so the CLOSES edge (the slowest indexing path)
    # catches up while tests 1-7 run; only its wait stays in the test setup.
    # Merging closes issue B, which is safe: tests 1-7 assert id membership,
    # never state.
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
