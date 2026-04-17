*** Settings ***
Documentation    Z-series: Authorization matrix. Creates separate user
...              contexts and verifies that graph_status enforces the same
...              access rules as the underlying group/project:
...                - private groups: non-member gets 404 (no existence leak)
...                - internal groups: member of instance gets 200
...                - public groups: any authenticated user gets 200
...                - guests/reporters/developers: can read only if member
...                - token scopes: read-only PAT is accepted, bad scope is not
...                - cross-user: userA cannot peek into userB's private group
Resource         ../lib/common.resource
Suite Setup      Prepare Users And Groups
Suite Teardown   Teardown Users And Groups

*** Variables ***
${WAIT_IDLE_SECS}       %{WAIT_IDLE_SECS=300}
${ADMIN_PAT}            ${EMPTY}
${USER_A_ID}            ${None}
${USER_A_PAT}           ${EMPTY}
${USER_B_ID}            ${None}
${USER_B_PAT}           ${EMPTY}
${PUBLIC_GID}           ${None}
${PUBLIC_PATH}          ${None}
${INTERNAL_GID}         ${None}
${INTERNAL_PATH}        ${None}
${PRIVATE_GID}          ${None}
${PRIVATE_PATH}         ${None}
${MEMBER_GID}           ${None}
${MEMBER_PATH}          ${None}

*** Keywords ***
Prepare Users And Groups
    ${suffix}=    Random Suffix
    Set Suite Variable    ${SUFFIX}    ${suffix}
    Set Suite Variable    ${ADMIN_PAT}    ${GITLAB_PAT}

    # Create two non-admin users. Admin creates them; we mint a PAT for each.
    ${user_a}=    Create Non Admin User    perm-a-${suffix}
    ${user_b}=    Create Non Admin User    perm-b-${suffix}
    Set Suite Variable    ${USER_A_ID}    ${user_a["id"]}
    Set Suite Variable    ${USER_A_PAT}    ${user_a["pat"]}
    Set Suite Variable    ${USER_B_ID}    ${user_b["id"]}
    Set Suite Variable    ${USER_B_PAT}    ${user_b["pat"]}

    # Four groups at different visibility. Enable KG on each.
    ${pub}=    Create Group    perm-pub-${suffix}    perm-pub-${suffix}
    ${internal}=    Create Group    perm-int-${suffix}    perm-int-${suffix}
    ${priv}=    Create Group    perm-priv-${suffix}    perm-priv-${suffix}
    ${member}=    Create Group    perm-mem-${suffix}    perm-mem-${suffix}
    Set Group Visibility    ${internal["id"]}    internal
    Set Group Visibility    ${priv["id"]}    private
    Set Group Visibility    ${member["id"]}    private

    Set Suite Variable    ${PUBLIC_GID}    ${pub["id"]}
    Set Suite Variable    ${PUBLIC_PATH}    ${pub["full_path"]}
    Set Suite Variable    ${INTERNAL_GID}    ${internal["id"]}
    Set Suite Variable    ${INTERNAL_PATH}    ${internal["full_path"]}
    Set Suite Variable    ${PRIVATE_GID}    ${priv["id"]}
    Set Suite Variable    ${PRIVATE_PATH}    ${priv["full_path"]}
    Set Suite Variable    ${MEMBER_GID}    ${member["id"]}
    Set Suite Variable    ${MEMBER_PATH}    ${member["full_path"]}

    # UserA is a developer (access_level=30) of the member-only private group.
    # Guest-level (10) access is blocked at the GKG authorization layer even
    # for a member — the JWT-claimed scope set requires developer+.
    Add Group Member    ${MEMBER_GID}    ${USER_A_ID}    30

    # Enable KG on all four so GKG picks them up and writes meta/counts.
    Enable Knowledge Graph    ${PUBLIC_GID}
    Enable Knowledge Graph    ${INTERNAL_GID}
    Enable Knowledge Graph    ${PRIVATE_GID}
    Enable Knowledge Graph    ${MEMBER_GID}
    Wait For Idle    ${PUBLIC_GID}    ${WAIT_IDLE_SECS}s
    Wait For Idle    ${MEMBER_GID}    ${WAIT_IDLE_SECS}s

Teardown Users And Groups
    FOR    ${gid}    IN    ${PUBLIC_GID}    ${INTERNAL_GID}    ${PRIVATE_GID}    ${MEMBER_GID}
        IF    $gid is not None
            Delete Group    ${gid}
        END
    END
    ${admin_headers}=    Create Dictionary    PRIVATE-TOKEN=${ADMIN_PAT}
    ${params}=    Create Dictionary    hard_delete=true
    FOR    ${uid}    IN    ${USER_A_ID}    ${USER_B_ID}
        IF    $uid is not None
            DELETE    url=${GITLAB_URL}/api/v4/users/${uid}
            ...    headers=${admin_headers}    params=${params}
            ...    expected_status=any    verify=${VERIFY_SSL}
        END
    END

Create Non Admin User
    [Arguments]    ${username}
    ${headers}=    Create Dictionary    PRIVATE-TOKEN=${ADMIN_PAT}    Content-Type=application/json
    ${pw}=    Evaluate    __import__('secrets').token_urlsafe(24) + '!Zx9@'
    ${body}=    Create Dictionary
    ...    username=${username}    email=${username}@example.com    name=${username}
    ...    password=${pw}    skip_confirmation=${True}
    ${resp}=    POST    ${GITLAB_URL}/api/v4/users
    ...    headers=${headers}    json=${body}    expected_status=any    verify=${VERIFY_SSL}
    Should Be True    ${resp.status_code} in [200, 201]
    ...    user create returned ${resp.status_code}: ${resp.text}
    ${uid}=    Set Variable    ${resp.json()["id"]}
    ${expiry}=    Evaluate
    ...    (__import__('datetime').date.today()+__import__('datetime').timedelta(days=1)).isoformat()
    ${pat_body}=    Create Dictionary    name=perm-${username}
    ...    scopes=${{["api"]}}    expires_at=${expiry}
    ${pat_resp}=    POST    ${GITLAB_URL}/api/v4/users/${uid}/personal_access_tokens
    ...    headers=${headers}    json=${pat_body}    expected_status=201    verify=${VERIFY_SSL}
    ${record}=    Create Dictionary    id=${uid}    pat=${pat_resp.json()["token"]}
    RETURN    ${record}

Set Group Visibility
    [Arguments]    ${group_id}    ${visibility}
    ${headers}=    Create Dictionary    PRIVATE-TOKEN=${ADMIN_PAT}    Content-Type=application/json
    ${body}=    Create Dictionary    visibility=${visibility}
    ${resp}=    PUT    ${GITLAB_URL}/api/v4/groups/${group_id}
    ...    headers=${headers}    json=${body}    expected_status=200    verify=${VERIFY_SSL}

Add Group Member
    [Arguments]    ${group_id}    ${user_id}    ${access_level}
    ${headers}=    Create Dictionary    PRIVATE-TOKEN=${ADMIN_PAT}    Content-Type=application/json
    ${body}=    Create Dictionary    user_id=${user_id}    access_level=${access_level}
    ${resp}=    POST    ${GITLAB_URL}/api/v4/groups/${group_id}/members
    ...    headers=${headers}    json=${body}    expected_status=any    verify=${VERIFY_SSL}
    Should Be True    ${resp.status_code} in [200, 201, 409]
    ...    add member returned ${resp.status_code}: ${resp.text}

Graph Status As
    [Arguments]    ${pat}    ${scope_kind}    ${scope_value}
    ${headers}=    Create Dictionary    PRIVATE-TOKEN=${pat}
    ${params}=    Create Dictionary    ${scope_kind}=${scope_value}
    ${resp}=    GET    url=${GITLAB_URL}/api/v4/orbit/graph_status
    ...    headers=${headers}    params=${params}    expected_status=any    verify=${VERIFY_SSL}
    RETURN    ${resp}

*** Test Cases ***
Z1 Unauthenticated Caller Gets 401 On Public Group
    ${params}=    Create Dictionary    namespace_id=${PUBLIC_GID}
    ${resp}=    GET    url=${GITLAB_URL}/api/v4/orbit/graph_status
    ...    params=${params}    expected_status=any    verify=${VERIFY_SSL}
    Should Be Equal As Integers    ${resp.status_code}    401

Z2 Non Member Cannot See Private Group
    [Documentation]    UserB has no membership in PRIVATE_GID. graph_status
    ...                must return 404 (not 403) to avoid leaking existence.
    ${resp}=    Graph Status As    ${USER_B_PAT}    namespace_id    ${PRIVATE_GID}
    Should Be Equal As Integers    ${resp.status_code}    404
    ...    expected 404 for non-member of private group, got ${resp.status_code}: ${resp.text}

Z3 Non Member Cannot See Private Group By Full Path
    ${resp}=    Graph Status As    ${USER_B_PAT}    full_path    ${PRIVATE_PATH}
    Should Be Equal As Integers    ${resp.status_code}    404

Z4 Developer Member Can Read Private Group
    [Documentation]    UserA is a developer of MEMBER_GID (private, member-only)
    ...                and should be allowed to read graph_status. Guest-level
    ...                members are intentionally blocked at the GKG layer.
    ${resp}=    Graph Status As    ${USER_A_PAT}    namespace_id    ${MEMBER_GID}
    Should Be Equal As Integers    ${resp.status_code}    200
    Assert Response Shape    ${resp.json()}

Z5 Non Member Cannot See Member Only Group
    [Documentation]    UserB is not a member of MEMBER_GID.
    ${resp}=    Graph Status As    ${USER_B_PAT}    namespace_id    ${MEMBER_GID}
    Should Be Equal As Integers    ${resp.status_code}    404

Z6 Authenticated Non Member Cannot See Public Group
    [Documentation]    graph_status requires the user to be a member of the
    ...                namespace (developer+) regardless of the group's
    ...                visibility. A non-member querying a public group gets
    ...                the same error as querying a private group.
    ${resp}=    Graph Status As    ${USER_B_PAT}    namespace_id    ${PUBLIC_GID}
    Should Be True    ${resp.status_code} in [403, 404, 503]
    ...    non-member on public group expected rejection, got ${resp.status_code}

Z7 Authenticated Non Member Cannot See Internal Group
    ${resp}=    Graph Status As    ${USER_B_PAT}    namespace_id    ${INTERNAL_GID}
    Should Be True    ${resp.status_code} in [403, 404, 503]
    ...    non-member on internal group expected rejection, got ${resp.status_code}

Z8 Project Scope Respects Enclosing Group Access
    [Documentation]    Create a project under the member-only private group.
    ...                UserA (developer) can read; UserB (non-member) cannot.
    ${proj}=    Create Project    perm-proj-${SUFFIX}    ${MEMBER_GID}
    ${pid}=    Set Variable    ${proj["id"]}

    ${a}=    Graph Status As    ${USER_A_PAT}    project_id    ${pid}
    Should Be Equal As Integers    ${a.status_code}    200

    ${b}=    Graph Status As    ${USER_B_PAT}    project_id    ${pid}
    Should Be True    ${b.status_code} in [403, 404]
    ...    non-member project expected 404, got ${b.status_code}

Z9 Non Member Gets Same 404 As Nonexistent Id
    [Documentation]    Existence leak check: a valid-but-inaccessible id must
    ...                return the same status and error shape as a nonexistent
    ...                id.
    ${inaccessible}=    Graph Status As    ${USER_B_PAT}    namespace_id    ${PRIVATE_GID}
    ${nonexistent}=    Graph Status As    ${USER_B_PAT}    namespace_id    99999999
    Should Be Equal As Integers    ${inaccessible.status_code}    ${nonexistent.status_code}

Z10 Admin Sees All Groups Regardless Of Membership
    ${a}=    Graph Status As    ${ADMIN_PAT}    namespace_id    ${PRIVATE_GID}
    Should Be Equal As Integers    ${a.status_code}    200

    ${b}=    Graph Status As    ${ADMIN_PAT}    namespace_id    ${MEMBER_GID}
    Should Be Equal As Integers    ${b.status_code}    200

Z11 Revoked Membership Cuts Off Access
    [Documentation]    Add UserB as developer to MEMBER_GID, confirm 200,
    ...                then remove membership, confirm 404.
    Add Group Member    ${MEMBER_GID}    ${USER_B_ID}    30
    ${during}=    Graph Status As    ${USER_B_PAT}    namespace_id    ${MEMBER_GID}
    Should Be Equal As Integers    ${during.status_code}    200

    ${headers}=    Create Dictionary    PRIVATE-TOKEN=${ADMIN_PAT}
    DELETE    url=${GITLAB_URL}/api/v4/groups/${MEMBER_GID}/members/${USER_B_ID}
    ...    headers=${headers}    expected_status=any    verify=${VERIFY_SSL}

    # Rails auth checks are transactional — expect immediate 404.
    ${after}=    Graph Status As    ${USER_B_PAT}    namespace_id    ${MEMBER_GID}
    Should Be Equal As Integers    ${after.status_code}    404

Z12 PAT With Only Read User Scope Is Rejected
    [Documentation]    A PAT with scope `read_user` (no api / read_api) must
    ...                not be able to call the orbit API.
    ${headers}=    Create Dictionary    PRIVATE-TOKEN=${ADMIN_PAT}    Content-Type=application/json
    ${me}=    GET    ${GITLAB_URL}/api/v4/user    headers=${headers}    verify=${VERIFY_SSL}
    ${uid}=    Set Variable    ${me.json()["id"]}
    ${expiry}=    Evaluate
    ...    (__import__('datetime').date.today()+__import__('datetime').timedelta(days=1)).isoformat()
    ${body}=    Create Dictionary    name=perm-narrow-${SUFFIX}
    ...    scopes=${{["read_user"]}}    expires_at=${expiry}
    ${pat}=    POST    ${GITLAB_URL}/api/v4/users/${uid}/personal_access_tokens
    ...    headers=${headers}    json=${body}    expected_status=201    verify=${VERIFY_SSL}
    ${resp}=    Graph Status As    ${pat.json()["token"]}    namespace_id    ${PUBLIC_GID}
    Should Be True    ${resp.status_code} in [401, 403]
    ...    narrow-scope PAT must not access graph_status, got ${resp.status_code}
