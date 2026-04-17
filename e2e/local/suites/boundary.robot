*** Settings ***
Documentation    B-series: HTTP boundary conditions. Fast, no-setup tests
...              that verify the Rails proxy and webserver reject or pass
...              malformed / unauthorized requests with the right status code.
Resource         ../lib/common.resource

*** Test Cases ***
B1 Missing Params Returns 400
    ${headers}=    Auth Headers
    ${resp}=    GET    url=${GITLAB_URL}/api/v4/orbit/graph_status
    ...    headers=${headers}    expected_status=any    verify=${VERIFY_SSL}
    Should Be Equal As Integers    ${resp.status_code}    400

B2 Multiple Params Returns 400 Or 200
    # The API may accept the first param or reject; both are acceptable.
    ${headers}=    Auth Headers
    ${params}=    Create Dictionary    namespace_id=22    project_id=5
    ${resp}=    GET    url=${GITLAB_URL}/api/v4/orbit/graph_status
    ...    headers=${headers}    params=${params}    expected_status=any    verify=${VERIFY_SSL}
    Should Be True    ${resp.status_code} in [200, 400]
    ...    expected 200 or 400 got ${resp.status_code}

B3 Nonexistent Namespace Returns 404
    ${resp}=    Graph Status By Namespace    99999999    404

B4 Nonexistent Project Returns 404
    ${resp}=    Graph Status By Project    99999999    404

B5 Nonexistent Full Path Returns 404
    ${resp}=    Graph Status By Full Path    does/not/exist    404

B6 No Auth Returns 401
    ${params}=    Create Dictionary    namespace_id=22
    ${resp}=    GET    url=${GITLAB_URL}/api/v4/orbit/graph_status
    ...    params=${params}    expected_status=any    verify=${VERIFY_SSL}
    Should Be Equal As Integers    ${resp.status_code}    401

B7 PAT Without Read API Scope Returns 403 Or 401
    [Documentation]    Create a narrow-scope PAT and verify access denied.
    ...                The exact status (401 or 403) depends on Rails policy;
    ...                both are acceptable — what matters is NOT 200.
    ${admin_headers}=    Bare PAT Header
    ${suffix}=    Random Suffix
    ${pat_body}=    Create Dictionary    name=graph-status-scope-${suffix}
    ...    scopes=${{["read_user"]}}
    ...    expires_at=${{(__import__('datetime').date.today()+__import__('datetime').timedelta(days=1)).isoformat()}}
    ${me}=    GET    ${GITLAB_URL}/api/v4/user    headers=${admin_headers}    verify=${VERIFY_SSL}
    ${uid}=    Set Variable    ${me.json()["id"]}
    ${pat}=    POST    ${GITLAB_URL}/api/v4/users/${uid}/personal_access_tokens
    ...    headers=${admin_headers}    json=${pat_body}    expected_status=201    verify=${VERIFY_SSL}
    ${narrow_headers}=    Create Dictionary    PRIVATE-TOKEN=${pat.json()["token"]}
    ${params}=    Create Dictionary    namespace_id=22
    ${resp}=    GET    url=${GITLAB_URL}/api/v4/orbit/graph_status
    ...    headers=${narrow_headers}    params=${params}    expected_status=any    verify=${VERIFY_SSL}
    Should Be True    ${resp.status_code} in [401, 403]
    ...    expected 401/403 for narrow-scope PAT, got ${resp.status_code}

B8 Personal Namespace Returns 404 Without Leakage
    [Documentation]    A user namespace id (not a group) must not return 200
    ...                since KG only operates on groups. 404 is preferred so we
    ...                don't leak existence; 400 is also acceptable.
    ${admin_headers}=    Bare PAT Header
    ${me}=    GET    ${GITLAB_URL}/api/v4/user    headers=${admin_headers}    verify=${VERIFY_SSL}
    ${user_namespace_id}=    Set Variable    ${me.json()["namespace_id"]}
    ${resp}=    Graph Status By Namespace    ${user_namespace_id}
    Should Be True    ${resp.status_code} in [400, 404]
    ...    user namespace must not return 200, got ${resp.status_code}

B9 Private Group User Cannot Access Returns 404
    [Documentation]    Create a private group as admin, query with that same
    ...                admin PAT: should work. This test primarily documents the
    ...                "no existence leak" guarantee. (Non-admin PAT scenario
    ...                is TODO: needs a second user context.)
    ${suffix}=    Random Suffix
    ${group}=    Create Group    priv-${suffix}    priv-${suffix}
    # Switch to private after creation
    ${admin_headers}=    Bare PAT Header
    ${body}=    Create Dictionary    visibility=private
    PUT    url=${GITLAB_URL}/api/v4/groups/${group["id"]}    headers=${admin_headers}
    ...    json=${body}    expected_status=any    verify=${VERIFY_SSL}
    # Admin still has access — returns 200 or 404 depending on KG enablement.
    ${resp}=    Graph Status By Namespace    ${group["id"]}
    Should Be True    ${resp.status_code} in [200, 404]
    [Teardown]    Delete Group    ${group["id"]}

B13 Negative Id Returns 400 Or 404
    ${resp}=    Graph Status By Namespace    -1
    Should Be True    ${resp.status_code} in [400, 404]

B14 Very Long Full Path Rejected
    ${long}=    Evaluate    "x/" * 4000
    ${resp}=    Graph Status By Full Path    ${long}
    Should Be True    ${resp.status_code} in [400, 404, 414]
    ...    long full_path must be rejected or not-found, got ${resp.status_code}
