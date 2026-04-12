*** Settings ***
Documentation       Phase 2: Verify the Orbit query API is accessible.
...                 These are basic connectivity tests. Full query validation
...                 requires an indexed graph (future work).

Resource            resources/common.robot

Suite Setup         Suite Init


*** Test Cases ***
Orbit API Endpoint Exists
    [Documentation]    Verify the Orbit query endpoint returns a valid response
    [Tags]    api    critical

    ${query}=    Evaluate    {"query_type": "search", "node": {"entity_type": "Group", "columns": ["name"]}}
    ${resp}=    Orbit Query    ${query}
    # A 200 with results or a 400/422 with error message both prove the endpoint works
    ${status}=    Convert To Integer    ${resp.status_code}
    Should Be True    ${status} < 500    msg=Orbit API returned server error: ${resp.text}

    Log    Orbit API responded with HTTP ${status}


*** Keywords ***
Suite Init
    Create API Session
