*** Settings ***
Documentation       The orbit CLI against the e2e GitLab: the service status, the scoped status of
...                 the shared group, and the schema. The CLI reads each from GraphQL first and
...                 falls back to the REST routes on a GitLab image that predates the fields, so
...                 the pinned lanes prove both paths. The robot pod gets the CLI from the
...                 orbit-cli-e2e image (ORBIT_CLI_DIR); without it the suite skips.

Library             Process
Resource            gitlab.resource
Resource            orbit.resource

Suite Setup         Run Keywords
...                 Skip If    not $ORBIT_CLI_DIR    The robot pod has no orbit CLI (ORBIT_CLI_DIR is unset)
...                 AND    Attach To Shared Fixture


*** Variables ***
${ORBIT_CLI_DIR}    %{ORBIT_CLI_DIR=}


*** Test Cases ***
CLI Reports Service Health
    [Tags]    cli
    ${status}=    Orbit CLI JSON    status
    Should Be True    $status.get("state") == "HEALTHY" or $status.get("status") == "healthy"    ${status}

CLI Reports The Shared Group Status
    [Tags]    cli
    ${status}=    Orbit CLI JSON    status    --full-path    ${SHARED_NAMESPACE_NAME}
    Should Be True    $status.get("domains") or $status.get("itemCounts", {}).get("domains")    ${status}

CLI Lists The Schema
    [Tags]    cli
    ${out}=    Orbit CLI    schema    Project
    Should Contain    ${out}    "name": "Project"


*** Keywords ***
Orbit CLI JSON
    [Arguments]    @{args}
    ${out}=    Orbit CLI    @{args}
    ${value}=    Evaluate    json.loads($out)    modules=json
    RETURN    ${value}

Orbit CLI
    [Documentation]    Run the orbit CLI with the bot PAT against the e2e GitLab and return stdout.
    [Arguments]    @{args}
    ${out}=    Wait Until Keyword Succeeds    30s    5s    Run Orbit CLI    @{args}
    RETURN    ${out}

Run Orbit CLI
    [Arguments]    @{args}
    ${result}=    Run Process    ${ORBIT_CLI_DIR}/orbit    @{args}    timeout=120s
    ...    env:GITLAB_URL=${GITLAB_URL}    env:GITLAB_TOKEN=${GITLAB_PAT}
    ...    env:LD_LIBRARY_PATH=${ORBIT_CLI_DIR}    env:ORBIT_TELEMETRY_ENABLED=false
    Should Be Equal As Integers    ${result.rc}    0    orbit ${args} exited ${result.rc}: ${result.stderr}
    RETURN    ${result.stdout}
