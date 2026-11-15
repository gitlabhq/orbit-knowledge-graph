*** Settings ***
Documentation       The orbit CLI against the e2e GitLab: the service status, the scoped status of
...                 the shared group, and the schema, all read over GraphQL. The robot pod gets the
...                 CLI from the orbit-cli-e2e image (ORBIT_CLI_DIR); without it the suite skips.

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
    Wait Until Keyword Succeeds    60s    10s    CLI Answer Holds
    ...    $answer.get("state") == "HEALTHY"    status

CLI Reports The Shared Group Status
    [Tags]    cli
    Wait Until Keyword Succeeds    60s    10s    CLI Answer Holds
    ...    bool(($answer.get("itemCounts") or {}).get("domains"))
    ...    status    --full-path    ${SHARED_NAMESPACE_NAME}

CLI Lists The Schema
    [Tags]    cli
    Wait Until Keyword Succeeds    60s    10s    CLI Answer Holds
    ...    "Project" in [n["name"] for n in $answer.get("nodes") or []]    schema    Project


*** Keywords ***
CLI Answer Holds
    [Arguments]    ${condition}    @{args}
    ${answer}=    Orbit CLI JSON    @{args}
    Should Be True    ${condition}    orbit ${args}: ${answer}

Orbit CLI JSON
    [Arguments]    @{args}
    ${out}=    Orbit CLI    @{args}
    ${value}=    Evaluate    json.loads($out)    modules=json
    RETURN    ${value}

Orbit CLI
    [Arguments]    @{args}
    ${result}=    Run Process    ${ORBIT_CLI_DIR}/orbit    @{args}    timeout=120s
    ...    env:GITLAB_URL=${GITLAB_URL}    env:GITLAB_TOKEN=${GITLAB_PAT}
    ...    env:LD_LIBRARY_PATH=${ORBIT_CLI_DIR}    env:ORBIT_TELEMETRY_ENABLED=false
    Should Be Equal As Integers    ${result.rc}    0    orbit ${args} exited ${result.rc}: ${result.stderr}
    RETURN    ${result.stdout}
