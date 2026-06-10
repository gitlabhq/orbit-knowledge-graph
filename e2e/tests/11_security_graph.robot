*** Settings ***
Documentation       Verify the security subgraph flows end-to-end. Creates one Vulnerability via the
...                 GraphQL vulnerabilityCreate mutation (which also plants an occurrence, scanner,
...                 and identifier) and asserts the node plus its core edges become queryable.
...                 Complements 05, which only covers Vulnerability aggregation under the
...                 security-role authz threshold; here we assert basic indexing and the
...                 occurrence/project/author linkage.

Resource            gitlab.resource
Resource            orbit.resource

Suite Setup         Attach To Shared Fixture


*** Test Cases ***
Vulnerability And Its Core Edges Are Indexed
    [Tags]    security
    ${suffix}=    Random Suffix
    Start Indexing Budget    300
    ${project}=    Create Project    e2e-sec-prj-${suffix}    ${SHARED_NAMESPACE_ID}
    ${title}=    Set Variable    e2e-sec vuln ${suffix}
    ${vuln}=    Create Vulnerability    ${project["id"]}    ${title}    severity=high

    Wait For Node Indexed Within Budget    Vulnerability    ${vuln["id"]}    ${title}    label_field=title
    Wait For Edge Indexed Within Budget    Vulnerability    ${vuln["id"]}
    ...    IN_PROJECT    Project    ${project["id"]}
    Wait For Edge Indexed Within Budget    User    ${None}
    ...    AUTHORED    Vulnerability    ${vuln["id"]}
    # OCCURRENCE_OF proves the finding/occurrence subgraph (occurrence -> vulnerability) materialized.
    Wait For Edge Indexed Within Budget    VulnerabilityOccurrence    ${None}
    ...    OCCURRENCE_OF    Vulnerability    ${vuln["id"]}
