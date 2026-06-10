*** Settings ***
Documentation       Verify the namespace enable/disable lifecycle as observed through the API:
...                 disabling Knowledge Graph stops dispatching but retains already-indexed data
...                 (the 30-day deletion grace), and re-enabling resumes indexing for new entities.
...                 Uses a dedicated group so it never disturbs the shared namespace.
...
...                 Out of scope here: the post-grace deletion purge, which needs a scheduler-cron
...                 override and a direct namespace_deletion_schedule seed. Tracked in #792.

Resource            gitlab.resource
Resource            orbit.resource


*** Test Cases ***
Disabling Knowledge Graph Retains Indexed Data
    [Documentation]    Enable a fresh group, index an issue, disable, then assert the issue is still
    ...                queryable. Disable removes the enabled-namespace record but must not purge data.
    [Tags]    lifecycle
    ${suffix}=    Random Suffix
    ${group}=    Create Group    e2e-lifecycle-${suffix}
    Set Suite Variable    ${LIFECYCLE_GROUP_ID}    ${group["id"]}
    Enable Knowledge Graph    ${LIFECYCLE_GROUP_ID}

    Start Indexing Budget    300
    ${project}=    Create Project    e2e-lifecycle-prj-${suffix}    ${LIFECYCLE_GROUP_ID}
    ${issue}=    Create Issue    ${project["id"]}    e2e-lifecycle-issue-${suffix}
    Wait For Node Indexed Within Budget    WorkItem    ${issue["id"]}    e2e-lifecycle-issue-${suffix}
    ...    label_field=title

    Disable Knowledge Graph    ${LIFECYCLE_GROUP_ID}
    Verify Node Indexed    WorkItem    ${issue["id"]}

Re-enabling Knowledge Graph Resumes Indexing
    [Documentation]    Re-enable the same group and create a new issue; it must be indexed, proving
    ...                the namespace dispatcher picks the namespace back up.
    [Tags]    lifecycle
    ${suffix}=    Random Suffix
    Enable Knowledge Graph    ${LIFECYCLE_GROUP_ID}

    Start Indexing Budget    300
    ${project}=    Create Project    e2e-reenable-prj-${suffix}    ${LIFECYCLE_GROUP_ID}
    ${issue}=    Create Issue    ${project["id"]}    e2e-reenable-issue-${suffix}
    Wait For Node Indexed Within Budget    WorkItem    ${issue["id"]}    e2e-reenable-issue-${suffix}
    ...    label_field=title
