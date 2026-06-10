*** Settings ***
Documentation       Verify the core membership/ownership edges that no other suite covers: MEMBER_OF
...                 (User->Group, from siphon_members) and CREATOR (User->Project, from
...                 project.creator_id). Both anchor on ids the suite controls so the queries stay
...                 constrained.

Resource            gitlab.resource
Resource            orbit.resource


*** Test Cases ***
Group Membership Produces A MEMBER_OF Edge
    [Tags]    membership
    ${suffix}=    Random Suffix
    ${user}=    Create User    e2e-member-${suffix}
    Add Group Member    ${SHARED_NAMESPACE_ID}    ${user["id"]}    20
    Start Indexing Budget    180
    Wait For Edge Indexed Within Budget    User    ${user["id"]}
    ...    MEMBER_OF    Group    ${SHARED_NAMESPACE_ID}

Project Creation Produces A CREATOR Edge
    [Documentation]    The e2e-bot creates the project, so its user id is the project's creator_id.
    [Tags]    membership
    ${suffix}=    Random Suffix
    Start Indexing Budget    180
    ${project}=    Create Project    e2e-creator-prj-${suffix}    ${SHARED_NAMESPACE_ID}
    Wait For Node Indexed Within Budget    Project    ${project["id"]}    e2e-creator-prj-${suffix}
    Wait For Edge Indexed Within Budget    User    ${E2E_BOT_USER_ID}
    ...    CREATOR    Project    ${project["id"]}
