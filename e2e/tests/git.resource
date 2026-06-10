*** Settings ***
Documentation       Git helpers — stage a fixture directory into a fresh repo and push it
...                 to a GitLab project as the initial commit on main, using the e2e-bot PAT
...                 for HTTP basic auth (oauth2:<pat>@host).

Library             Process
Library             OperatingSystem
Library             String
Resource            gitlab.resource


*** Variables ***
${GIT_AUTHOR_EMAIL}     e2e-bot@example.com
${GIT_AUTHOR_NAME}      E2E Bot


*** Keywords ***
Push Fixture To Project
    [Documentation]    Copy ${fixture_dir} into a temp work tree, init+commit, and push to
    ...                ${project} as the initial commit on main. ${project} is the dict
    ...                returned by Create Project (must have path_with_namespace).
    [Arguments]    ${project}    ${fixture_dir}
    Directory Should Exist    ${fixture_dir}
    ${work}=    Evaluate    tempfile.mkdtemp(prefix="e2e-push-")    modules=tempfile
    TRY
        Copy Fixture Tree    ${fixture_dir}    ${work}
        Init Repo And Commit    ${work}
        ${push_url}=    Build Push URL    ${project}
        Wait Until Keyword Succeeds    60s    5s    Push To Remote    ${work}    ${push_url}
    FINALLY
        Remove Directory    ${work}    recursive=True
    END

Copy Fixture Tree
    [Arguments]    ${src}    ${dst}
    ${result}=    Run Process    cp    -R    ${src}/.    ${dst}/    stderr=STDOUT
    Should Be Equal As Integers    ${result.rc}    0    cp failed: ${result.stdout}

Init Repo And Commit
    [Arguments]    ${work}
    Run Git    ${work}    init    -q    -b    main
    Run Git    ${work}    add    -A
    ${env}=    Create Dictionary
    ...    GIT_AUTHOR_EMAIL=${GIT_AUTHOR_EMAIL}    GIT_AUTHOR_NAME=${GIT_AUTHOR_NAME}
    ...    GIT_COMMITTER_EMAIL=${GIT_AUTHOR_EMAIL}    GIT_COMMITTER_NAME=${GIT_AUTHOR_NAME}
    ${result}=    Run Process    git    -C    ${work}    commit    -q    -m    fixture: initial
    ...    env=${env}    stderr=STDOUT
    Should Be Equal As Integers    ${result.rc}    0    git commit failed: ${result.stdout}

Build Push URL
    [Documentation]    Inject oauth2:<pat>@ into ${GITLAB_URL} and append the project path.
    [Arguments]    ${project}
    ${parts}=    Split String    ${GITLAB_URL}    ://    1
    ${scheme}=    Set Variable    ${parts}[0]
    ${host}=    Set Variable    ${parts}[1]
    RETURN    ${scheme}://oauth2:${GITLAB_PAT}@${host}/${project}[path_with_namespace].git

Push To Remote
    [Arguments]    ${work}    ${push_url}
    ${result}=    Run Process    git    -C    ${work}    push    ${push_url}    HEAD:refs/heads/main
    ...    stderr=STDOUT
    Should Be Equal As Integers    ${result.rc}    0    git push failed: ${result.stdout}

Run Git
    [Arguments]    ${work}    @{args}
    ${result}=    Run Process    git    -C    ${work}    @{args}    stderr=STDOUT
    Should Be Equal As Integers    ${result.rc}    0    git ${args} failed: ${result.stdout}
