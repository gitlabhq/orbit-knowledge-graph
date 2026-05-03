*** Settings ***
Documentation       Verify code-indexing backfill: create a namespace with Knowledge Graph
...                 disabled, create two projects under it, push ruby and java fixtures, then
...                 enable the namespace and confirm NamespaceCodeBackfillDispatcher resolves
...                 every pre-existing project via project_namespace_traversal_paths and
...                 dispatches code indexing for each — without any additional pushes. Uses a
...                 simple File node-presence check per project; per-language correctness is
...                 covered by 03_code_indexing.robot.

Resource            gitlab.resource
Resource            orbit.resource
Resource            git.resource


*** Variables ***
${RUBY_FIXTURE_DIR}     /fixtures/ruby/weather-app
${JAVA_FIXTURE_DIR}     /fixtures/java/weather-app

# Expected fixture cardinalities (must match e2e/fixtures/*/weather-app/).
${RUBY_FILE_COUNT}      ${8}
${JAVA_FILE_COUNT}      ${8}


*** Test Cases ***
Namespace With Code Is Prepared Without Knowledge Graph Enabled
    [Documentation]    Create a dedicated group with KG disabled, create ruby- and java-weather
    ...                projects under it, and push their fixtures. Leaving KG disabled ensures
    ...                the upcoming enablement is the only trigger for code indexing — any
    ...                File nodes observed afterward must come from the backfill path.
    [Tags]    code-backfill
    ${suffix}=    Random Suffix
    ${namespace_name}=    Set Variable    e2e-backfill-${suffix}
    ${group}=    Create Group    ${namespace_name}
    Set Suite Variable    ${BACKFILL_NAMESPACE_ID}    ${group["id"]}
    Set Suite Variable    ${BACKFILL_NAMESPACE_NAME}    ${namespace_name}

    ${ruby_name}=    Set Variable    backfill-ruby-${suffix}
    ${ruby}=    Create Project    ${ruby_name}    ${BACKFILL_NAMESPACE_ID}
    Push Fixture To Project    ${ruby}    ${RUBY_FIXTURE_DIR}
    Set Suite Variable    ${BACKFILL_RUBY_PROJECT}    ${ruby}

    ${java_name}=    Set Variable    backfill-java-${suffix}
    ${java}=    Create Project    ${java_name}    ${BACKFILL_NAMESPACE_ID}
    Push Fixture To Project    ${java}    ${JAVA_FIXTURE_DIR}
    Set Suite Variable    ${BACKFILL_JAVA_PROJECT}    ${java}

Enabling Knowledge Graph Backfills Code For Existing Projects
    [Documentation]    Flip Knowledge Graph on for the prepared namespace. The
    ...                knowledge_graph_enabled_namespaces CDC event must reach
    ...                NamespaceCodeBackfillDispatcher, which resolves every project under the
    ...                namespace's traversal path and publishes a CodeIndexingTaskRequest per
    ...                project. Both projects' File nodes should appear with the expected
    ...                fixture cardinalities, proving the backfill path ran end-to-end.
    [Tags]    code-backfill
    Enable Knowledge Graph    ${BACKFILL_NAMESPACE_ID}
    Wait For Node Indexed    Group    ${BACKFILL_NAMESPACE_ID}    ${BACKFILL_NAMESPACE_NAME}
    ...    timeout=60s

    ${ruby_pid}=    Set Variable    ${BACKFILL_RUBY_PROJECT}[id]
    ${java_pid}=    Set Variable    ${BACKFILL_JAVA_PROJECT}[id]

    File Count For Project Is    ${ruby_pid}    ${RUBY_FILE_COUNT}    timeout=60s
    File Count For Project Is    ${java_pid}    ${JAVA_FILE_COUNT}    timeout=60s
