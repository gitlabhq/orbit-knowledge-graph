*** Settings ***
Documentation       ClickHouse admin helpers used by tests to flush stale dictionary
...                 cache after creating new namespaces. The traversal-path dictionaries
...                 use a CACHE layout with LIFETIME(MIN 60 MAX 300), so a key looked up
...                 *before* its source row lands in `namespace_traversal_paths` gets
...                 negatively cached as '0/' for up to 5 minutes. Without an explicit
...                 reload, work_items / notes inserted shortly after a fresh namespace
...                 land with traversal_path='0/' and never become visible to GKG's
...                 namespace-scoped queries within the test budget.
...
...                 Tracked in
...                 https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/483

Library             RequestsLibrary
Library             Collections


*** Variables ***
${CLICKHOUSE_URL}           %{CLICKHOUSE_URL}
${CLICKHOUSE_USER}          %{CLICKHOUSE_USER}
${CLICKHOUSE_PASSWORD}      %{CLICKHOUSE_PASSWORD}


*** Keywords ***
ClickHouse Exec
    [Documentation]    Run a single SQL statement against ClickHouse via the HTTP interface.
    ...                Returns the response body as a string.
    [Arguments]    ${sql}
    ${headers}=    Create Dictionary
    ...    X-ClickHouse-User=${CLICKHOUSE_USER}
    ...    X-ClickHouse-Key=${CLICKHOUSE_PASSWORD}
    ${resp}=    POST    ${CLICKHOUSE_URL}    data=${sql}    headers=${headers}
    ...    expected_status=200
    RETURN    ${resp.text}

Reload Traversal Path Dictionaries
    [Documentation]    Force a synchronous reload of every traversal-path dictionary so that
    ...                cells negatively cached during a race between siphon_routes and
    ...                siphon_namespaces are evicted. Call this immediately after any API
    ...                action that creates a new namespace (group, project, user) so that
    ...                downstream INSERTs evaluate DEFAULT expressions against fresh dict
    ...                state.
    ClickHouse Exec    SYSTEM RELOAD DICTIONARY datalake.namespace_traversal_paths_dict
    ClickHouse Exec    SYSTEM RELOAD DICTIONARY datalake.project_traversal_paths_dict
    ClickHouse Exec    SYSTEM RELOAD DICTIONARY datalake.organization_traversal_paths_dict
