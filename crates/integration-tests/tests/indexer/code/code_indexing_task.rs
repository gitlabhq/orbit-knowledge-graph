use arrow::array::StringArray;
use gkg_utils::arrow::ArrowUtils;
use indexer::handler::Handler;
use indexer::modules::code::CodeIndexingTaskHandler;
use indexer::topic::CodeIndexingTaskRequest;
use indexer::types::Envelope;

use super::helpers::*;

#[tokio::test]
async fn indexes_repository() {
    let project_id: i64 = 1;
    let commit_sha = "abc123";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[(
            "src/Main.java",
            "public class Main {
            public void save() { validate(); }
            public void validate() {}
        }",
        )],
    );

    create_project_in_graph(&clickhouse, project_id, "/test", "test/repo").await;

    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler();
    let context = handler_context(&clickhouse);
    let envelope = code_indexing_task_envelope(project_id, commit_sha, 1, "/test");

    let result = handler.handle(context, envelope).await;
    assert!(result.is_ok(), "handler failed: {:?}", result);

    assert_code_indexed(&clickhouse, project_id).await;
}

#[tokio::test]
async fn soft_deletes_stale_code_data_after_reindexing() {
    let project_id: i64 = 2;

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[(
            "src/Main.java",
            "public class Main {
            public void save() { validate(); }
            public void validate() {}
        }",
        )],
    );

    create_project_in_graph(&clickhouse, project_id, "/stale-test", "stale/test").await;
    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler();

    index_code(
        &handler,
        &clickhouse,
        project_id,
        "commit1",
        1,
        "/stale-test",
    )
    .await;

    assert_file_is_active(&clickhouse, project_id, "src/Main.java").await;
    assert_active_definitions(
        &clickhouse,
        project_id,
        "src/Main.java",
        &["Main", "save", "validate"],
    )
    .await;
    assert_eq!(
        count_active_edges(&clickhouse, project_id, "DEFINES").await,
        2,
        "Main→save and Main→validate DEFINES edges should exist"
    );

    mock.replace_archive(
        project_id,
        &[(
            "src/Other.java",
            "public class Other {
            public void run() {}
        }",
        )],
    );
    index_code(
        &handler,
        &clickhouse,
        project_id,
        "commit2",
        2,
        "/stale-test",
    )
    .await;

    assert_file_not_active(&clickhouse, project_id, "src/Main.java").await;
    assert_no_active_definitions(&clickhouse, project_id, "src/Main.java").await;

    assert_file_is_active(&clickhouse, project_id, "src/Other.java").await;
    assert_active_definitions(&clickhouse, project_id, "src/Other.java", &["Other", "run"]).await;

    assert_eq!(
        count_active_edges(&clickhouse, project_id, "DEFINES").await,
        1,
        "only Other→run DEFINES edge should remain active"
    );
}

async fn index_code(
    handler: &CodeIndexingTaskHandler,
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    commit_sha: &str,
    task_id: i64,
    traversal_path: &str,
) {
    let context = handler_context(clickhouse);
    let envelope = code_indexing_task_envelope(project_id, commit_sha, task_id, traversal_path);

    handler
        .handle(context, envelope)
        .await
        .unwrap_or_else(|e| panic!("indexing commit {commit_sha} (task {task_id}) failed: {e}"));
}

fn code_indexing_task_envelope(
    project_id: i64,
    commit_sha: &str,
    task_id: i64,
    traversal_path: &str,
) -> Envelope {
    Envelope::new(&CodeIndexingTaskRequest {
        task_id,
        project_id,
        branch: Some("main".to_string()),
        commit_sha: Some(commit_sha.to_string()),
        traversal_path: traversal_path.to_string(),
    })
    .expect("failed to create envelope")
}

async fn assert_file_not_active(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    path: &str,
) {
    let active_rows = clickhouse
        .query(&format!(
            "SELECT id FROM gl_file FINAL \
             WHERE project_id = {project_id} AND path = '{path}' AND _deleted = false"
        ))
        .await;
    assert!(
        active_rows.first().is_none_or(|b| b.num_rows() == 0),
        "file '{path}' should not be active after soft-deletion"
    );
}

async fn assert_file_is_active(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    path: &str,
) {
    let result = clickhouse
        .query(&format!(
            "SELECT id FROM gl_file FINAL \
             WHERE project_id = {project_id} AND path = '{path}' AND _deleted = false"
        ))
        .await;
    assert!(
        result.first().is_some_and(|b| b.num_rows() > 0),
        "file '{path}' should be active"
    );
}

async fn query_active_definition_names(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    file_path: &str,
) -> Vec<String> {
    let result = clickhouse
        .query(&format!(
            "SELECT name FROM gl_definition FINAL \
             WHERE project_id = {project_id} AND file_path = '{file_path}' AND _deleted = false"
        ))
        .await;
    let Some(batch) = result.first() else {
        return Vec::new();
    };
    let names = ArrowUtils::get_column_by_name::<StringArray>(batch, "name").expect("name column");
    (0..batch.num_rows())
        .map(|i| names.value(i).to_string())
        .collect()
}

async fn assert_active_definitions(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    file_path: &str,
    expected_names: &[&str],
) {
    let mut actual = query_active_definition_names(clickhouse, project_id, file_path).await;
    actual.sort();
    let mut expected: Vec<&str> = expected_names.to_vec();
    expected.sort();
    assert_eq!(
        actual, expected,
        "active definitions in '{file_path}' should be {expected:?}, got {actual:?}"
    );
}

async fn count_active_edges(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    relationship_kind: &str,
) -> usize {
    let result = clickhouse
        .query(&format!(
            "SELECT source_id FROM gl_edge FINAL \
             WHERE relationship_kind = '{relationship_kind}' AND _deleted = false \
             AND source_id IN (SELECT id FROM gl_definition FINAL WHERE project_id = {project_id} AND _deleted = false)"
        ))
        .await;
    result.first().map_or(0, |b| b.num_rows())
}

async fn assert_no_active_definitions(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    file_path: &str,
) {
    let active = query_active_definition_names(clickhouse, project_id, file_path).await;
    assert!(
        active.is_empty(),
        "definitions in '{file_path}' should not be active (soft-deleted), but found: {active:?}"
    );
}
