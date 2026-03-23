use arrow::array::{BooleanArray, Int64Array, StringArray};
use gkg_utils::arrow::ArrowUtils;
use indexer::handler::Handler;
use indexer::modules::code::CodeIndexingTaskHandler;
use indexer::topic::CodeIndexingTaskRequest;
use indexer::types::Envelope;
use integration_testkit::assert_edge_count_for_traversal_path;

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

    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler();
    let context = handler_context(&clickhouse);
    let envelope = code_indexing_task_envelope(project_id, commit_sha, 1, "/test");

    let result = handler.handle(context, envelope).await;
    assert!(result.is_ok(), "handler failed: {:?}", result);

    assert_code_indexed(&clickhouse, project_id).await;
    assert_branch_indexed(&clickhouse, project_id, "main", "/test").await;

    // Nested files should not have direct Branch --CONTAINS--> File edges
    assert_edge_count_for_traversal_path(&clickhouse, "CONTAINS", "Branch", "File", "/test", 0)
        .await;
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

#[tokio::test]
async fn incremental_update_applies_changed_paths_and_blobs() {
    let project_id: i64 = 3;

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

    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler();

    index_code(&handler, &clickhouse, project_id, "commit1", 1, "/inc-test").await;

    assert_file_is_active(&clickhouse, project_id, "src/Main.java").await;
    assert_active_definitions(
        &clickhouse,
        project_id,
        "src/Main.java",
        &["Main", "save", "validate"],
    )
    .await;

    let modified_main = "public class Main { public void run() {} }";
    let new_other = "public class Other { public void execute() {} }";

    mock.set_changed_paths(
        project_id,
        &[
            r#"{"path":"src/Main.java","status":"MODIFIED","old_path":"","new_mode":33188,"old_mode":33188,"old_blob_id":"old_main","new_blob_id":"blob_main_v2"}"#,
            r#"{"path":"src/Other.java","status":"ADDED","old_path":"","new_mode":33188,"old_mode":0,"old_blob_id":"","new_blob_id":"blob_other"}"#,
        ]
        .join("\n"),
    );
    mock.add_blob(project_id, "blob_main_v2", modified_main.as_bytes());
    mock.add_blob(project_id, "blob_other", new_other.as_bytes());

    index_code(&handler, &clickhouse, project_id, "commit2", 2, "/inc-test").await;

    assert_file_is_active(&clickhouse, project_id, "src/Main.java").await;
    assert_active_definitions(&clickhouse, project_id, "src/Main.java", &["Main", "run"]).await;
    assert_no_active_definitions_named(
        &clickhouse,
        project_id,
        "src/Main.java",
        &["save", "validate"],
    )
    .await;

    assert_file_is_active(&clickhouse, project_id, "src/Other.java").await;
    assert_active_definitions(
        &clickhouse,
        project_id,
        "src/Other.java",
        &["Other", "execute"],
    )
    .await;
}

#[tokio::test]
async fn small_repo_evicted_from_cache_after_indexing() {
    let project_id: i64 = 4;

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
            "public class Main { public void run() {} }",
        )],
    );

    let config = indexer::configuration::RepositoryCacheConfiguration::default();
    let deps = CodeIndexingDeps::with_cache_config(&mock, &clickhouse, config);
    let handler = deps.code_indexing_task_handler();

    index_code(
        &handler,
        &clickhouse,
        project_id,
        "commit1",
        1,
        "/evict-test",
    )
    .await;

    assert_code_indexed(&clickhouse, project_id).await;

    let project_dir = deps.cache_dir.path().join(project_id.to_string());
    let branch_entries: Vec<_> = std::fs::read_dir(&project_dir)
        .expect("project dir should be readable")
        .collect();
    assert!(
        branch_entries.is_empty(),
        "small repo cache should be evicted after indexing, found: {branch_entries:?}"
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

async fn assert_no_active_definitions_named(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    file_path: &str,
    names: &[&str],
) {
    let active = query_active_definition_names(clickhouse, project_id, file_path).await;
    for name in names {
        assert!(
            !active.contains(&name.to_string()),
            "definition '{name}' in '{file_path}' should not be active, but found in {active:?}"
        );
    }
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

async fn assert_branch_indexed(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    expected_name: &str,
    expected_traversal_path: &str,
) {
    let result = clickhouse
        .query(&format!(
            "SELECT name, is_default, traversal_path, project_id \
             FROM gl_branch FINAL \
             WHERE project_id = {project_id} AND _deleted = false"
        ))
        .await;

    let batch = result
        .first()
        .expect("gl_branch should have rows after indexing");
    assert_eq!(batch.num_rows(), 1, "expected exactly one branch row");

    let names = ArrowUtils::get_column_by_name::<StringArray>(batch, "name").expect("name column");
    assert_eq!(names.value(0), expected_name);

    let is_default = ArrowUtils::get_column_by_name::<BooleanArray>(batch, "is_default")
        .expect("is_default column");
    assert!(is_default.value(0), "branch should be marked as default");

    let traversal_paths = ArrowUtils::get_column_by_name::<StringArray>(batch, "traversal_path")
        .expect("traversal_path column");
    assert_eq!(traversal_paths.value(0), expected_traversal_path);

    let project_ids = ArrowUtils::get_column_by_name::<Int64Array>(batch, "project_id")
        .expect("project_id column");
    assert_eq!(project_ids.value(0), project_id);

    assert_edge_count_for_traversal_path(
        clickhouse,
        "IN_PROJECT",
        "Branch",
        "Project",
        expected_traversal_path,
        1,
    )
    .await;

    assert_edge_count_for_traversal_path(
        clickhouse,
        "CONTAINS",
        "Branch",
        "Directory",
        expected_traversal_path,
        1,
    )
    .await;
}
