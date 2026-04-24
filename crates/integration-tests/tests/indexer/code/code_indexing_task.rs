use std::sync::Arc;

use arrow::array::{BooleanArray, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use gkg_utils::arrow::ArrowUtils;
use indexer::destination::{BatchWriter, Destination, DestinationError};
use indexer::handler::{Handler, HandlerContext};
use indexer::indexing_status::IndexingStatusStore;
use indexer::nats::ProgressNotifier;
use indexer::testkit::{MockLockService, MockNatsServices};
use indexer::topic::CodeIndexingTaskRequest;
use indexer::types::Envelope;
use integration_testkit::{assert_edge_count_for_traversal_path, t};

use super::helpers::*;

#[tokio::test]
async fn indexes_repository() {
    let project_id: i64 = 1;
    let commit_sha = "abc123";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
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
        *integration_testkit::GRAPH_SCHEMA_SQL,
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
async fn disk_is_clean_after_successful_indexing() {
    let project_id: i64 = 4;
    let commit_sha = "abc123";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[(
            "src/Main.java",
            "public class Main {
            public void save() {}
        }",
        )],
    );

    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let cache_dir = deps.cache_dir_path().to_path_buf();
    let handler = deps.code_indexing_task_handler();

    index_code(
        &handler,
        &clickhouse,
        project_id,
        commit_sha,
        1,
        "/cleanup-test",
    )
    .await;

    assert_code_indexed(&clickhouse, project_id).await;

    let remaining: Vec<_> = std::fs::read_dir(&cache_dir)
        .into_iter()
        .flatten()
        .flatten()
        .collect();
    assert!(
        remaining.is_empty(),
        "cache dir should be empty after indexing, found: {remaining:?}"
    );
}

#[tokio::test]
async fn disk_is_clean_after_multiple_reindexes() {
    let project_id: i64 = 5;

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[("src/Main.java", "public class Main { public void v1() {} }")],
    );

    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let cache_dir = deps.cache_dir_path().to_path_buf();
    let handler = deps.code_indexing_task_handler();

    index_code(
        &handler,
        &clickhouse,
        project_id,
        "commit1",
        1,
        "/multi-test",
    )
    .await;

    mock.replace_archive(
        project_id,
        &[("src/Main.java", "public class Main { public void v2() {} }")],
    );
    index_code(
        &handler,
        &clickhouse,
        project_id,
        "commit2",
        2,
        "/multi-test",
    )
    .await;

    mock.replace_archive(
        project_id,
        &[("src/Main.java", "public class Main { public void v3() {} }")],
    );
    index_code(
        &handler,
        &clickhouse,
        project_id,
        "commit3",
        3,
        "/multi-test",
    )
    .await;

    assert_active_definitions(&clickhouse, project_id, "src/Main.java", &["Main", "v3"]).await;

    let remaining: Vec<_> = std::fs::read_dir(&cache_dir)
        .into_iter()
        .flatten()
        .flatten()
        .collect();
    assert!(
        remaining.is_empty(),
        "cache dir should be empty after repeated indexing, found: {remaining:?}"
    );
}

#[tokio::test]
async fn does_not_checkpoint_or_stale_delete_when_writer_fails() {
    let project_id: i64 = 6;
    let traversal_path = "/write-failure-test";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[(
            "src/Main.java",
            "public class Main { public void keep() {} }",
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
        traversal_path,
    )
    .await;

    assert_file_is_active(&clickhouse, project_id, "src/Main.java").await;
    assert_eq!(
        latest_checkpoint_task_id(&clickhouse, traversal_path, project_id, "main").await,
        Some(1)
    );

    mock.replace_archive(
        project_id,
        &[(
            "src/Other.java",
            "public class Other { public void run() {} }",
        )],
    );
    let (context, indexing_status) = handler_context_with_destination(Arc::new(FailingDestination));
    let envelope = code_indexing_task_envelope(project_id, "commit2", 2, traversal_path);
    let error = handler
        .handle(context, envelope)
        .await
        .expect_err("writer failure should fail the task");

    assert!(
        error
            .to_string()
            .contains("fatal code indexing pipeline error"),
        "unexpected error: {error}"
    );
    assert_file_is_active(&clickhouse, project_id, "src/Main.java").await;
    assert_file_not_active(&clickhouse, project_id, "src/Other.java").await;
    assert_eq!(
        latest_checkpoint_task_id(&clickhouse, traversal_path, project_id, "main").await,
        Some(1),
        "failed reindex must not advance the checkpoint"
    );

    let progress = indexing_status
        .get(traversal_path)
        .await
        .expect("progress lookup should succeed")
        .expect("failed run should record indexing progress");
    assert!(
        progress
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("fatal code indexing pipeline error")),
        "failed run should record the fatal pipeline error in indexing progress, got {progress:?}"
    );
}

async fn index_code(
    handler: &indexer::modules::code::CodeIndexingTaskHandler,
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

struct FailingDestination;

#[async_trait]
impl Destination for FailingDestination {
    async fn new_batch_writer(
        &self,
        _table: &str,
    ) -> Result<Box<dyn BatchWriter>, DestinationError> {
        Ok(Box::new(FailingBatchWriter))
    }
}

struct FailingBatchWriter;

#[async_trait]
impl BatchWriter for FailingBatchWriter {
    async fn write_batch(&self, _batch: &[RecordBatch]) -> Result<(), DestinationError> {
        Err(DestinationError::Write(
            "forced write failure".to_string(),
            None,
        ))
    }
}

fn handler_context_with_destination(
    destination: Arc<dyn Destination>,
) -> (HandlerContext, Arc<IndexingStatusStore>) {
    let mock_nats = Arc::new(MockNatsServices::new());
    let indexing_status = Arc::new(IndexingStatusStore::new(mock_nats.clone()));
    let context = HandlerContext::new(
        destination,
        mock_nats.clone(),
        Arc::new(MockLockService::new()),
        ProgressNotifier::noop(),
        indexing_status.clone(),
    );
    (context, indexing_status)
}

async fn latest_checkpoint_task_id(
    clickhouse: &integration_testkit::TestContext,
    traversal_path: &str,
    project_id: i64,
    branch: &str,
) -> Option<i64> {
    let result = clickhouse
        .query(&format!(
            "SELECT last_task_id FROM {} FINAL \
             WHERE traversal_path = '{traversal_path}' AND project_id = {project_id} \
             AND branch = '{branch}' AND _deleted = false",
            t("code_indexing_checkpoint")
        ))
        .await;
    let batch = result.first()?;
    if batch.num_rows() == 0 {
        return None;
    }
    let task_ids = ArrowUtils::get_column_by_name::<Int64Array>(batch, "last_task_id")
        .expect("task id column");
    Some(task_ids.value(0))
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
            "SELECT id FROM {} FINAL \
             WHERE project_id = {project_id} AND path = '{path}' AND _deleted = false",
            t("gl_file")
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
            "SELECT id FROM {} FINAL \
             WHERE project_id = {project_id} AND path = '{path}' AND _deleted = false",
            t("gl_file")
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
            "SELECT name FROM {} FINAL \
             WHERE project_id = {project_id} AND file_path = '{file_path}' AND _deleted = false",
            t("gl_definition")
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
    let ontology = integration_testkit::load_ontology();
    let edge_table = ontology.edge_table_for_relationship(relationship_kind);
    let result = clickhouse
        .query(&format!(
            "SELECT source_id FROM {edge_table} FINAL \
             WHERE relationship_kind = '{relationship_kind}' AND _deleted = false \
             AND source_id IN (SELECT id FROM {} FINAL WHERE project_id = {project_id} AND _deleted = false)",
            t("gl_definition")
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

async fn assert_branch_indexed(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    expected_name: &str,
    expected_traversal_path: &str,
) {
    let result = clickhouse
        .query(&format!(
            "SELECT name, is_default, traversal_path, project_id \
             FROM {} FINAL \
             WHERE project_id = {project_id} AND _deleted = false",
            t("gl_branch")
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
