use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Int64Array, StringArray};
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
async fn indexes_file_nodes_for_all_archive_files() {
    let project_id: i64 = 8;
    let commit_sha = "allfiles";
    let traversal_path = "/all-files";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[
            ("src/main.py", "def hello():\n    return 1\n"),
            ("README.md", "# Project\n"),
            ("config/app.yml", "enabled: true\n"),
            ("Dockerfile", "FROM scratch\n"),
            (".gitignore", "target/\n"),
            ("assets/logo.png", "fake png bytes"),
            ("docs/only/README.md", "# Nested\n"),
        ],
    );

    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler();
    index_code(
        &handler,
        &clickhouse,
        project_id,
        commit_sha,
        1,
        traversal_path,
    )
    .await;

    let files = active_file_rows(&clickhouse, project_id).await;
    let paths: Vec<_> = files.iter().map(|row| row.0.as_str()).collect();
    for expected in [
        ".gitignore",
        "Dockerfile",
        "README.md",
        "assets/logo.png",
        "config/app.yml",
        "docs/only/README.md",
        "src/main.py",
    ] {
        assert!(
            paths.contains(&expected),
            "expected File node for {expected}, got {paths:?}"
        );
    }
    assert_eq!(
        language_for(&files, "README.md"),
        Some("unknown"),
        "non-parsable markdown should use the stable unknown language"
    );
    assert_eq!(language_for(&files, "assets/logo.png"), Some("unknown"));
    assert_eq!(language_for(&files, "src/main.py"), Some("python"));

    assert_directory_is_active(&clickhouse, project_id, "config").await;
    assert_directory_is_active(&clickhouse, project_id, "docs/only").await;
    assert_contains_edge_between_directory_and_file(
        &clickhouse,
        project_id,
        "config",
        "config/app.yml",
    )
    .await;
    assert_contains_edge_between_directory_and_file(
        &clickhouse,
        project_id,
        "docs/only",
        "docs/only/README.md",
    )
    .await;

    assert_no_active_definitions(&clickhouse, project_id, "README.md").await;
    assert_no_active_definitions(&clickhouse, project_id, "config/app.yml").await;
    assert_no_active_definitions(&clickhouse, project_id, "assets/logo.png").await;
    assert_active_definitions(&clickhouse, project_id, "src/main.py", &["hello"]).await;
}

/// End-to-end test for CALLS and EXTENDS edges:
/// indexes Java code with class inheritance and a method call, then
/// queries `gl_code_edge` to verify both relationship kinds were written.
#[tokio::test]
async fn indexes_calls_and_extends_edges() {
    let project_id: i64 = 99;
    let commit_sha = "callsdef";
    let traversal_path = "1/99";

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
            "src/Zoo.java",
            "public class Animal {
                public void speak() {}
            }
            public class Dog extends Animal {
                public void fetch() { speak(); }
            }",
        )],
    );

    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler();
    index_code(
        &handler,
        &clickhouse,
        project_id,
        commit_sha,
        1,
        traversal_path,
    )
    .await;

    // Both edges land in gl_code_edge per ontology routing
    // (the schema version prefix is added at runtime).
    let ontology = integration_testkit::load_ontology();
    let calls_table = ontology.edge_table_for_relationship("CALLS");
    let extends_table = ontology.edge_table_for_relationship("EXTENDS");
    assert!(
        calls_table.ends_with("gl_code_edge"),
        "CALLS must route to gl_code_edge, got {calls_table}"
    );
    assert!(
        extends_table.ends_with("gl_code_edge"),
        "EXTENDS must route to gl_code_edge, got {extends_table}"
    );

    let calls = count_active_edges(&clickhouse, project_id, "CALLS").await;
    assert!(
        calls >= 1,
        "expected at least one CALLS edge for fetch()->speak(), got {calls}"
    );

    let extends = count_active_edges(&clickhouse, project_id, "EXTENDS").await;
    assert!(
        extends >= 1,
        "expected at least one EXTENDS edge for Dog extends Animal, got {extends}"
    );

    // Now run a real query through the compiler against the indexed data:
    // CALLS traversal Definition -> Definition. The compiler resolves the
    // CALLS edge through the embedded ontology (proves schema.yaml
    // registration works) and the SQL must hit gl_code_edge.
    let json = format!(
        r#"{{
        "query_type": "traversal",
        "nodes": [
            {{"id": "caller", "entity": "Definition", "filters": {{"project_id": {project_id}}}, "columns": ["name", "fqn"]}},
            {{"id": "callee", "entity": "Definition", "columns": ["name", "fqn"]}}
        ],
        "relationships": [{{"type": "CALLS", "from": "caller", "to": "callee"}}],
        "limit": 25
    }}"#
    );
    let json = json.as_str();
    let security_ctx = compiler::SecurityContext::new(1, vec!["1/".into()])
        .expect("security context")
        .with_role(true, None);
    let compiled = compiler::compile(json, &ontology, &security_ctx).expect("CALLS query compiles");
    let sql = compiled.base.render();
    assert!(
        sql.contains("gl_code_edge"),
        "compiled CALLS query must scan gl_code_edge (table: {calls_table}): {sql}"
    );
    let rows = clickhouse.query(&sql).await;
    let total: usize = rows.iter().map(|b| b.num_rows()).sum();
    assert!(
        total >= 1,
        "compiled CALLS query should return at least one row, got {total}"
    );
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

#[tokio::test]
async fn empty_200_archive_checkpoints_as_empty_repository() {
    let project_id: i64 = 7;
    let traversal_path = "/empty-archive-test";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project_with_empty_archive(project_id, "main");

    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler();
    let context = handler_context(&clickhouse);
    let envelope = code_indexing_task_envelope(project_id, "abc123", 11, traversal_path);

    let result = handler.handle(context, envelope).await;
    assert!(
        result.is_ok(),
        "empty 200 archive should ack on first attempt, got {:?}",
        result
    );

    // Checkpoint is set with no commit, marking the project as indexed-empty.
    let checkpoint_rows = clickhouse
        .query(&format!(
            "SELECT last_task_id, last_commit FROM {} FINAL \
             WHERE traversal_path = '{traversal_path}' AND project_id = {project_id} \
             AND branch = 'main' AND _deleted = false",
            t("code_indexing_checkpoint")
        ))
        .await;
    let batch = checkpoint_rows
        .first()
        .expect("checkpoint row must exist after empty-archive ack");
    assert_eq!(batch.num_rows(), 1, "expected exactly one checkpoint row");
    let task_ids = ArrowUtils::get_column_by_name::<Int64Array>(batch, "last_task_id")
        .expect("last_task_id column");
    assert_eq!(task_ids.value(0), 11);
    let last_commits = ArrowUtils::get_column_by_name::<StringArray>(batch, "last_commit")
        .expect("last_commit column");
    assert!(
        last_commits.is_null(0) || last_commits.value(0).is_empty(),
        "empty-archive checkpoint should record no commit, got {:?}",
        last_commits.value(0)
    );

    // No graph rows should have been written for this project.
    let files = clickhouse
        .query(&format!(
            "SELECT path FROM {} WHERE project_id = {project_id}",
            t("gl_file")
        ))
        .await;
    assert!(
        files.first().is_none_or(|b| b.num_rows() == 0),
        "no files should be written for an empty-archive project"
    );
    let definitions = clickhouse
        .query(&format!(
            "SELECT name FROM {} WHERE project_id = {project_id}",
            t("gl_definition")
        ))
        .await;
    assert!(
        definitions.first().is_none_or(|b| b.num_rows() == 0),
        "no definitions should be written for an empty-archive project"
    );
    let ontology = integration_testkit::load_ontology();
    let defines_edges = clickhouse
        .query(&format!(
            "SELECT source_id FROM {} \
             WHERE relationship_kind = 'DEFINES' \
             AND source_id IN (SELECT id FROM {} WHERE project_id = {project_id})",
            ontology.edge_table_for_relationship("DEFINES"),
            t("gl_definition")
        ))
        .await;
    assert!(
        defines_edges.first().is_none_or(|b| b.num_rows() == 0),
        "no DEFINES edges should be written for an empty-archive project"
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

async fn active_file_rows(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
) -> Vec<(String, String, String)> {
    let result = clickhouse
        .query(&format!(
            "SELECT path, extension, language FROM {} FINAL \
             WHERE project_id = {project_id} AND _deleted = false \
             ORDER BY path",
            t("gl_file")
        ))
        .await;

    let mut rows = Vec::new();
    for batch in result {
        let paths =
            ArrowUtils::get_column_by_name::<StringArray>(&batch, "path").expect("path column");
        let extensions = ArrowUtils::get_column_by_name::<StringArray>(&batch, "extension")
            .expect("extension column");
        let languages = ArrowUtils::get_column_by_name::<StringArray>(&batch, "language")
            .expect("language column");
        for row in 0..batch.num_rows() {
            rows.push((
                paths.value(row).to_string(),
                extensions.value(row).to_string(),
                languages.value(row).to_string(),
            ));
        }
    }
    rows
}

fn language_for<'a>(rows: &'a [(String, String, String)], path: &str) -> Option<&'a str> {
    rows.iter()
        .find(|row| row.0 == path)
        .map(|row| row.2.as_str())
}

async fn assert_directory_is_active(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    path: &str,
) {
    let result = clickhouse
        .query(&format!(
            "SELECT id FROM {} FINAL \
             WHERE project_id = {project_id} AND path = '{path}' AND _deleted = false",
            t("gl_directory")
        ))
        .await;
    assert!(
        result.first().is_some_and(|b| b.num_rows() > 0),
        "directory '{path}' should be active"
    );
}

async fn assert_contains_edge_between_directory_and_file(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    directory_path: &str,
    file_path: &str,
) {
    let ontology = integration_testkit::load_ontology();
    let edge_table = ontology.edge_table_for_relationship("CONTAINS");
    let result = clickhouse
        .query(&format!(
            "SELECT e.source_id FROM {edge_table} AS e FINAL \
             INNER JOIN {} AS d FINAL ON e.source_id = d.id \
             INNER JOIN {} AS f FINAL ON e.target_id = f.id \
             WHERE e.relationship_kind = 'CONTAINS' \
               AND e.source_kind = 'Directory' \
               AND e.target_kind = 'File' \
               AND d.project_id = {project_id} \
               AND f.project_id = {project_id} \
               AND d.path = '{directory_path}' \
               AND f.path = '{file_path}' \
               AND d._deleted = false \
               AND f._deleted = false \
               AND e._deleted = false",
            t("gl_directory"),
            t("gl_file")
        ))
        .await;
    assert!(
        result.first().is_some_and(|b| b.num_rows() > 0),
        "expected CONTAINS edge from directory '{directory_path}' to file '{file_path}'"
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
