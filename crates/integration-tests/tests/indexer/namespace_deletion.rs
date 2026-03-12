use super::common;

use std::sync::Arc;

use common::{GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext, handler_context};
use indexer::handler::Handler;
use indexer::modules::namespace_deletion::{
    ClickHouseNamespaceDeletionStore, NamespaceDeletionHandler, NamespaceDeletionHandlerConfig,
    NamespaceDeletionStore,
};
use indexer::topic::NamespaceDeletionRequest;
use indexer::types::Envelope;

const DELETED_NAMESPACE_PATH: &str = "1/100/";
const SIBLING_NAMESPACE_PATH: &str = "1/200/";
const DELETED_NAMESPACE_ID: i64 = 100;

#[tokio::test]
async fn deletes_namespace_data_and_marks_schedule_complete() {
    let context = TestContext::new(&[GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL]).await;
    let ontology = ontology::Ontology::load_embedded().expect("ontology must load");

    seed_graph_data(&context).await;
    seed_sdlc_checkpoints(&context).await;
    seed_code_checkpoints(&context).await;
    seed_deletion_schedule(&context).await;
    seed_namespace_as_deleted(&context).await;

    run_deletion_handler(&context, &ontology).await;

    assert_graph_data_deleted(&context).await;
    assert_sibling_graph_data_retained(&context).await;
    assert_deletion_schedule_marked_complete(&context).await;
    assert_sdlc_checkpoints_deleted(&context).await;
    assert_sibling_sdlc_checkpoints_retained(&context).await;
    assert_code_checkpoints_deleted(&context).await;
    assert_sibling_code_checkpoints_retained(&context).await;
}

async fn seed_graph_data(context: &TestContext) {
    context
        .execute(
            "INSERT INTO gl_project (traversal_path, id, _version, _deleted) VALUES \
             ('1/100/', 1, '2024-01-01 00:00:00.000000', false), \
             ('1/200/', 2, '2024-01-01 00:00:00.000000', false)",
        )
        .await;

    context
        .execute(
            "INSERT INTO gl_file (traversal_path, project_id, branch, id, path, name, extension, language, _version, _deleted) VALUES \
             ('1/100/', 10, 'main', 1, 'src/lib.rs', 'lib.rs', 'rs', 'Rust', '2024-01-01 00:00:00.000000', false), \
             ('1/200/', 20, 'main', 2, 'src/lib.rs', 'lib.rs', 'rs', 'Rust', '2024-01-01 00:00:00.000000', false)",
        )
        .await;

    context
        .execute(
            "INSERT INTO gl_edge (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, _version, _deleted) VALUES \
             ('1/100/', 1, 'Project', 'has_member', 10, 'User', '2024-01-01 00:00:00.000000', false), \
             ('1/200/', 2, 'Project', 'has_member', 20, 'User', '2024-01-01 00:00:00.000000', false)",
        )
        .await;
}

async fn seed_sdlc_checkpoints(context: &TestContext) {
    context
        .execute(
            "INSERT INTO checkpoint (key, watermark, cursor_values) VALUES \
             ('ns.100.Group', '2024-01-01 00:00:00.000000', 'null'), \
             ('ns.100.Project', '2024-01-01 00:00:00.000000', 'null'), \
             ('ns.200.Group', '2024-01-01 00:00:00.000000', 'null')",
        )
        .await;
}

async fn seed_code_checkpoints(context: &TestContext) {
    context
        .execute(
            "INSERT INTO code_indexing_checkpoint \
             (traversal_path, project_id, branch, last_event_id, last_commit, indexed_at) VALUES \
             ('1/100/', 10, 'main', 1, 'abc123', '2024-01-01 00:00:00.000000'), \
             ('1/200/', 20, 'main', 1, 'def456', '2024-01-01 00:00:00.000000')",
        )
        .await;
}

async fn seed_deletion_schedule(context: &TestContext) {
    context
        .execute(&format!(
            "INSERT INTO namespace_deletion_schedule \
             (namespace_id, traversal_path, scheduled_deletion_date) VALUES \
             ({DELETED_NAMESPACE_ID}, '{DELETED_NAMESPACE_PATH}', '2024-01-01 00:00:00.000000')"
        ))
        .await;
}

async fn seed_namespace_as_deleted(context: &TestContext) {
    context
        .execute(&format!(
            "INSERT INTO siphon_knowledge_graph_enabled_namespaces \
             (id, root_namespace_id, _siphon_deleted) VALUES \
             ({DELETED_NAMESPACE_ID}, {DELETED_NAMESPACE_ID}, true)"
        ))
        .await;
}

async fn run_deletion_handler(context: &TestContext, ontology: &ontology::Ontology) {
    let graph = Arc::new(context.config.build_client());
    let datalake = Arc::new(context.config.build_client());
    let store: Arc<dyn NamespaceDeletionStore> = Arc::new(ClickHouseNamespaceDeletionStore::new(
        datalake, graph, ontology,
    ));
    let handler = NamespaceDeletionHandler::new(store, NamespaceDeletionHandlerConfig::default());

    let request = NamespaceDeletionRequest {
        namespace_id: DELETED_NAMESPACE_ID,
        traversal_path: DELETED_NAMESPACE_PATH.to_string(),
    };
    let envelope = Envelope::new(&request).unwrap();
    handler
        .handle(handler_context(context), envelope)
        .await
        .unwrap();
}

async fn assert_graph_data_deleted(context: &TestContext) {
    for table in ["gl_project", "gl_file", "gl_edge"] {
        assert_active_row_count(context, table, DELETED_NAMESPACE_PATH, 0).await;
    }
}

async fn assert_sibling_graph_data_retained(context: &TestContext) {
    for table in ["gl_project", "gl_file", "gl_edge"] {
        assert_active_row_count(context, table, SIBLING_NAMESPACE_PATH, 1).await;
    }
}

async fn assert_deletion_schedule_marked_complete(context: &TestContext) {
    let result = context
        .query(&format!(
            "SELECT 1 FROM namespace_deletion_schedule FINAL \
             WHERE namespace_id = {DELETED_NAMESPACE_ID} AND _deleted = false"
        ))
        .await;
    let count = result.first().map_or(0, |batch| batch.num_rows());
    assert_eq!(count, 0, "deletion schedule should be marked as complete");
}

async fn assert_sdlc_checkpoints_deleted(context: &TestContext) {
    let result = context
        .query(
            "SELECT key FROM checkpoint FINAL \
             WHERE startsWith(key, 'ns.100.') AND _deleted = false",
        )
        .await;
    let count = result.first().map_or(0, |b| b.num_rows());
    assert_eq!(
        count, 0,
        "SDLC checkpoints for deleted namespace should be soft-deleted"
    );
}

async fn assert_sibling_sdlc_checkpoints_retained(context: &TestContext) {
    let result = context
        .query(
            "SELECT key FROM checkpoint FINAL \
             WHERE startsWith(key, 'ns.200.') AND _deleted = false",
        )
        .await;
    let count = result.first().map_or(0, |b| b.num_rows());
    assert_eq!(
        count, 1,
        "SDLC checkpoints for sibling namespace should remain"
    );
}

async fn assert_code_checkpoints_deleted(context: &TestContext) {
    let result = context
        .query(
            "SELECT 1 FROM code_indexing_checkpoint FINAL \
             WHERE startsWith(traversal_path, '1/100/') AND _deleted = false",
        )
        .await;
    let count = result.first().map_or(0, |b| b.num_rows());
    assert_eq!(
        count, 0,
        "code checkpoints for deleted namespace should be soft-deleted"
    );
}

async fn assert_sibling_code_checkpoints_retained(context: &TestContext) {
    let result = context
        .query(
            "SELECT 1 FROM code_indexing_checkpoint FINAL \
             WHERE startsWith(traversal_path, '1/200/') AND _deleted = false",
        )
        .await;
    let count = result.first().map_or(0, |b| b.num_rows());
    assert_eq!(
        count, 1,
        "code checkpoints for sibling namespace should remain"
    );
}

async fn assert_active_row_count(
    context: &TestContext,
    table: &str,
    traversal_path: &str,
    expected: usize,
) {
    let result = context
        .query(&format!(
            "SELECT 1 FROM {table} FINAL \
             WHERE startsWith(traversal_path, '{traversal_path}') AND _deleted = false"
        ))
        .await;

    let actual = result.first().map_or(0, |batch| batch.num_rows());
    assert_eq!(
        actual, expected,
        "{table}: expected {expected} active rows for traversal_path '{traversal_path}', got {actual}"
    );
}
