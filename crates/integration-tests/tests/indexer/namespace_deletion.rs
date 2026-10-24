use integration_testkit::t;

use super::common;

use std::sync::Arc;

use clickhouse_client::ClickHouseConfigurationExt;
use common::{
    GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext, create_namespace_with_path, create_project,
    handler_context, namespace_envelope, namespace_handler,
};
use indexer::checkpoint::ClickHouseCheckpointStore;
use indexer::handler::Handler;
use indexer::modules::namespace_deletion::{
    ClickHouseNamespaceDeletionStore, DeletionMetrics, NamespaceDeletionHandler,
    NamespaceDeletionStore,
};
use indexer::orchestrator::scheduled::{
    NamespaceDeletionScheduler, ScheduledTask, ScheduledTaskMetrics,
};
use indexer::testkit::MockNatsServices;
use indexer::topic::NamespaceDeletionRequest;
use indexer::types::{Envelope, Event};
use orbit_utils::traversal_path::TraversalPath;

const DELETED_NAMESPACE_PATH: &str = "1/100/";
const SIBLING_NAMESPACE_PATH: &str = "1/200/";
const DELETED_NAMESPACE_ID: i64 = 100;

#[tokio::test]
async fn deletes_namespace_data_and_marks_schedule_complete() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL]).await;
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
        .execute(&format!(
            "INSERT INTO {} (traversal_path, id, _version, _deleted) VALUES \
             ('1/100/', 1, '2024-01-01 00:00:00.000000', false), \
             ('1/200/', 2, '2024-01-01 00:00:00.000000', false)",
            t("gl_project")
        ))
        .await;

    context
        .execute(&format!(
            "INSERT INTO {} (traversal_path, project_id, branch, id, path, name, extension, language, _version, _deleted) VALUES \
             ('1/100/', 10, 'main', 1, 'src/lib.rs', 'lib.rs', 'rs', 'Rust', '2024-01-01 00:00:00.000000', false), \
             ('1/200/', 20, 'main', 2, 'src/lib.rs', 'lib.rs', 'rs', 'Rust', '2024-01-01 00:00:00.000000', false)",
            t("gl_file")
        ))
        .await;

    context
        .execute(&format!(
            "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, _version, _deleted) VALUES \
             ('1/100/', 1, 'Project', 'has_member', 10, 'User', '2024-01-01 00:00:00.000000', false), \
             ('1/200/', 2, 'Project', 'has_member', 20, 'User', '2024-01-01 00:00:00.000000', false)",
            t("gl_edge")
        ))
        .await;
}

async fn seed_sdlc_checkpoints(context: &TestContext) {
    context
        .execute(&format!(
            "INSERT INTO {} (key, watermark, cursor_values) VALUES \
             ('ns.100.Group', '2024-01-01 00:00:00.000000', 'null'), \
             ('ns.100.Project', '2024-01-01 00:00:00.000000', 'null'), \
             ('ns.200.Group', '2024-01-01 00:00:00.000000', 'null')",
            t("checkpoint")
        ))
        .await;
}

async fn seed_code_checkpoints(context: &TestContext) {
    context
        .execute(&format!(
            "INSERT INTO {} \
             (traversal_path, project_id, branch, last_task_id, last_commit, indexed_at) VALUES \
             ('1/100/', 10, 'main', 1, 'abc123', '2024-01-01 00:00:00.000000'), \
             ('1/200/', 20, 'main', 1, 'def456', '2024-01-01 00:00:00.000000')",
            t("code_indexing_checkpoint")
        ))
        .await;
}

async fn seed_deletion_schedule(context: &TestContext) {
    context
        .execute(&format!(
            "INSERT INTO {} \
             (namespace_id, traversal_path, scheduled_deletion_date) VALUES \
             ({DELETED_NAMESPACE_ID}, '{DELETED_NAMESPACE_PATH}', '2024-01-01 00:00:00.000000')",
            t("namespace_deletion_schedule")
        ))
        .await;
}

async fn seed_namespace_as_deleted(context: &TestContext) {
    context
        .execute(&format!(
            "INSERT INTO siphon_knowledge_graph_enabled_namespaces \
             (id, root_namespace_id, traversal_path, _siphon_deleted) VALUES \
             ({DELETED_NAMESPACE_ID}, {DELETED_NAMESPACE_ID}, '{DELETED_NAMESPACE_PATH}', true)"
        ))
        .await;
}

async fn run_deletion_handler(context: &TestContext, ontology: &ontology::Ontology) {
    let graph = Arc::new(context.config.build_client("graph"));
    let datalake = Arc::new(context.config.build_client("graph"));
    let store: Arc<dyn NamespaceDeletionStore> = Arc::new(ClickHouseNamespaceDeletionStore::new(
        datalake, graph, ontology,
    ));
    let handler = NamespaceDeletionHandler::new(
        store,
        DeletionMetrics::new(),
        NamespaceDeletionRequest::subscription(),
    );

    let request = NamespaceDeletionRequest {
        namespace_id: DELETED_NAMESPACE_ID,
        traversal_path: TraversalPath::new_unchecked(DELETED_NAMESPACE_PATH),
        dispatch_id: uuid::Uuid::new_v4(),
    };
    let envelope = Envelope::new(&request).unwrap();
    handler.handle(handler_context(), envelope).await.unwrap();
}

async fn assert_graph_data_deleted(context: &TestContext) {
    for table in [t("gl_project"), t("gl_file"), t("gl_edge")] {
        assert_active_row_count(context, &table, DELETED_NAMESPACE_PATH, 0).await;
    }
    let result = context
        .query(&format!(
            "SELECT count() FROM {} \
             WHERE traversal_path = '{DELETED_NAMESPACE_PATH}' AND id = 1 AND _deleted = true \
               AND path = 'src/lib.rs' AND name = 'lib.rs' AND extension = 'rs'",
            t("gl_file")
        ))
        .await;
    let batch = result.first().expect("count batch");
    let count = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .expect("count column")
        .value(0);
    assert_eq!(
        count, 1,
        "the file tombstone must carry its unchanging columns"
    );
}

async fn assert_sibling_graph_data_retained(context: &TestContext) {
    for table in [t("gl_project"), t("gl_file"), t("gl_edge")] {
        assert_active_row_count(context, &table, SIBLING_NAMESPACE_PATH, 1).await;
    }
}

async fn assert_deletion_schedule_marked_complete(context: &TestContext) {
    let result = context
        .query(&format!(
            "SELECT 1 FROM {} FINAL \
             WHERE namespace_id = {DELETED_NAMESPACE_ID} AND _deleted = false",
            t("namespace_deletion_schedule")
        ))
        .await;
    let count = result.first().map_or(0, |batch| batch.num_rows());
    assert_eq!(count, 0, "deletion schedule should be marked as complete");
}

async fn assert_sdlc_checkpoints_deleted(context: &TestContext) {
    let result = context
        .query(&format!(
            "SELECT key FROM {} FINAL \
             WHERE startsWith(key, 'ns.100.') AND _deleted = false",
            t("checkpoint")
        ))
        .await;
    let count = result.first().map_or(0, |b| b.num_rows());
    assert_eq!(
        count, 0,
        "SDLC checkpoints for deleted namespace should be soft-deleted"
    );
}

async fn assert_sibling_sdlc_checkpoints_retained(context: &TestContext) {
    let result = context
        .query(&format!(
            "SELECT key FROM {} FINAL \
             WHERE startsWith(key, 'ns.200.') AND _deleted = false",
            t("checkpoint")
        ))
        .await;
    let count = result.first().map_or(0, |b| b.num_rows());
    assert_eq!(
        count, 1,
        "SDLC checkpoints for sibling namespace should remain"
    );
}

async fn assert_code_checkpoints_deleted(context: &TestContext) {
    let result = context
        .query(&format!(
            "SELECT 1 FROM {} FINAL \
             WHERE startsWith(traversal_path, '1/100/') AND _deleted = false",
            t("code_indexing_checkpoint")
        ))
        .await;
    let count = result.first().map_or(0, |b| b.num_rows());
    assert_eq!(
        count, 0,
        "code checkpoints for deleted namespace should be soft-deleted"
    );
}

async fn assert_sibling_code_checkpoints_retained(context: &TestContext) {
    let result = context
        .query(&format!(
            "SELECT 1 FROM {} FINAL \
             WHERE startsWith(traversal_path, '1/200/') AND _deleted = false",
            t("code_indexing_checkpoint")
        ))
        .await;
    let count = result.first().map_or(0, |b| b.num_rows());
    assert_eq!(
        count, 1,
        "code checkpoints for sibling namespace should remain"
    );
}

const ROOT_PATH: &str = "1/300/";
const MOVED_OLD_PATH: &str = "1/300/9/";
const LIVE_CURRENT_PATH: &str = "1/300/7/";

#[tokio::test]
async fn reconcile_tombstones_old_path_rows_left_by_a_move() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL]).await;
    let ontology = ontology::Ontology::load_embedded().expect("ontology must load");

    seed_moved_and_live_graph_rows(&context).await;
    seed_current_routes(&context).await;

    reconcile_root(&context, &ontology, ROOT_PATH).await;

    assert_active_row_count(&context, &t("gl_project"), MOVED_OLD_PATH, 0).await;
    assert_active_row_count(&context, &t("gl_edge"), MOVED_OLD_PATH, 0).await;
    assert_active_row_count(&context, &t("gl_project"), LIVE_CURRENT_PATH, 1).await;
    assert_active_row_count(&context, &t("gl_edge"), LIVE_CURRENT_PATH, 1).await;
}

#[tokio::test]
async fn reconcile_is_a_noop_when_root_has_no_current_routes() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL]).await;
    let ontology = ontology::Ontology::load_embedded().expect("ontology must load");

    seed_moved_and_live_graph_rows(&context).await;

    reconcile_root(&context, &ontology, ROOT_PATH).await;

    assert_active_row_count(&context, &t("gl_project"), LIVE_CURRENT_PATH, 1).await;
    assert_active_row_count(&context, &t("gl_project"), MOVED_OLD_PATH, 1).await;
}

const TRANSFERRED_OLD_PATH: &str = "1/300/100/";
const TRANSFERRED_NEW_PATH: &str = "1/300/200/";
const TRANSFERRED_PROJECT_ID: i64 = 1;
const CONTROL_PROJECT_ID: i64 = 2;

#[tokio::test]
async fn reconcile_tombstones_old_path_after_move() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL]).await;
    let ontology = ontology::Ontology::load_embedded().expect("ontology must load");

    seed_duplicated_across_old_and_new_path(&context).await;
    seed_routes_after_transfer(&context).await;

    assert_active_id_count(&context, TRANSFERRED_PROJECT_ID, 2).await;

    reconcile_root(&context, &ontology, ROOT_PATH).await;

    assert_active_row_count(&context, &t("gl_project"), TRANSFERRED_OLD_PATH, 0).await;
    assert_active_row_count(&context, &t("gl_edge"), TRANSFERRED_OLD_PATH, 0).await;
    assert_active_row_count(&context, &t("gl_project"), TRANSFERRED_NEW_PATH, 2).await;
    assert_active_row_count(&context, &t("gl_edge"), TRANSFERRED_NEW_PATH, 2).await;
    assert_active_id_count(&context, TRANSFERRED_PROJECT_ID, 1).await;
    assert_active_id_count(&context, CONTROL_PROJECT_ID, 1).await;

    reconcile_root(&context, &ontology, ROOT_PATH).await;

    assert_active_id_count(&context, TRANSFERRED_PROJECT_ID, 1).await;
    assert_active_id_count(&context, CONTROL_PROJECT_ID, 1).await;
}

const LARGE_ROOT_PATH: &str = "1/400/";
const LARGE_ROOT_CURRENT_PREFIX: &str = "1/400/10/";
const LARGE_ROOT_STALE_PREFIX: &str = "1/400/20/";

#[tokio::test]
async fn reconcile_handles_route_sets_larger_than_a_request_uri() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL]).await;
    let ontology = ontology::Ontology::load_embedded().expect("ontology must load");

    context
        .execute(&format!(
            "INSERT INTO namespace_traversal_paths (id, traversal_path, deleted) VALUES \
             (400, '{LARGE_ROOT_PATH}', false)"
        ))
        .await;
    context
        .execute(&format!(
            "INSERT INTO project_namespace_traversal_paths (id, traversal_path, deleted) \
             SELECT number, concat('{LARGE_ROOT_CURRENT_PREFIX}', toString(1000000000 + number), '/'), false \
             FROM numbers(3000)"
        ))
        .await;
    context
        .execute(&format!(
            "INSERT INTO {} (traversal_path, id, _version, _deleted) \
             SELECT concat('{LARGE_ROOT_CURRENT_PREFIX}', toString(1000000000 + number), '/'), number, \
                    '2024-01-01 00:00:00.000000', false \
             FROM numbers(3000)",
            t("gl_project")
        ))
        .await;
    context
        .execute(&format!(
            "INSERT INTO {} (traversal_path, id, _version, _deleted) \
             SELECT concat('{LARGE_ROOT_STALE_PREFIX}', toString(1000000000 + number), '/'), 10000 + number, \
                    '2024-01-01 00:00:00.000000', false \
             FROM numbers(2000)",
            t("gl_project")
        ))
        .await;

    reconcile_root(&context, &ontology, LARGE_ROOT_PATH).await;

    assert_active_row_count(&context, &t("gl_project"), LARGE_ROOT_STALE_PREFIX, 0).await;
    assert_active_row_count(&context, &t("gl_project"), LARGE_ROOT_CURRENT_PREFIX, 3000).await;
}

const MOVED_ROOT_ID: i64 = 500;
const MOVED_ROOT_PATH: &str = "1/500/";
const MOVED_FROM_PATH: &str = "1/500/510/5000/";
const MOVED_TO_PATH: &str = "1/500/520/5000/";

#[tokio::test]
async fn scheduled_reconcile_removes_a_moved_project_under_a_root_over_the_uri_cap() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL]).await;

    create_namespace_with_path(&context, MOVED_ROOT_ID, None, 20, MOVED_ROOT_PATH, None).await;
    create_namespace_with_path(&context, 510, Some(MOVED_ROOT_ID), 20, "1/500/510/", None).await;
    create_namespace_with_path(&context, 520, Some(MOVED_ROOT_ID), 20, "1/500/520/", None).await;
    create_project(&context, 5000, 510, 1, 20, MOVED_FROM_PATH).await;
    context
        .execute(&format!(
            "INSERT INTO namespace_traversal_paths (id, traversal_path) \
             SELECT 100000 + number, concat('{MOVED_ROOT_PATH}', toString(1000000000 + number), '/') \
             FROM numbers(3000)"
        ))
        .await;
    context
        .execute(&format!(
            "INSERT INTO siphon_knowledge_graph_enabled_namespaces \
             (id, root_namespace_id, traversal_path) VALUES \
             ({MOVED_ROOT_ID}, {MOVED_ROOT_ID}, '{MOVED_ROOT_PATH}')"
        ))
        .await;

    index_moved_root(&context).await;
    assert_active_row_count(&context, &t("gl_project"), MOVED_FROM_PATH, 1).await;

    context
        .execute(&format!(
            "INSERT INTO {} (key, watermark, cursor_values, indexed_at) VALUES \
             ('ns.{MOVED_ROOT_ID}.Project', '2024-01-20 12:00:00.000000', 'null', '2024-01-20 12:00:00.000000')",
            t("checkpoint")
        ))
        .await;
    context
        .execute(&format!(
            "INSERT INTO project_namespace_traversal_paths (id, traversal_path, version) VALUES \
             (5000, '{MOVED_TO_PATH}', '2030-01-01 00:00:00')"
        ))
        .await;
    context
        .execute(
            "INSERT INTO siphon_projects \
             (id, name, description, visibility_level, path, namespace_id, creator_id, \
              created_at, updated_at, archived, star_count, last_activity_at, _siphon_replicated_at) \
             VALUES (5000, 'project-5000', NULL, 20, 'project-5000', 520, 1, \
                     '2023-01-01', '2024-01-20', false, 0, '2024-01-20', '2024-01-20 18:00:00')",
        )
        .await;

    index_moved_root(&context).await;
    assert_active_row_count(&context, &t("gl_project"), MOVED_FROM_PATH, 1).await;
    assert_active_row_count(&context, &t("gl_project"), MOVED_TO_PATH, 1).await;

    run_namespace_deletion_scheduler(&context).await;

    for table in [t("gl_project"), t("gl_edge")] {
        assert_active_row_count(&context, &table, MOVED_FROM_PATH, 0).await;
    }
    assert_active_row_count(&context, &t("gl_project"), MOVED_TO_PATH, 1).await;
    assert_active_row_count(&context, &t("gl_group"), MOVED_ROOT_PATH, 3).await;
}

async fn index_moved_root(context: &TestContext) {
    namespace_handler(context)
        .await
        .handle(handler_context(), namespace_envelope(1, MOVED_ROOT_ID))
        .await
        .expect("namespace indexing must succeed");
}

async fn run_namespace_deletion_scheduler(context: &TestContext) {
    let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
    let graph = Arc::new(context.config.build_client("graph"));
    let store: Arc<dyn NamespaceDeletionStore> = Arc::new(ClickHouseNamespaceDeletionStore::new(
        Arc::new(context.config.build_client("graph")),
        graph.clone(),
        &ontology,
    ));
    NamespaceDeletionScheduler::new(
        store,
        Arc::new(ClickHouseCheckpointStore::new(graph)),
        Arc::new(MockNatsServices::new()),
        ScheduledTaskMetrics::new(),
        orbit_server_config::AppConfig::embedded_defaults()
            .schedule
            .tasks
            .namespace_deletion,
    )
    .run()
    .await
    .expect("namespace deletion scheduler must succeed");
}

async fn seed_duplicated_across_old_and_new_path(context: &TestContext) {
    context
        .execute(&format!(
            "INSERT INTO {} (traversal_path, id, _version, _deleted) VALUES \
             ('{TRANSFERRED_OLD_PATH}', {TRANSFERRED_PROJECT_ID}, '2024-01-01 00:00:00.000000', false), \
             ('{TRANSFERRED_NEW_PATH}', {TRANSFERRED_PROJECT_ID}, '2024-02-01 00:00:00.000000', false), \
             ('{TRANSFERRED_NEW_PATH}', {CONTROL_PROJECT_ID}, '2024-01-01 00:00:00.000000', false)",
            t("gl_project")
        ))
        .await;

    context
        .execute(&format!(
            "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, _version, _deleted) VALUES \
             ('{TRANSFERRED_OLD_PATH}', {TRANSFERRED_PROJECT_ID}, 'Project', 'has_member', 11, 'User', '2024-01-01 00:00:00.000000', false), \
             ('{TRANSFERRED_NEW_PATH}', {TRANSFERRED_PROJECT_ID}, 'Project', 'has_member', 11, 'User', '2024-02-01 00:00:00.000000', false), \
             ('{TRANSFERRED_NEW_PATH}', {CONTROL_PROJECT_ID}, 'Project', 'has_member', 22, 'User', '2024-01-01 00:00:00.000000', false)",
            t("gl_edge")
        ))
        .await;
}

async fn seed_routes_after_transfer(context: &TestContext) {
    context
        .execute(&format!(
            "INSERT INTO project_namespace_traversal_paths (id, traversal_path, deleted) VALUES \
             ({TRANSFERRED_PROJECT_ID}, '{TRANSFERRED_NEW_PATH}', false), \
             ({CONTROL_PROJECT_ID}, '{TRANSFERRED_NEW_PATH}', false)"
        ))
        .await;
    context
        .execute(&format!(
            "INSERT INTO namespace_traversal_paths (id, traversal_path, deleted) VALUES \
             (300, '{ROOT_PATH}', false)"
        ))
        .await;
}

async fn assert_active_id_count(context: &TestContext, id: i64, expected: usize) {
    let result = context
        .query(&format!(
            "SELECT 1 FROM {} FINAL WHERE id = {id} AND _deleted = false",
            t("gl_project")
        ))
        .await;

    let actual = result.first().map_or(0, |batch| batch.num_rows());

    assert_eq!(
        actual, expected,
        "gl_project: expected {expected} active rows for id {id}, got {actual}"
    );
}

async fn seed_moved_and_live_graph_rows(context: &TestContext) {
    context
        .execute(&format!(
            "INSERT INTO {} (traversal_path, id, _version, _deleted) VALUES \
             ('{MOVED_OLD_PATH}', 9, '2024-01-01 00:00:00.000000', false), \
             ('{LIVE_CURRENT_PATH}', 7, '2024-01-01 00:00:00.000000', false)",
            t("gl_project")
        ))
        .await;

    context
        .execute(&format!(
            "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, _version, _deleted) VALUES \
             ('{MOVED_OLD_PATH}', 9, 'Project', 'has_member', 90, 'User', '2024-01-01 00:00:00.000000', false), \
             ('{LIVE_CURRENT_PATH}', 7, 'Project', 'has_member', 70, 'User', '2024-01-01 00:00:00.000000', false)",
            t("gl_edge")
        ))
        .await;
}

async fn seed_current_routes(context: &TestContext) {
    context
        .execute(&format!(
            "INSERT INTO project_namespace_traversal_paths (id, traversal_path, deleted) VALUES \
             (7, '{LIVE_CURRENT_PATH}', false)"
        ))
        .await;
    context
        .execute(&format!(
            "INSERT INTO namespace_traversal_paths (id, traversal_path, deleted) VALUES \
             (300, '{ROOT_PATH}', false)"
        ))
        .await;
}

async fn reconcile_root(context: &TestContext, ontology: &ontology::Ontology, root: &str) {
    let graph = Arc::new(context.config.build_client("graph"));
    let datalake = Arc::new(context.config.build_client("graph"));
    let store: Arc<dyn NamespaceDeletionStore> = Arc::new(ClickHouseNamespaceDeletionStore::new(
        datalake, graph, ontology,
    ));

    let outcomes = store
        .reconcile_moved_entities(&TraversalPath::new_unchecked(root))
        .await;
    for outcome in &outcomes {
        assert!(
            outcome.error.is_none(),
            "{}: reconcile failed: {:?}",
            outcome.table,
            outcome.error
        );
    }
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

    let actual: usize = result.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(
        actual, expected,
        "{table}: expected {expected} active rows for traversal_path '{traversal_path}', got {actual}"
    );
}
