use std::sync::Arc;

use clickhouse_client::{ClickHouseConfigurationExt, FromArrowColumn};
use gkg_server_config::TableCleanupConfig;
use indexer::orchestrator::scheduled::table_cleanup::TableCleanup;
use indexer::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics};
use integration_testkit::{GRAPH_SCHEMA_SQL, TestContext, t};

fn build_table_cleanup_task(context: &TestContext) -> TableCleanup {
    let ontology = ontology::Ontology::load_embedded().unwrap();
    let checkpoint_store = Arc::new(indexer::checkpoint::ClickHouseCheckpointStore::new(
        Arc::new(context.config.build_client()),
    ));
    TableCleanup::new(
        context.config.build_client(),
        &ontology,
        checkpoint_store,
        ScheduledTaskMetrics::new(),
        TableCleanupConfig::default(),
    )
}

#[tokio::test]
async fn cleanup_succeeds_on_all_tables() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    build_table_cleanup_task(&context).run().await.unwrap();
}

#[tokio::test]
async fn cleanup_removes_soft_deleted_rows() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    context
        .execute(&format!(
            "INSERT INTO {} (id, username, _version, _deleted) VALUES \
             (1, 'alice', '2024-01-01 00:00:00.000000', false), \
             (2, 'bob',   '2024-01-01 00:00:00.000000', false)",
            t("gl_user")
        ))
        .await;

    context
        .execute(&format!(
            "INSERT INTO {} (id, username, _version, _deleted) VALUES \
             (1, 'alice', '2024-01-02 00:00:00.000000', true)",
            t("gl_user")
        ))
        .await;

    build_table_cleanup_task(&context).run().await.unwrap();

    let result = context
        .query(&format!("SELECT id FROM {}", t("gl_user")))
        .await;
    let ids = i64::extract_column(&result, 0).unwrap();

    assert_eq!(ids, vec![2], "only non-deleted user should remain");
}

#[tokio::test]
async fn cleanup_removes_soft_deleted_edges() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    context
        .execute(&format!(
            "INSERT INTO {} \
             (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, _version, _deleted) \
             VALUES \
             ('1/', 1, 'User', 'AUTHORED', 10, 'MergeRequest', '2024-01-01 00:00:00.000000', false), \
             ('1/', 2, 'User', 'AUTHORED', 20, 'MergeRequest', '2024-01-01 00:00:00.000000', false)",
            t("gl_edge")
        ))
        .await;

    context
        .execute(&format!(
            "INSERT INTO {} \
             (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, _version, _deleted) \
             VALUES \
             ('1/', 1, 'User', 'AUTHORED', 10, 'MergeRequest', '2024-01-02 00:00:00.000000', true)",
            t("gl_edge")
        ))
        .await;

    build_table_cleanup_task(&context).run().await.unwrap();

    let result = context
        .query(&format!("SELECT source_id FROM {}", t("gl_edge")))
        .await;
    let source_ids = i64::extract_column(&result, 0).unwrap();

    assert_eq!(source_ids, vec![2], "only non-deleted edge should remain");
}

#[tokio::test]
async fn cleanup_keeps_an_edge_recreated_after_its_tombstone() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    for (version, deleted) in [
        ("2024-01-01 00:00:00.000000", "false"),
        ("2024-01-02 00:00:00.000000", "true"),
        ("2024-01-03 00:00:00.000000", "false"),
    ] {
        context
            .execute(&format!(
                "INSERT INTO {} \
                 (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, _version, _deleted) \
                 VALUES ('1/', 7, 'User', 'MEMBER_OF', 70, 'Project', '{version}', {deleted})",
                t("gl_edge")
            ))
            .await;
    }

    build_table_cleanup_task(&context).run().await.unwrap();

    let result = context
        .query(&format!(
            "SELECT source_id FROM {} FINAL WHERE relationship_kind = 'MEMBER_OF' AND _deleted = false",
            t("gl_edge")
        ))
        .await;
    let source_ids = i64::extract_column(&result, 0).unwrap();

    assert_eq!(
        source_ids,
        vec![7],
        "re-created edge must survive the sweep"
    );
}

#[tokio::test]
async fn cleanup_is_idempotent_across_runs() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    context
        .execute(&format!(
            "INSERT INTO {} (id, username, _version, _deleted) VALUES \
             (1, 'alice', '2024-01-01 00:00:00.000000', false), \
             (2, 'bob',   '2024-01-01 00:00:00.000000', false)",
            t("gl_user")
        ))
        .await;
    context
        .execute(&format!(
            "INSERT INTO {} (id, username, _version, _deleted) VALUES \
             (1, 'alice', '2024-01-02 00:00:00.000000', true)",
            t("gl_user")
        ))
        .await;

    build_table_cleanup_task(&context).run().await.unwrap();
    build_table_cleanup_task(&context).run().await.unwrap();

    let result = context
        .query(&format!("SELECT id FROM {}", t("gl_user")))
        .await;
    let ids = i64::extract_column(&result, 0).unwrap();

    assert_eq!(ids, vec![2]);
}
