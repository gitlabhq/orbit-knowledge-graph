use clickhouse_client::{ClickHouseConfigurationExt, FromArrowColumn};
use gkg_server_config::TableCleanupConfig;
use indexer::scheduler::table_cleanup::TableCleanup;
use indexer::scheduler::{ScheduledTask, ScheduledTaskMetrics};
use integration_testkit::{GRAPH_SCHEMA_SQL, TestContext, t};

// Verifies that OPTIMIZE TABLE ... FINAL CLEANUP is valid for every ontology table.
// Tables need allow_experimental_replacing_merge_with_cleanup enabled for this to work.
#[tokio::test]
async fn cleanup_succeeds_on_all_tables() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    let graph = context.config.build_client();
    let task = TableCleanup::new(
        graph,
        ScheduledTaskMetrics::new(),
        TableCleanupConfig::default(),
    );

    task.run().await.unwrap();
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

    let graph = context.config.build_client();
    let task = TableCleanup::new(
        graph,
        ScheduledTaskMetrics::new(),
        TableCleanupConfig::default(),
    );

    task.run().await.unwrap();

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

    let graph = context.config.build_client();
    let task = TableCleanup::new(
        graph,
        ScheduledTaskMetrics::new(),
        TableCleanupConfig::default(),
    );

    task.run().await.unwrap();

    let result = context
        .query(&format!("SELECT source_id FROM {}", t("gl_edge")))
        .await;
    let source_ids = i64::extract_column(&result, 0).unwrap();

    assert_eq!(source_ids, vec![2], "only non-deleted edge should remain");
}
