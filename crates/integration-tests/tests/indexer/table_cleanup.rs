use clickhouse_client::{ClickHouseConfigurationExt, FromArrowColumn};
use indexer::orchestrator::scheduled::table_cleanup::TableCleanup;
use indexer::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics};
use integration_testkit::{GRAPH_SCHEMA_SQL, TestContext, t};
use orbit_server_config::TableCleanupConfig;

fn build_cleanup_task(context: &TestContext) -> TableCleanup {
    let ontology = ontology::Ontology::load_embedded().unwrap();
    TableCleanup::new(
        context.config.build_client(),
        &ontology,
        ScheduledTaskMetrics::new(),
        TableCleanupConfig::default(),
    )
}

async fn seed_user(context: &TestContext, id: i64) {
    context
        .execute(&format!(
            "INSERT INTO {} (id, username, _version, _deleted) \
             VALUES ({id}, 'u{id}', now(), false)",
            t("gl_user")
        ))
        .await;
}

async fn lightweight_delete_user(context: &TestContext, id: i64) {
    context
        .execute(&format!(
            "DELETE FROM {} WHERE id = {id} \
             SETTINGS lightweight_deletes_sync = 1",
            t("gl_user")
        ))
        .await;
}

async fn live_user_ids(context: &TestContext) -> Vec<i64> {
    let result = context
        .query(&format!(
            "SELECT id FROM {} FINAL WHERE _deleted = false ORDER BY id",
            t("gl_user")
        ))
        .await;
    i64::extract_column(&result, 0).unwrap()
}

#[tokio::test]
async fn apply_deleted_mask_succeeds_on_every_table() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    build_cleanup_task(&context).run().await.unwrap();
}

#[tokio::test]
async fn apply_deleted_mask_removes_lightweight_deleted_rows() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    seed_user(&context, 1).await;
    seed_user(&context, 2).await;
    lightweight_delete_user(&context, 1).await;

    assert_eq!(live_user_ids(&context).await, vec![2]);

    build_cleanup_task(&context).run().await.unwrap();

    assert_eq!(live_user_ids(&context).await, vec![2]);
}

#[tokio::test]
async fn apply_deleted_mask_is_idempotent() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    seed_user(&context, 1).await;
    seed_user(&context, 2).await;
    lightweight_delete_user(&context, 1).await;

    build_cleanup_task(&context).run().await.unwrap();
    build_cleanup_task(&context).run().await.unwrap();

    assert_eq!(live_user_ids(&context).await, vec![2]);
}

#[tokio::test]
async fn apply_deleted_mask_removes_lightweight_deleted_edges() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    for source_id in [1, 2] {
        context
            .execute(&format!(
                "INSERT INTO {} \
                 (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, _version, _deleted) \
                 VALUES ('1/100/', {source_id}, 'User', 'MEMBER_OF', {source_id}0, 'Project', now(), false)",
                t("gl_edge")
            ))
            .await;
    }

    context
        .execute(&format!(
            "DELETE FROM {} WHERE source_id = 1 \
             SETTINGS lightweight_deletes_sync = 1",
            t("gl_edge")
        ))
        .await;

    build_cleanup_task(&context).run().await.unwrap();

    let result = context
        .query(&format!(
            "SELECT source_id FROM {} FINAL WHERE _deleted = false ORDER BY source_id",
            t("gl_edge")
        ))
        .await;
    assert_eq!(i64::extract_column(&result, 0).unwrap(), vec![2]);
}
