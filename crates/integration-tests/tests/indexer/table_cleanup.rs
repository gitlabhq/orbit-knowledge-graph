use clickhouse_client::{ClickHouseConfigurationExt, FromArrowColumn};
use indexer::orchestrator::scheduled::table_cleanup::TableCleanup;
use indexer::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics};
use integration_testkit::{GRAPH_SCHEMA_SQL, TestContext, t};
use orbit_server_config::TableCleanupConfig;

fn build_tombstone_sweep_task(context: &TestContext) -> TableCleanup {
    let ontology = ontology::Ontology::load_embedded().unwrap();
    TableCleanup::new(
        context.config.build_client(),
        &ontology,
        ScheduledTaskMetrics::new(),
        TableCleanupConfig::default(),
    )
}

/// Fixed past timestamps would fall outside the sweep's `_version` window.
async fn seed_user(context: &TestContext, id: i64, hours_ago: u32, deleted: bool) {
    context
        .execute(&format!(
            "INSERT INTO {} (id, username, _version, _deleted) \
             VALUES ({id}, 'u{id}', now() - INTERVAL {hours_ago} HOUR, {deleted})",
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
async fn sweep_succeeds_on_every_table() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    build_tombstone_sweep_task(&context).run().await.unwrap();
}

#[tokio::test]
async fn sweep_removes_a_tombstoned_row_and_its_superseded_version() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    seed_user(&context, 1, 48, false).await;
    seed_user(&context, 2, 48, false).await;
    seed_user(&context, 1, 24, true).await;

    build_tombstone_sweep_task(&context).run().await.unwrap();

    assert_eq!(live_user_ids(&context).await, vec![2]);
    let total = context
        .query(&format!("SELECT toInt64(count()) FROM {}", t("gl_user")))
        .await;
    assert_eq!(
        i64::extract_column(&total, 0).unwrap(),
        vec![1],
        "the tombstone must go too, not just the row it superseded"
    );
}

#[tokio::test]
async fn sweep_keeps_a_row_recreated_after_its_tombstone() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    seed_user(&context, 7, 48, false).await;
    seed_user(&context, 7, 24, true).await;
    seed_user(&context, 7, 1, false).await;

    build_tombstone_sweep_task(&context).run().await.unwrap();

    assert_eq!(live_user_ids(&context).await, vec![7]);
}

#[tokio::test]
async fn sweep_is_idempotent_across_runs() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    seed_user(&context, 1, 48, false).await;
    seed_user(&context, 2, 48, false).await;
    seed_user(&context, 1, 24, true).await;

    build_tombstone_sweep_task(&context).run().await.unwrap();
    build_tombstone_sweep_task(&context).run().await.unwrap();

    assert_eq!(live_user_ids(&context).await, vec![2]);
}

#[tokio::test]
async fn sweep_removes_tombstoned_edges() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    for (source_id, hours_ago, deleted) in [(1, 48, "false"), (2, 48, "false"), (1, 24, "true")] {
        context
            .execute(&format!(
                "INSERT INTO {} \
                 (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, _version, _deleted) \
                 VALUES ('1/100/', {source_id}, 'User', 'MEMBER_OF', {source_id}0, 'Project', now() - INTERVAL {hours_ago} HOUR, {deleted})",
                t("gl_edge")
            ))
            .await;
    }

    build_tombstone_sweep_task(&context).run().await.unwrap();

    let result = context
        .query(&format!(
            "SELECT source_id FROM {} FINAL WHERE _deleted = false ORDER BY source_id",
            t("gl_edge")
        ))
        .await;
    assert_eq!(i64::extract_column(&result, 0).unwrap(), vec![2]);
}
