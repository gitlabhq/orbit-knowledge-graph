use std::sync::Arc;

use clickhouse_client::{ClickHouseConfigurationExt, FromArrowColumn};
use indexer::checkpoint::ClickHouseCheckpointStore;
use indexer::orchestrator::scheduled::tombstone_sweep::TombstoneSweep;
use indexer::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics};
use integration_testkit::{GRAPH_SCHEMA_SQL, TestContext, t};
use orbit_server_config::{ScheduleConfiguration, TombstoneSweepConfig};

fn drain_everything() -> TombstoneSweepConfig {
    TombstoneSweepConfig {
        schedule: ScheduleConfiguration { cron: None },
        // Reach back far enough that the window floor never excludes a seeded
        // tombstone as the calendar advances past its fixed _version.
        lookback_secs: 3650 * 24 * 60 * 60,
        max_keys_per_run: 1_000_000,
        max_query_size_bytes: 10 * 1024 * 1024,
    }
}

fn checkpoint_store(context: &TestContext) -> Arc<ClickHouseCheckpointStore> {
    Arc::new(ClickHouseCheckpointStore::new(Arc::new(
        context.config.build_client(),
    )))
}

fn all_tables_sweep(context: &TestContext) -> TombstoneSweep {
    let ontology = ontology::Ontology::load_embedded().unwrap();
    TombstoneSweep::for_all_tables(
        context.config.build_client(),
        &ontology,
        checkpoint_store(context),
        ScheduledTaskMetrics::new(),
        drain_everything(),
    )
}

fn edge_sweep(context: &TestContext) -> TombstoneSweep {
    let ontology = ontology::Ontology::load_embedded().unwrap();
    TombstoneSweep::for_edge_tables(
        context.config.build_client(),
        &ontology,
        checkpoint_store(context),
        ScheduledTaskMetrics::new(),
        drain_everything(),
    )
}

async fn seed_user(context: &TestContext, id: i64, version: &str, deleted: bool) {
    context
        .execute(&format!(
            "INSERT INTO {} (id, username, _version, _deleted) \
             VALUES ({id}, 'u{id}', '{version}', {deleted})",
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

async fn physical_ids(context: &TestContext, sql: &str) -> Vec<i64> {
    let result = context.query(sql).await;
    i64::extract_column(&result, 0).unwrap_or_default()
}

/// The sweep issues fire-and-forget deletes (`lightweight_deletes_sync = 0`), so
/// physical removal is not visible the instant `run()` returns; poll for it.
async fn wait_for_physical_ids(context: &TestContext, sql: &str, expected: Vec<i64>) {
    for _ in 0..100 {
        if physical_ids(context, sql).await == expected {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(
        physical_ids(context, sql).await,
        expected,
        "tombstoned rows were never physically removed"
    );
}

#[tokio::test]
async fn node_sweep_physically_removes_tombstoned_rows() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    seed_user(&context, 1, "2020-01-01 00:00:00.000000", false).await;
    seed_user(&context, 2, "2020-01-01 00:00:00.000000", false).await;
    seed_user(&context, 1, "2026-01-01 00:00:00.000000", true).await;

    assert_eq!(live_user_ids(&context).await, vec![2]);

    all_tables_sweep(&context).run().await.unwrap();

    wait_for_physical_ids(
        &context,
        &format!("SELECT id FROM {} ORDER BY id", t("gl_user")),
        vec![2],
    )
    .await;
    assert_eq!(live_user_ids(&context).await, vec![2]);
}

#[tokio::test]
async fn node_sweep_keeps_a_key_resurrected_after_the_tombstone() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    seed_user(&context, 1, "2020-01-01 00:00:00.000000", false).await;
    seed_user(&context, 1, "2026-01-01 00:00:00.000000", true).await;
    // A newer live row lands after the tombstone; `_version <= window_end` keeps it.
    seed_user(&context, 1, "2035-01-01 00:00:00.000000", false).await;

    all_tables_sweep(&context).run().await.unwrap();

    assert_eq!(
        live_user_ids(&context).await,
        vec![1],
        "a key re-created after its tombstone must survive the sweep"
    );
}

#[tokio::test]
async fn node_sweep_succeeds_on_empty_tables() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    all_tables_sweep(&context).run().await.unwrap();
}

async fn seed_edge(
    context: &TestContext,
    traversal_path: &str,
    source_id: i64,
    version: &str,
    deleted: bool,
) {
    context
        .execute(&format!(
            "INSERT INTO {} \
             (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, _version, _deleted) \
             VALUES ('{traversal_path}', {source_id}, 'User', 'MEMBER_OF', {source_id}0, 'Project', '{version}', {deleted})",
            t("gl_edge")
        ))
        .await;
}

async fn seed_code_checkpoint(context: &TestContext, traversal_path: &str, indexed_at: &str) {
    context
        .execute(&format!(
            "INSERT INTO {} \
             (traversal_path, project_id, branch, last_task_id, last_commit, indexed_at) \
             VALUES ('{traversal_path}', 7, 'main', 1, 'abc', '{indexed_at}')",
            t("code_indexing_checkpoint")
        ))
        .await;
}

async fn edge_source_ids(context: &TestContext) -> Vec<i64> {
    physical_ids(
        context,
        &format!("SELECT source_id FROM {} ORDER BY source_id", t("gl_edge")),
    )
    .await
}

#[tokio::test]
async fn edge_sweep_reclaims_tombstones_in_a_reindexed_scope() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    seed_edge(&context, "1/100/", 1, "2020-01-01 00:00:00.000000", false).await;
    seed_edge(&context, "1/100/", 2, "2020-01-01 00:00:00.000000", false).await;
    seed_edge(&context, "1/100/", 1, "2026-01-01 00:00:00.000000", true).await;
    seed_code_checkpoint(&context, "1/100/", "2026-01-01 00:00:01.000000").await;

    edge_sweep(&context).run().await.unwrap();

    wait_for_physical_ids(
        &context,
        &format!("SELECT source_id FROM {} ORDER BY source_id", t("gl_edge")),
        vec![2],
    )
    .await;
}

#[tokio::test]
async fn edge_sweep_ignores_scopes_absent_from_the_checkpoint() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    seed_edge(&context, "1/100/", 1, "2020-01-01 00:00:00.000000", false).await;
    seed_edge(&context, "1/100/", 1, "2026-01-01 00:00:00.000000", true).await;

    edge_sweep(&context).run().await.unwrap();
    // No checkpoint row for this scope, so the tight sweep never probes it; a scan
    // would have removed the tombstone. Both physical rows remain.
    assert_eq!(edge_source_ids(&context).await, vec![1, 1]);

    all_tables_sweep(&context).run().await.unwrap();
    wait_for_physical_ids(
        &context,
        &format!("SELECT source_id FROM {} ORDER BY source_id", t("gl_edge")),
        Vec::<i64>::new(),
    )
    .await;
}

#[tokio::test]
async fn edge_sweep_keeps_an_edge_re_emitted_by_the_reindex() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    seed_edge(&context, "1/100/", 1, "2020-01-01 00:00:00.000000", false).await;
    seed_edge(&context, "1/100/", 1, "2026-01-01 00:00:00.000000", true).await;
    seed_edge(&context, "1/100/", 1, "2026-02-01 00:00:00.000000", false).await;
    seed_code_checkpoint(&context, "1/100/", "2026-02-01 00:00:01.000000").await;

    edge_sweep(&context).run().await.unwrap();

    let result = context
        .query(&format!(
            "SELECT source_id FROM {} FINAL WHERE _deleted = false ORDER BY source_id",
            t("gl_edge")
        ))
        .await;
    assert_eq!(
        i64::extract_column(&result, 0).unwrap(),
        vec![1],
        "an edge re-emitted above its tombstone must survive the sweep"
    );
}
