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
        lookback_secs: 24 * 60 * 60,
        max_keys_per_run: 1_000_000,
        max_query_size_bytes: 10 * 1024 * 1024,
    }
}

fn checkpoint_store(context: &TestContext) -> Arc<ClickHouseCheckpointStore> {
    Arc::new(ClickHouseCheckpointStore::new(Arc::new(
        context.config.build_client(),
    )))
}

fn node_sweep(context: &TestContext) -> TombstoneSweep {
    let ontology = ontology::Ontology::load_embedded().unwrap();
    TombstoneSweep::for_node_tables(
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

async fn physical_row_count(context: &TestContext, table: &str, id: i64) -> usize {
    let result = context
        .query(&format!("SELECT id FROM {table} WHERE id = {id}"))
        .await;
    result.first().map_or(0, |b| b.num_rows())
}

#[tokio::test]
async fn node_sweep_physically_removes_tombstoned_rows() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    seed_user(&context, 1, "2020-01-01 00:00:00.000000", false).await;
    seed_user(&context, 2, "2020-01-01 00:00:00.000000", false).await;
    seed_user(&context, 1, "2026-01-01 00:00:00.000000", true).await;

    assert_eq!(live_user_ids(&context).await, vec![2]);

    node_sweep(&context).run().await.unwrap();

    assert_eq!(
        physical_row_count(&context, &t("gl_user"), 1).await,
        0,
        "the tombstoned key's rows must be gone from disk, not just hidden by FINAL"
    );
    assert_eq!(physical_row_count(&context, &t("gl_user"), 2).await, 1);
    assert_eq!(live_user_ids(&context).await, vec![2]);
}

#[tokio::test]
async fn node_sweep_keeps_a_key_resurrected_after_the_tombstone() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    seed_user(&context, 1, "2020-01-01 00:00:00.000000", false).await;
    seed_user(&context, 1, "2026-01-01 00:00:00.000000", true).await;
    // A newer live row lands after the tombstone; `_version <= window_end` keeps it.
    seed_user(&context, 1, "2035-01-01 00:00:00.000000", false).await;

    node_sweep(&context).run().await.unwrap();

    assert_eq!(
        live_user_ids(&context).await,
        vec![1],
        "a key re-created after its tombstone must survive the sweep"
    );
}

#[tokio::test]
async fn node_sweep_succeeds_on_empty_tables() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    node_sweep(&context).run().await.unwrap();
}

#[tokio::test]
async fn edge_sweep_physically_removes_tombstoned_edges() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    let edge = t("gl_edge");

    for (source_id, version, deleted) in [
        (1_i64, "2020-01-01 00:00:00.000000", false),
        (2, "2020-01-01 00:00:00.000000", false),
        (1, "2026-01-01 00:00:00.000000", true),
    ] {
        context
            .execute(&format!(
                "INSERT INTO {edge} \
                 (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, _version, _deleted) \
                 VALUES ('1/100/', {source_id}, 'User', 'MEMBER_OF', {source_id}0, 'Project', '{version}', {deleted})"
            ))
            .await;
    }

    edge_sweep(&context).run().await.unwrap();

    let physical = context
        .query(&format!("SELECT source_id FROM {edge} ORDER BY source_id"))
        .await;
    assert_eq!(
        i64::extract_column(&physical, 0).unwrap(),
        vec![2],
        "source 1's live and tombstone rows must both be physically gone"
    );
}
