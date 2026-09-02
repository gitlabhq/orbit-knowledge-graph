use std::sync::Arc;

use clickhouse_client::{ClickHouseConfigurationExt, FromArrowColumn};
use indexer::checkpoint::{CheckpointStore, ClickHouseCheckpointStore};
use indexer::orchestrator::scheduled::edge_tombstone_collapse::EdgeTombstoneCollapse;
use indexer::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics};
use integration_testkit::{GRAPH_SCHEMA_SQL, TestContext, t};
use orbit_server_config::EdgeTombstoneCollapseConfig;

const TASK_NAME: &str = "maintenance.edge_tombstone_collapse";
const TEN_YEARS_SECS: u64 = 3650 * 24 * 60 * 60;

fn checkpoint_store(context: &TestContext) -> Arc<ClickHouseCheckpointStore> {
    Arc::new(ClickHouseCheckpointStore::new(Arc::new(
        context.config.build_client(),
    )))
}

fn collapse(context: &TestContext, max_keys_per_run: usize) -> EdgeTombstoneCollapse {
    EdgeTombstoneCollapse::new(
        context.config.build_client(),
        &ontology::Ontology::load_embedded().unwrap(),
        checkpoint_store(context),
        ScheduledTaskMetrics::new(),
        EdgeTombstoneCollapseConfig {
            lookback_secs: TEN_YEARS_SECS,
            max_keys_per_run,
            ..EdgeTombstoneCollapseConfig::default()
        },
    )
}

async fn seed_edge(context: &TestContext, source_id: i64, version: &str, deleted: bool) {
    context
        .execute(&format!(
            "INSERT INTO {} \
             (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, _version, _deleted) \
             VALUES ('1/100/', {source_id}, 'User', 'MEMBER_OF', {source_id}0, 'Project', '{version}', {deleted})",
            t("gl_edge")
        ))
        .await;
}

async fn seed_code_checkpoint(context: &TestContext, indexed_at: &str) {
    context
        .execute(&format!(
            "INSERT INTO {} \
             (traversal_path, project_id, branch, last_task_id, last_commit, indexed_at) \
             VALUES ('1/100/', 7, 'main', 1, 'abc', '{indexed_at}')",
            t("code_indexing_checkpoint")
        ))
        .await;
}

async fn physical_source_ids(context: &TestContext) -> Vec<i64> {
    let result = context
        .query(&format!(
            "SELECT source_id FROM {} ORDER BY source_id",
            t("gl_edge")
        ))
        .await;
    i64::extract_column(&result, 0).unwrap_or_default()
}

async fn live_source_ids(context: &TestContext) -> Vec<i64> {
    let result = context
        .query(&format!(
            "SELECT source_id FROM {} FINAL WHERE _deleted = false ORDER BY source_id",
            t("gl_edge")
        ))
        .await;
    i64::extract_column(&result, 0).unwrap_or_default()
}

async fn wait_for_physical_row_count(context: &TestContext, expected: usize) {
    for _ in 0..100 {
        if physical_source_ids(context).await.len() == expected {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(physical_source_ids(context).await.len(), expected);
}

#[tokio::test]
async fn reclaims_tombstones_in_a_reindexed_scope() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    seed_edge(&context, 1, "2020-01-01 00:00:00.000000", false).await;
    seed_edge(&context, 2, "2020-01-01 00:00:00.000000", false).await;
    seed_edge(&context, 1, "2026-01-01 00:00:00.000000", true).await;
    seed_code_checkpoint(&context, "2026-01-01 00:00:01.000000").await;

    collapse(&context, 100_000).run().await.unwrap();

    wait_for_physical_row_count(&context, 1).await;
    assert_eq!(physical_source_ids(&context).await, vec![2]);
}

#[tokio::test]
async fn ignores_scopes_absent_from_the_code_checkpoint() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    seed_edge(&context, 1, "2020-01-01 00:00:00.000000", false).await;
    seed_edge(&context, 1, "2026-01-01 00:00:00.000000", true).await;

    collapse(&context, 100_000).run().await.unwrap();

    assert_eq!(physical_source_ids(&context).await, vec![1, 1]);
}

#[tokio::test]
async fn keeps_an_edge_re_emitted_above_its_tombstone() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    seed_edge(&context, 1, "2020-01-01 00:00:00.000000", false).await;
    seed_edge(&context, 1, "2026-01-01 00:00:00.000000", true).await;
    seed_edge(&context, 1, "2026-02-01 00:00:00.000000", false).await;
    seed_code_checkpoint(&context, "2026-02-01 00:00:01.000000").await;

    collapse(&context, 100_000).run().await.unwrap();

    assert_eq!(live_source_ids(&context).await, vec![1]);
}

#[tokio::test]
async fn advances_the_cursor_only_once_the_scope_is_drained() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    seed_edge(&context, 1, "2026-01-01 00:00:00.000000", true).await;
    seed_edge(&context, 2, "2026-01-01 00:00:00.000000", true).await;
    seed_code_checkpoint(&context, "2026-01-01 00:00:01.000000").await;
    let store = checkpoint_store(&context);

    collapse(&context, 1).run().await.unwrap();
    wait_for_physical_row_count(&context, 1).await;
    assert!(store.load(TASK_NAME).await.unwrap().is_none());

    collapse(&context, 1).run().await.unwrap();
    wait_for_physical_row_count(&context, 0).await;
    assert!(store.load(TASK_NAME).await.unwrap().is_some());
}
