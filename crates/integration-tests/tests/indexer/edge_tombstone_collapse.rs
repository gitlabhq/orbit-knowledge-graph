use std::sync::Arc;

use arrow::array::{BooleanArray, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use clickhouse_client::{ClickHouseConfigurationExt, FromArrowColumn};
use indexer::checkpoint::{CheckpointStore, ClickHouseCheckpointStore};
use indexer::clickhouse::ClickHouseWriter;
use indexer::metrics::EngineMetrics;
use indexer::orchestrator::scheduled::edge_tombstone_collapse::EdgeTombstoneCollapse;
use indexer::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics};
use integration_testkit::{GRAPH_SCHEMA_SQL, TestContext, t};
use orbit_server_config::EdgeTombstoneCollapseConfig;

const TASK_NAME: &str = "maintenance.edge_tombstone_collapse";
const TEN_YEARS_SECS: u64 = 3650 * 24 * 60 * 60;
const SCOPE: &str = "1/100/";
const OLD: &str = "2020-01-01 00:00:00.000000";
const TOMBSTONED: &str = "2026-01-01 00:00:00.000000";
const REINDEXED: &str = "2026-01-01 00:00:01.000000";

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

async fn seed_edge(
    context: &TestContext,
    scope: &str,
    source_id: i64,
    version: &str,
    deleted: bool,
) {
    context
        .execute(&format!(
            "INSERT INTO {} \
             (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, _version, _deleted) \
             VALUES ('{}', {source_id}, 'User', 'MEMBER_OF', {source_id}0, 'Project', '{version}', {deleted})",
            t("gl_edge"),
            scope.replace('\'', "''"),
        ))
        .await;
}

async fn seed_code_edges(context: &TestContext, count: usize, deleted: bool) {
    context
        .execute(&format!(
            "INSERT INTO {} \
             (traversal_path, project_id, branch, source_id, source_kind, relationship_kind, target_id, target_kind, _version, _deleted) \
             SELECT '{SCOPE}', 7, 'main', number, 'Definition', 'CALLS', number + 1, 'Definition', '{TOMBSTONED}', {deleted} \
             FROM numbers({count})",
            t("gl_code_edge")
        ))
        .await;
}

async fn seed_code_checkpoint(context: &TestContext, scope: &str) {
    context
        .execute(&format!(
            "INSERT INTO {} \
             (traversal_path, project_id, branch, last_task_id, last_commit, indexed_at) \
             VALUES ('{}', 7, 'main', 1, 'abc', '{REINDEXED}')",
            t("code_indexing_checkpoint"),
            scope.replace('\'', "''"),
        ))
        .await;
}

async fn physical_rows(context: &TestContext, table: &str) -> usize {
    let result = context
        .query(&format!("SELECT toInt64(count()) FROM {}", t(table)))
        .await;
    i64::extract_column(&result, 0).unwrap()[0] as usize
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

async fn wait_for_physical_rows(context: &TestContext, table: &str, expected: usize) {
    for _ in 0..100 {
        if physical_rows(context, table).await == expected {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(physical_rows(context, table).await, expected, "{table}");
}

#[tokio::test]
async fn reclaims_tombstones_in_a_reindexed_scope() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    seed_edge(&context, SCOPE, 1, OLD, false).await;
    seed_edge(&context, SCOPE, 2, OLD, false).await;
    seed_edge(&context, SCOPE, 1, TOMBSTONED, true).await;
    seed_code_checkpoint(&context, SCOPE).await;

    collapse(&context, 100_000).run().await.unwrap();

    wait_for_physical_rows(&context, "gl_edge", 1).await;
    assert_eq!(physical_source_ids(&context).await, vec![2]);
}

#[tokio::test]
async fn ignores_scopes_absent_from_the_code_checkpoint() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    seed_edge(&context, SCOPE, 1, OLD, false).await;
    seed_edge(&context, SCOPE, 1, TOMBSTONED, true).await;

    collapse(&context, 100_000).run().await.unwrap();

    assert_eq!(physical_source_ids(&context).await, vec![1, 1]);
}

#[tokio::test]
async fn keeps_an_edge_re_emitted_above_its_tombstone() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    seed_edge(&context, SCOPE, 1, OLD, false).await;
    seed_edge(&context, SCOPE, 1, TOMBSTONED, true).await;
    seed_edge(&context, SCOPE, 1, "2026-02-01 00:00:00.000000", false).await;
    seed_code_checkpoint(&context, SCOPE).await;

    collapse(&context, 100_000).run().await.unwrap();

    assert_eq!(live_source_ids(&context).await, vec![1]);
}

#[tokio::test]
async fn advances_the_cursor_only_once_every_table_is_drained() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    seed_edge(&context, SCOPE, 1, TOMBSTONED, true).await;
    seed_edge(&context, SCOPE, 2, TOMBSTONED, true).await;
    seed_code_edges(&context, 3, true).await;
    seed_code_checkpoint(&context, SCOPE).await;
    let store = checkpoint_store(&context);

    collapse(&context, 2).run().await.unwrap();
    wait_for_physical_rows(&context, "gl_edge", 0).await;
    wait_for_physical_rows(&context, "gl_code_edge", 1).await;
    assert!(store.load(TASK_NAME).await.unwrap().is_none());

    collapse(&context, 2).run().await.unwrap();
    wait_for_physical_rows(&context, "gl_code_edge", 0).await;
    assert!(store.load(TASK_NAME).await.unwrap().is_some());
}

#[tokio::test]
async fn splits_large_key_lists_across_delete_statements() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    seed_code_edges(&context, 6_000, true).await;
    seed_code_checkpoint(&context, SCOPE).await;

    collapse(&context, 100_000).run().await.unwrap();

    wait_for_physical_rows(&context, "gl_code_edge", 0).await;
}

#[tokio::test]
async fn probes_more_than_fifty_scopes_and_quoted_paths() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    for n in 0..60 {
        let scope = format!("1/{n}'/");
        seed_edge(&context, &scope, n, TOMBSTONED, true).await;
        seed_code_checkpoint(&context, &scope).await;
    }

    collapse(&context, 100_000).run().await.unwrap();

    wait_for_physical_rows(&context, "gl_edge", 0).await;
}

#[tokio::test]
async fn collapses_tombstones_written_by_the_sdlc_writer() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    seed_edge(&context, SCOPE, 1, OLD, false).await;
    let writer =
        ClickHouseWriter::new(context.config.clone(), Arc::new(EngineMetrics::default())).unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("traversal_path", DataType::Utf8, false),
            Field::new("source_id", DataType::Int64, false),
            Field::new("source_kind", DataType::Utf8, false),
            Field::new("relationship_kind", DataType::Utf8, false),
            Field::new("target_id", DataType::Int64, false),
            Field::new("target_kind", DataType::Utf8, false),
            Field::new(
                "_version",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("_deleted", DataType::Boolean, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec![SCOPE])),
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["User"])),
            Arc::new(StringArray::from(vec!["MEMBER_OF"])),
            Arc::new(Int64Array::from(vec![10])),
            Arc::new(StringArray::from(vec!["Project"])),
            Arc::new(
                TimestampMicrosecondArray::from(vec![1_767_225_600_000_000]).with_timezone("UTC"),
            ),
            Arc::new(BooleanArray::from(vec![true])),
        ],
    )
    .unwrap();
    writer
        .write(&t("gl_edge"), vec![batch], None)
        .await
        .unwrap();

    collapse(&context, 100_000).run().await.unwrap();

    wait_for_physical_rows(&context, "gl_edge", 0).await;
}
