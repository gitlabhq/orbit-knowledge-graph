use std::sync::Arc;

use clickhouse_client::ClickHouseConfigurationExt;
use indexer::checkpoint::{CheckpointStore, ClickHouseCheckpointStore};
use indexer::modules::code::config::CodeTableNames;
use indexer::orchestrator::dispatch::DispatchOutcome;
use indexer::orchestrator::scheduled::CodeStaleSweep;
use integration_testkit::{TestContext, t};

const WATERMARK: &str = "2026-01-02 00:00:00.000000";
const PRE_WATERMARK: &str = "2026-01-01 00:00:00.000000";

#[tokio::test]
async fn drained_backfill_sweeps_unclaimed_rows_once() {
    let project_id: i64 = 40;
    let traversal_path = "1/40/";
    let branch = "main";

    let clickhouse = TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    clickhouse
        .execute(&format!(
            "INSERT INTO {} (traversal_path, project_id, branch, last_task_id, indexed_at) \
             VALUES ('{traversal_path}', {project_id}, '{branch}', 1, '{WATERMARK}')",
            t("code_indexing_checkpoint")
        ))
        .await;

    insert_file(
        &clickhouse,
        traversal_path,
        project_id,
        branch,
        111,
        PRE_WATERMARK,
    )
    .await;
    insert_file(
        &clickhouse,
        traversal_path,
        project_id,
        branch,
        222,
        WATERMARK,
    )
    .await;
    clickhouse
        .execute(&format!(
            "INSERT INTO {} \
             (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, _version) \
             VALUES ('{traversal_path}', 111, 'File', 'ON_BRANCH', 999, 'Branch', '{PRE_WATERMARK}')",
            t("gl_edge")
        ))
        .await;
    clickhouse
        .execute(&format!(
            "INSERT INTO {} \
             (traversal_path, project_id, branch, source_id, source_kind, relationship_kind, target_id, target_kind, _version) \
             VALUES ('{traversal_path}', {project_id}, '{branch}', 111, 'File', 'DEFINES', 555, 'Definition', '{PRE_WATERMARK}')",
            t("gl_code_edge")
        ))
        .await;

    let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
    let table_names = CodeTableNames::from_ontology(&ontology).expect("code tables must resolve");
    let store = Arc::new(ClickHouseCheckpointStore::new(Arc::new(
        clickhouse.config.build_client(),
    )));
    let sweep = CodeStaleSweep::new(
        clickhouse.config.build_client(),
        &table_names,
        store.clone(),
    );

    sweep
        .run_after_drain(&DispatchOutcome {
            dispatched: 3,
            skipped: 0,
        })
        .await
        .expect("pending backfill must not sweep");
    assert!(
        file_is_active(&clickhouse, project_id, 111).await,
        "sweep must not run while the backfill still dispatches work"
    );

    let drained = DispatchOutcome {
        dispatched: 0,
        skipped: 0,
    };
    sweep.run_after_drain(&drained).await.expect("sweep failed");

    assert!(!file_is_active(&clickhouse, project_id, 111).await);
    assert!(file_is_active(&clickhouse, project_id, 222).await);
    assert_eq!(active_edge_count(&clickhouse, "gl_edge", 111).await, 0);
    assert_eq!(active_edge_count(&clickhouse, "gl_code_edge", 111).await, 0);
    assert!(
        store
            .load("maintenance.code_stale_sweep")
            .await
            .expect("load marker")
            .is_some(),
        "the sweep must record its maintenance checkpoint"
    );

    insert_file(
        &clickhouse,
        traversal_path,
        project_id,
        branch,
        333,
        PRE_WATERMARK,
    )
    .await;
    sweep
        .run_after_drain(&drained)
        .await
        .expect("marked sweep must be a no-op");
    assert!(
        file_is_active(&clickhouse, project_id, 333).await,
        "a completed sweep must not run again for the same schema version"
    );
}

async fn insert_file(
    clickhouse: &TestContext,
    traversal_path: &str,
    project_id: i64,
    branch: &str,
    id: i64,
    version: &str,
) {
    clickhouse
        .execute(&format!(
            "INSERT INTO {} \
             (id, traversal_path, project_id, branch, path, name, extension, language, _version) \
             VALUES ({id}, '{traversal_path}', {project_id}, '{branch}', \
                     'src/F{id}.java', 'F{id}.java', 'java', 'java', '{version}')",
            t("gl_file")
        ))
        .await;
}

async fn file_is_active(clickhouse: &TestContext, project_id: i64, id: i64) -> bool {
    let rows = clickhouse
        .query(&format!(
            "SELECT id FROM {} FINAL \
             WHERE project_id = {project_id} AND id = {id} AND _deleted = false",
            t("gl_file")
        ))
        .await;
    rows.first().is_some_and(|b| b.num_rows() > 0)
}

async fn active_edge_count(clickhouse: &TestContext, table: &str, source_id: i64) -> usize {
    let rows = clickhouse
        .query(&format!(
            "SELECT source_id FROM {} FINAL \
             WHERE source_id = {source_id} AND _deleted = false",
            t(table)
        ))
        .await;
    rows.first().map_or(0, |b| b.num_rows())
}
