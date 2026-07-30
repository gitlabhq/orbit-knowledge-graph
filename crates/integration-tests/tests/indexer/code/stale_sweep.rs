use std::sync::Arc;

use clickhouse_client::ClickHouseConfigurationExt;
use indexer::checkpoint::{CheckpointStore, ClickHouseCheckpointStore};
use indexer::modules::code::config::CodeTableNames;
use indexer::orchestrator::scheduled::CodeStaleSweep;
use integration_testkit::{TestContext, t};

const WATERMARK: &str = "2026-01-02 00:00:00.000000";
const PRE_WATERMARK: &str = "2026-01-01 00:00:00.000000";

#[tokio::test]
async fn drained_namespace_sweeps_unclaimed_rows_once() {
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

    let (sweep, store) = build_sweep(&clickhouse);

    sweep
        .run_for_drained(&[])
        .await
        .expect("no drained namespaces must not sweep");
    assert!(
        file_is_active(&clickhouse, project_id, 111).await,
        "sweep must not touch a namespace the backfill has not drained"
    );

    let drained = vec![traversal_path.to_string()];
    sweep.run_for_drained(&drained).await.expect("sweep failed");

    assert!(!file_is_active(&clickhouse, project_id, 111).await);
    assert!(file_is_active(&clickhouse, project_id, 222).await);
    assert_eq!(active_edge_count(&clickhouse, "gl_edge", 111).await, 0);
    assert_eq!(active_edge_count(&clickhouse, "gl_code_edge", 111).await, 0);
    assert!(
        store
            .load(&format!("maintenance.code_stale_sweep.{traversal_path}"))
            .await
            .expect("load marker")
            .is_some(),
        "the sweep must record its per-namespace maintenance checkpoint"
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
        .run_for_drained(&drained)
        .await
        .expect("marked sweep must be a no-op");
    assert!(
        file_is_active(&clickhouse, project_id, 333).await,
        "a swept namespace must not sweep again for the same schema version"
    );
}

#[tokio::test]
async fn sweep_scopes_to_the_drained_namespace() {
    let clickhouse = TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    for (path, project_id) in [("1/40/", 40i64), ("1/41/", 41i64)] {
        clickhouse
            .execute(&format!(
                "INSERT INTO {} (traversal_path, project_id, branch, last_task_id, indexed_at) \
                 VALUES ('{path}', {project_id}, 'main', 1, '{WATERMARK}')",
                t("code_indexing_checkpoint")
            ))
            .await;
        insert_file(
            &clickhouse,
            path,
            project_id,
            "main",
            project_id + 100,
            PRE_WATERMARK,
        )
        .await;
    }

    let (sweep, _) = build_sweep(&clickhouse);
    sweep
        .run_for_drained(&["1/40/".to_string()])
        .await
        .expect("sweep failed");

    assert!(
        !file_is_active(&clickhouse, 40, 140).await,
        "the drained namespace must be swept"
    );
    assert!(
        file_is_active(&clickhouse, 41, 141).await,
        "an undrained namespace must keep its rows even when a sibling sweeps"
    );
}

#[tokio::test]
async fn sweep_writes_no_tombstones_for_superseded_rows() {
    let traversal_path = "1/40/";
    let project_id: i64 = 40;

    let clickhouse = TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    clickhouse
        .execute(&format!(
            "INSERT INTO {} (traversal_path, project_id, branch, last_task_id, indexed_at) \
             VALUES ('{traversal_path}', {project_id}, 'main', 1, '{WATERMARK}')",
            t("code_indexing_checkpoint")
        ))
        .await;
    insert_file(
        &clickhouse,
        traversal_path,
        project_id,
        "main",
        444,
        PRE_WATERMARK,
    )
    .await;
    insert_file(
        &clickhouse,
        traversal_path,
        project_id,
        "main",
        444,
        WATERMARK,
    )
    .await;

    let (sweep, _) = build_sweep(&clickhouse);
    sweep
        .run_for_drained(&[traversal_path.to_string()])
        .await
        .expect("sweep failed");

    assert!(file_is_active(&clickhouse, project_id, 444).await);
    let rows = clickhouse
        .query(&format!(
            "SELECT id FROM {} WHERE id = 444 AND _deleted = true",
            t("gl_file")
        ))
        .await;
    assert_eq!(
        rows.first().map_or(0, |b| b.num_rows()),
        0,
        "a key with a live row at the watermark needs no tombstone; a raw-parts \
         scan would have written a no-op one per superseded row"
    );
}

fn build_sweep(clickhouse: &TestContext) -> (CodeStaleSweep, Arc<ClickHouseCheckpointStore>) {
    let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
    let table_names = CodeTableNames::from_ontology(&ontology).expect("code tables must resolve");
    let store = Arc::new(ClickHouseCheckpointStore::new(Arc::new(
        clickhouse.config.build_client(),
    )));
    (
        CodeStaleSweep::new(
            clickhouse.config.build_client(),
            &table_names,
            store.clone(),
        ),
        store,
    )
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
