use std::sync::Arc;

use clickhouse_client::ClickHouseConfigurationExt;
use indexer::checkpoint::{CheckpointStore, ClickHouseCheckpointStore};
use indexer::modules::code::config::CodeTableNames;
use indexer::orchestrator::scheduled::code_stale_sweep::request_sweeps;
use indexer::orchestrator::scheduled::{CodeStaleSweep, ScheduledTask, ScheduledTaskMetrics};
use integration_testkit::{TestContext, t};
use orbit_server_config::AppConfig;
use orbit_utils::traversal_path::TraversalPath;

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

    let (sweep, store) = build_sweep(&clickhouse, 10);

    sweep
        .run()
        .await
        .expect("a run without requests must succeed");
    assert!(
        file_is_active(&clickhouse, project_id, 111).await,
        "sweep must not touch a namespace the backfill has not drained"
    );

    let drained = vec![TraversalPath::new_unchecked(traversal_path)];
    assert_eq!(
        request_sweeps(store.as_ref(), &drained)
            .await
            .expect("request failed"),
        1
    );
    flush_requests(&clickhouse).await;
    let gate_key = format!("maintenance.code_stale_sweep.{traversal_path}");
    let request = store
        .load(&gate_key)
        .await
        .expect("load gate")
        .expect("request must be recorded");
    assert!(
        request.cursor_values.is_some(),
        "a requested sweep must be recorded as an in-progress gate"
    );

    sweep.run().await.expect("sweep failed");

    assert!(!file_is_active(&clickhouse, project_id, 111).await);
    assert!(file_is_active(&clickhouse, project_id, 222).await);
    assert_eq!(active_edge_count(&clickhouse, "gl_edge", 111).await, 0);
    assert_eq!(active_edge_count(&clickhouse, "gl_code_edge", 111).await, 0);
    let gate = store
        .load(&gate_key)
        .await
        .expect("load gate")
        .expect("gate must survive the sweep");
    assert!(
        gate.cursor_values.is_none(),
        "the sweep must complete its per-namespace maintenance checkpoint"
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
    assert_eq!(
        request_sweeps(store.as_ref(), &drained)
            .await
            .expect("request failed"),
        0,
        "a completed gate must not be requested again"
    );
    sweep.run().await.expect("run without requests failed");
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

    let (sweep, store) = build_sweep(&clickhouse, 10);
    request_and_run(&clickhouse, &sweep, &store, &["1/40/"]).await;

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
async fn sweep_honours_the_per_run_cap_and_finishes_on_the_next_run() {
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

    let (paused, store) = build_sweep(&clickhouse, 0);
    request_and_run(&clickhouse, &paused, &store, &["1/40/", "1/41/"]).await;
    assert!(
        file_is_active(&clickhouse, 40, 140).await && file_is_active(&clickhouse, 41, 141).await,
        "a zero cap must pause sweeping while the requests wait"
    );

    let (sweep, store) = build_sweep(&clickhouse, 1);
    request_and_run(&clickhouse, &sweep, &store, &["1/40/", "1/41/"]).await;

    let swept_after_first_run = [
        !file_is_active(&clickhouse, 40, 140).await,
        !file_is_active(&clickhouse, 41, 141).await,
    ]
    .into_iter()
    .filter(|swept| *swept)
    .count();
    assert_eq!(
        swept_after_first_run, 1,
        "a run must sweep no more namespaces than its cap"
    );

    sweep.run().await.expect("second run failed");
    assert!(!file_is_active(&clickhouse, 40, 140).await);
    assert!(!file_is_active(&clickhouse, 41, 141).await);
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

    let (sweep, store) = build_sweep(&clickhouse, 10);
    request_and_run(&clickhouse, &sweep, &store, &[traversal_path]).await;

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

fn build_sweep(
    clickhouse: &TestContext,
    max_namespaces_per_run: usize,
) -> (CodeStaleSweep, Arc<ClickHouseCheckpointStore>) {
    let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
    let table_names = CodeTableNames::from_ontology(&ontology).expect("code tables must resolve");
    let store = Arc::new(ClickHouseCheckpointStore::new(Arc::new(
        clickhouse.config.build_client(),
    )));
    let mut config = AppConfig::embedded_defaults()
        .schedule
        .tasks
        .code_stale_sweep;
    config.max_namespaces_per_run = max_namespaces_per_run;
    (
        CodeStaleSweep::new(
            clickhouse.config.build_client(),
            &table_names,
            store.clone(),
            ScheduledTaskMetrics::new(),
            config,
        ),
        store,
    )
}

async fn request_and_run(
    clickhouse: &TestContext,
    sweep: &CodeStaleSweep,
    store: &Arc<ClickHouseCheckpointStore>,
    drained: &[&str],
) {
    let drained: Vec<TraversalPath> = drained
        .iter()
        .map(|path| TraversalPath::new_unchecked(*path))
        .collect();
    request_sweeps(store.as_ref(), &drained)
        .await
        .expect("request failed");
    flush_requests(clickhouse).await;
    sweep.run().await.expect("sweep failed");
}

// Sweep requests are fire-and-forget async inserts; production picks them up a
// flush later on its next tick, a test forces the flush.
async fn flush_requests(clickhouse: &TestContext) {
    clickhouse.execute("SYSTEM FLUSH ASYNC INSERT QUEUE").await;
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
