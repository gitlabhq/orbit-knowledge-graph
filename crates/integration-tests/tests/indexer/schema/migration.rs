use std::sync::Arc;

use clickhouse_client::FromArrowColumn;
use indexer::locking::LockService;
use indexer::metrics::MigrationMetrics;
use indexer::schema::migration;
use indexer::schema::version::{
    SCHEMA_VERSION, ensure_version_table, read_active_version, table_prefix, write_schema_version,
};
use indexer::testkit::MockLockService;
use integration_testkit::{TestContext, t};
use query_engine::compiler::generate_graph_tables_with_prefix;

async fn setup() -> (TestContext, ontology::Ontology, MigrationMetrics) {
    let ctx = TestContext::new(&[]).await;
    let ontology = ontology::Ontology::load_embedded().unwrap();
    let metrics = MigrationMetrics::new();

    // Mirror indexer startup: version table must exist before migration runs.
    ensure_version_table(&ctx.create_client()).await.unwrap();

    (ctx, ontology, metrics)
}

fn lock() -> Arc<dyn LockService> {
    Arc::new(MockLockService::new())
}

#[tokio::test]
async fn fresh_install_creates_tables_and_records_version() {
    let (ctx, ontology, metrics) = setup().await;
    let client = ctx.create_client();

    migration::run_if_needed(&client, &lock(), &ontology, &metrics)
        .await
        .unwrap();

    assert_eq!(
        read_active_version(&client).await.unwrap(),
        Some(*SCHEMA_VERSION)
    );

    // Fresh install creates all ontology-driven tables.
    let prefix = table_prefix(*SCHEMA_VERSION);
    let expected_tables = generate_graph_tables_with_prefix(&ontology, &prefix);

    let result = ctx
        .query(
            "SELECT toInt64(count()) AS cnt FROM system.tables \
             WHERE database = 'test' AND name != 'gkg_schema_version'",
        )
        .await;
    let count = i64::extract_column(&result, 0).unwrap();
    assert_eq!(
        count,
        vec![expected_tables.len() as i64],
        "fresh install should create all ontology tables"
    );
}

#[tokio::test]
async fn matching_version_is_noop() {
    let (ctx, ontology, metrics) = setup().await;
    let client = ctx.create_client();
    write_schema_version(&client, *SCHEMA_VERSION)
        .await
        .unwrap();

    migration::run_if_needed(&client, &lock(), &ontology, &metrics)
        .await
        .unwrap();

    assert_eq!(
        read_active_version(&client).await.unwrap(),
        Some(*SCHEMA_VERSION)
    );
}

#[tokio::test]
async fn mismatch_creates_all_ontology_tables_and_marks_migrating() {
    let (ctx, ontology, metrics) = setup().await;
    let client = ctx.create_client();
    write_schema_version(&client, 99).await.unwrap();

    migration::run_if_needed(&client, &lock(), &ontology, &metrics)
        .await
        .unwrap();

    // Count tables created (excluding the version control table).
    let prefix = table_prefix(*SCHEMA_VERSION);
    let expected_tables = generate_graph_tables_with_prefix(&ontology, &prefix);

    let result = ctx
        .query(
            "SELECT name FROM system.tables \
             WHERE database = 'test' AND name != 'gkg_schema_version' \
             ORDER BY name",
        )
        .await;
    let created_names = String::extract_column(&result, 0).unwrap();

    assert_eq!(
        created_names.len(),
        expected_tables.len(),
        "expected {} tables from ontology, got {}: {created_names:?}",
        expected_tables.len(),
        created_names.len(),
    );

    for table in &expected_tables {
        assert!(
            created_names.contains(&table.name),
            "missing table '{}' — created: {created_names:?}",
            table.name
        );
    }

    // Version row should be 'migrating', not 'active'.
    let result = ctx
        .query(&format!(
            "SELECT CAST(status AS String) AS status \
             FROM gkg_schema_version FINAL WHERE version = {}",
            *SCHEMA_VERSION
        ))
        .await;
    let statuses = String::extract_column(&result, 0).unwrap();
    assert_eq!(statuses, vec!["migrating"]);
}

#[tokio::test]
async fn created_tables_have_correct_columns() {
    let (ctx, ontology, metrics) = setup().await;
    let client = ctx.create_client();
    write_schema_version(&client, 99).await.unwrap();

    migration::run_if_needed(&client, &lock(), &ontology, &metrics)
        .await
        .unwrap();

    // Spot-check a node table, an edge table, and an auxiliary table.
    for (table, expected_col) in [
        (t("gl_user"), "username"),
        (t("gl_edge"), "relationship_kind"),
        (t("checkpoint"), "watermark"),
    ] {
        let result = ctx
            .query(&format!(
                "SELECT name FROM system.columns WHERE database = 'test' AND table = '{table}'"
            ))
            .await;
        let columns = String::extract_column(&result, 0).unwrap();
        assert!(
            columns.contains(&expected_col.to_string()),
            "table '{table}' missing column '{expected_col}' — has: {columns:?}"
        );
    }
}

#[tokio::test]
async fn idempotent_rerun_succeeds() {
    let (ctx, ontology, metrics) = setup().await;
    let client = ctx.create_client();
    write_schema_version(&client, 99).await.unwrap();

    let lock_svc: Arc<dyn LockService> = Arc::new(MockLockService::new());

    migration::run_if_needed(&client, &lock_svc, &ontology, &metrics)
        .await
        .unwrap();

    // Lock is released after success, so second run can acquire it.
    // It will re-run CREATE TABLE IF NOT EXISTS (idempotent).
    migration::run_if_needed(&client, &lock_svc, &ontology, &metrics)
        .await
        .unwrap();
}

#[tokio::test]
async fn lock_released_after_migration() {
    let (ctx, ontology, metrics) = setup().await;
    let client = ctx.create_client();
    write_schema_version(&client, 99).await.unwrap();

    let mock = Arc::new(MockLockService::new());
    let lock_svc: Arc<dyn LockService> = mock.clone();

    migration::run_if_needed(&client, &lock_svc, &ontology, &metrics)
        .await
        .unwrap();

    assert!(!mock.is_held("schema_migration"), "lock should be released");
}

#[tokio::test]
async fn held_lock_causes_timeout() {
    let (ctx, ontology, metrics) = setup().await;
    let client = ctx.create_client();
    write_schema_version(&client, 99).await.unwrap();

    let mock = MockLockService::new();
    mock.set_lock("schema_migration");
    let lock_svc: Arc<dyn LockService> = Arc::new(mock);

    // Migration polls every 5s × 60 iterations. Use paused time to skip the wait.
    tokio::time::pause();

    let result = migration::run_if_needed(&client, &lock_svc, &ontology, &metrics).await;

    assert!(result.is_err());
    assert!(
        result.unwrap_err().to_string().contains("lock held"),
        "error should mention lock timeout"
    );
}
