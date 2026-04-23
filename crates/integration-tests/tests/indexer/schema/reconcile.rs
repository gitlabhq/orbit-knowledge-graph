use std::sync::Arc;

use clickhouse_client::FromArrowColumn;
use indexer::locking::LockService;
use indexer::metrics::MigrationMetrics;
use indexer::schema::version::{
    SCHEMA_VERSION, ensure_version_table, table_prefix, write_schema_version,
};
use indexer::schema::{migration, reconcile};
use indexer::testkit::MockLockService;
use integration_testkit::TestContext;

async fn setup() -> (TestContext, ontology::Ontology) {
    let ctx = TestContext::new(&[]).await;
    let ontology = ontology::Ontology::load_embedded().unwrap();
    ensure_version_table(&ctx.create_client()).await.unwrap();

    // Run a fresh migration to create all tables.
    let metrics = MigrationMetrics::new();
    let lock: Arc<dyn LockService> = Arc::new(MockLockService::new());
    migration::run_if_needed(&ctx.create_client(), &lock, &ontology, &metrics)
        .await
        .unwrap();

    (ctx, ontology)
}

fn prefixed(table: &str) -> String {
    format!("{}{table}", table_prefix(*SCHEMA_VERSION))
}

async fn count_indexes(ctx: &TestContext, table: &str) -> Vec<String> {
    let result = ctx
        .query(&format!(
            "SELECT name FROM system.data_skipping_indices WHERE table = '{table}'"
        ))
        .await;
    String::extract_column(&result, 0).unwrap_or_default()
}

async fn count_projections(ctx: &TestContext, table: &str) -> Vec<String> {
    let result = ctx
        .query(&format!(
            "SELECT name FROM system.projections WHERE table = '{table}'"
        ))
        .await;
    String::extract_column(&result, 0).unwrap_or_default()
}

#[tokio::test]
async fn reconcile_is_noop_when_schema_matches() {
    let (ctx, ontology) = setup().await;
    let client = ctx.create_client();

    // Tables were just created from the same ontology — reconcile should
    // find nothing to change.
    reconcile::reconcile(&client, &ontology).await.unwrap();

    // Verify indexes still exist (not accidentally dropped).
    let edge_table = prefixed("gl_edge");
    let indexes = count_indexes(&ctx, &edge_table).await;
    assert!(
        indexes.contains(&"idx_relationship".to_string()),
        "idx_relationship should still exist: {indexes:?}"
    );
}

#[tokio::test]
async fn reconcile_adds_missing_index() {
    let (ctx, ontology) = setup().await;
    let client = ctx.create_client();
    let edge_table = prefixed("gl_edge");

    // Drop an index manually to simulate drift.
    ctx.execute(&format!(
        "ALTER TABLE {edge_table} DROP INDEX idx_relationship"
    ))
    .await;

    let before = count_indexes(&ctx, &edge_table).await;
    assert!(
        !before.contains(&"idx_relationship".to_string()),
        "idx_relationship should be gone after manual drop"
    );

    // Reconcile should re-add it.
    reconcile::reconcile(&client, &ontology).await.unwrap();

    let after = count_indexes(&ctx, &edge_table).await;
    assert!(
        after.contains(&"idx_relationship".to_string()),
        "reconcile should have re-added idx_relationship: {after:?}"
    );
}

#[tokio::test]
async fn reconcile_drops_extra_index() {
    let (ctx, ontology) = setup().await;
    let client = ctx.create_client();
    let edge_table = prefixed("gl_edge");

    // Add an index that the ontology doesn't declare.
    ctx.execute(&format!(
        "ALTER TABLE {edge_table} ADD INDEX idx_extra source_id TYPE minmax GRANULARITY 1"
    ))
    .await;

    let before = count_indexes(&ctx, &edge_table).await;
    assert!(
        before.contains(&"idx_extra".to_string()),
        "idx_extra should exist after manual add"
    );

    // Reconcile should drop it.
    reconcile::reconcile(&client, &ontology).await.unwrap();

    let after = count_indexes(&ctx, &edge_table).await;
    assert!(
        !after.contains(&"idx_extra".to_string()),
        "reconcile should have dropped idx_extra: {after:?}"
    );
}

#[tokio::test]
async fn reconcile_adds_missing_projection() {
    let (ctx, ontology) = setup().await;
    let client = ctx.create_client();
    let edge_table = prefixed("gl_edge");

    // Drop a projection manually.
    ctx.execute(&format!(
        "ALTER TABLE {edge_table} DROP PROJECTION by_source"
    ))
    .await;

    let before = count_projections(&ctx, &edge_table).await;
    assert!(
        !before.contains(&"by_source".to_string()),
        "by_source should be gone"
    );

    reconcile::reconcile(&client, &ontology).await.unwrap();

    let after = count_projections(&ctx, &edge_table).await;
    assert!(
        after.contains(&"by_source".to_string()),
        "reconcile should have re-added by_source: {after:?}"
    );
}

#[tokio::test]
async fn reconcile_drops_extra_projection() {
    let (ctx, ontology) = setup().await;
    let client = ctx.create_client();
    let edge_table = prefixed("gl_edge");

    ctx.execute(&format!(
        "ALTER TABLE {edge_table} ADD PROJECTION extra_proj (SELECT * ORDER BY source_id)"
    ))
    .await;

    let before = count_projections(&ctx, &edge_table).await;
    assert!(
        before.contains(&"extra_proj".to_string()),
        "extra_proj should exist"
    );

    reconcile::reconcile(&client, &ontology).await.unwrap();

    let after = count_projections(&ctx, &edge_table).await;
    assert!(
        !after.contains(&"extra_proj".to_string()),
        "reconcile should have dropped extra_proj: {after:?}"
    );
}

#[tokio::test]
async fn reconcile_idempotent_on_second_run() {
    let (ctx, ontology) = setup().await;
    let client = ctx.create_client();
    let edge_table = prefixed("gl_edge");

    // Drop something, reconcile twice — second run should be a no-op.
    ctx.execute(&format!(
        "ALTER TABLE {edge_table} DROP INDEX idx_relationship"
    ))
    .await;

    reconcile::reconcile(&client, &ontology).await.unwrap();
    reconcile::reconcile(&client, &ontology).await.unwrap();

    let indexes = count_indexes(&ctx, &edge_table).await;
    assert!(
        indexes.contains(&"idx_relationship".to_string()),
        "idx_relationship should exist after two reconcile runs"
    );
}
