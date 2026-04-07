use async_trait::async_trait;
use migration_framework::{
    Migration, MigrationContext, MigrationLedger, MigrationRegistry, MigrationStatus, MigrationType,
};

use crate::common::TestContext;

struct TestMigration;

struct NamedTestMigration {
    version: u64,
    name: &'static str,
}

#[async_trait]
impl Migration for TestMigration {
    fn version(&self) -> u64 {
        1
    }

    fn name(&self) -> &str {
        "test_migration"
    }

    fn migration_type(&self) -> MigrationType {
        MigrationType::Additive
    }

    async fn prepare(&self, _ctx: &MigrationContext) -> std::result::Result<(), anyhow::Error> {
        Ok(())
    }
}

#[async_trait]
impl Migration for NamedTestMigration {
    fn version(&self) -> u64 {
        self.version
    }

    fn name(&self) -> &str {
        self.name
    }

    fn migration_type(&self) -> MigrationType {
        MigrationType::Additive
    }

    async fn prepare(&self, ctx: &MigrationContext) -> std::result::Result<(), anyhow::Error> {
        ctx.execute_ddl(&format!(
            "CREATE TABLE IF NOT EXISTS {} (id UInt64) ENGINE = MergeTree ORDER BY id",
            self.name
        ))
        .await
    }
}

async fn run_registry(
    ctx: &TestContext,
    registry: &MigrationRegistry,
) -> Vec<migration_framework::LedgerMigrationRecord> {
    let ledger = MigrationLedger::new(ctx.create_client());
    let migration_ctx = MigrationContext::new(ctx.create_client());
    ledger.ensure_table().await.expect("table");

    for migration in registry.migrations() {
        let retry_count = ledger
            .list()
            .await
            .expect("list rows")
            .into_iter()
            .find(|record| record.version == migration.version())
            .map_or(0, |record| record.retry_count + 1);

        ledger
            .mark_pending(migration.as_ref())
            .await
            .expect("pending");
        ledger
            .mark_preparing(migration.as_ref(), retry_count)
            .await
            .expect("preparing");

        match migration.prepare(&migration_ctx).await {
            Ok(()) => ledger
                .mark_completed(migration.as_ref(), retry_count)
                .await
                .expect("completed"),
            Err(error) => ledger
                .mark_failed(migration.as_ref(), &error.to_string(), retry_count)
                .await
                .expect("failed"),
        }
    }

    ledger.list().await.expect("list rows")
}

#[tokio::test]
async fn ensure_table_is_idempotent() {
    let ctx = TestContext::new(&[]).await;
    let ledger = MigrationLedger::new(ctx.create_client());

    ledger.ensure_table().await.expect("first create");
    ledger.ensure_table().await.expect("second create");
}

#[tokio::test]
async fn list_returns_empty_vec_for_empty_table() {
    let ctx = TestContext::new(&[]).await;
    let ledger = MigrationLedger::new(ctx.create_client());

    ledger.ensure_table().await.expect("table");
    let rows = ledger.list().await.expect("list rows");
    assert!(rows.is_empty());
}

#[tokio::test]
async fn ledger_round_trips_latest_status() {
    let ctx = TestContext::new(&[]).await;
    let ledger = MigrationLedger::new(ctx.create_client());
    let migration = TestMigration;

    ledger.ensure_table().await.expect("table");
    ledger.mark_pending(&migration).await.expect("pending");
    ledger
        .mark_preparing(&migration, 1)
        .await
        .expect("preparing");
    ledger
        .mark_completed(&migration, 1)
        .await
        .expect("completed");

    let rows = ledger.list().await.expect("list rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, MigrationStatus::Completed);
    assert_eq!(rows[0].retry_count, 1);
    assert!(rows[0].started_at.is_some());
    assert!(rows[0].completed_at.is_some());
}

#[tokio::test]
async fn ledger_records_failures() {
    let ctx = TestContext::new(&[]).await;
    let ledger = MigrationLedger::new(ctx.create_client());
    let migration = TestMigration;

    ledger.ensure_table().await.expect("table");
    ledger
        .mark_preparing(&migration, 2)
        .await
        .expect("preparing");
    ledger
        .mark_failed(&migration, "boom", 2)
        .await
        .expect("failed");

    let rows = ledger.list().await.expect("list rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, MigrationStatus::Failed);
    assert_eq!(rows[0].error_message.as_deref(), Some("boom"));
    assert_eq!(rows[0].retry_count, 2);
}

#[tokio::test]
async fn end_to_end_registry_run_applies_migration_and_updates_ledger() {
    let ctx = TestContext::new(&[]).await;
    let mut registry = MigrationRegistry::new();
    registry.register(Box::new(NamedTestMigration {
        version: 1,
        name: "test_registry_migration",
    }));

    let rows = run_registry(&ctx, &registry).await;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].version, 1);
    assert_eq!(rows[0].status, MigrationStatus::Completed);
    assert_eq!(rows[0].retry_count, 0);
    assert!(rows[0].started_at.is_some());
    assert!(rows[0].completed_at.is_some());

    let batches = ctx
        .query(
            "SELECT name FROM system.tables WHERE database = currentDatabase() AND name = 'test_registry_migration'",
        )
        .await;
    assert_eq!(batches[0].num_rows(), 1);
}

#[tokio::test]
async fn ledger_tracks_pending_preparing_completed_transition() {
    let ctx = TestContext::new(&[]).await;
    let ledger = MigrationLedger::new(ctx.create_client());
    let migration = TestMigration;

    ledger.ensure_table().await.expect("table");
    ledger.mark_pending(&migration).await.expect("pending");

    let rows = ledger.list().await.expect("list rows");
    assert_eq!(rows[0].status, MigrationStatus::Pending);
    assert!(rows[0].started_at.is_none());
    assert!(rows[0].completed_at.is_none());

    ledger
        .mark_preparing(&migration, 0)
        .await
        .expect("preparing");
    let rows = ledger.list().await.expect("list rows");
    assert_eq!(rows[0].status, MigrationStatus::Preparing);
    assert!(rows[0].started_at.is_some());
    assert!(rows[0].completed_at.is_none());

    ledger
        .mark_completed(&migration, 0)
        .await
        .expect("completed");
    let rows = ledger.list().await.expect("list rows");
    assert_eq!(rows[0].status, MigrationStatus::Completed);
    assert!(rows[0].started_at.is_some());
    assert!(rows[0].completed_at.is_some());
}

#[tokio::test]
async fn ledger_tracks_failed_transition_and_retry() {
    let ctx = TestContext::new(&[]).await;
    let ledger = MigrationLedger::new(ctx.create_client());
    let migration = TestMigration;

    ledger.ensure_table().await.expect("table");
    ledger.mark_pending(&migration).await.expect("pending");
    ledger
        .mark_preparing(&migration, 0)
        .await
        .expect("preparing");
    ledger
        .mark_failed(&migration, "boom", 0)
        .await
        .expect("failed");

    let rows = ledger.list().await.expect("list rows");
    assert_eq!(rows[0].status, MigrationStatus::Failed);
    assert_eq!(rows[0].error_message.as_deref(), Some("boom"));
    assert_eq!(rows[0].retry_count, 0);
    assert!(rows[0].completed_at.is_some());

    ledger
        .mark_pending(&migration)
        .await
        .expect("pending retry");
    ledger
        .mark_preparing(&migration, 1)
        .await
        .expect("retry preparing");
    ledger
        .mark_completed(&migration, 1)
        .await
        .expect("retry completed");

    let rows = ledger.list().await.expect("list rows");
    assert_eq!(rows[0].status, MigrationStatus::Completed);
    assert_eq!(rows[0].retry_count, 1);
    assert_eq!(rows[0].error_message, None);
}

#[test]
fn registry_rejects_non_monotonic_versions() {
    let result = std::panic::catch_unwind(|| {
        let mut registry = MigrationRegistry::new();
        registry.register(Box::new(NamedTestMigration {
            version: 2,
            name: "two",
        }));
        registry.register(Box::new(NamedTestMigration {
            version: 1,
            name: "one",
        }));
    });

    assert!(result.is_err());
}

#[test]
fn registry_starts_empty() {
    let registry = MigrationRegistry::new();
    assert!(registry.is_empty());
    assert!(registry.migrations().is_empty());
}
