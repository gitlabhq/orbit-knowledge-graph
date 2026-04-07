use async_trait::async_trait;
use migration_framework::{
    Migration, MigrationContext, MigrationLedger, MigrationStatus, MigrationType,
};

use crate::common::TestContext;

struct TestMigration;

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
