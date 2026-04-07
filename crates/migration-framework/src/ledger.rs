use anyhow::Result;
use arrow::array::{Array, StringArray, TimestampMicrosecondArray, UInt32Array, UInt64Array};
use chrono::{DateTime, Utc};
use clickhouse_client::ArrowClickHouseClient;
use gkg_utils::arrow::ArrowUtils;
use thiserror::Error;

use crate::types::{Migration, MigrationStatus, MigrationType};

pub const GKG_MIGRATIONS_TABLE: &str = "gkg_migrations";
const GKG_MIGRATIONS_TABLE_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS gkg_migrations (
    version UInt64,
    name String,
    migration_type LowCardinality(String),
    status LowCardinality(String),
    started_at Nullable(DateTime64(6, 'UTC')),
    completed_at Nullable(DateTime64(6, 'UTC')),
    error_message Nullable(String),
    retry_count UInt32 DEFAULT 0,
    _version DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (version)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1
"#;

#[derive(Debug, Error)]
pub enum MigrationLedgerError {
    #[error("clickhouse operation failed: {0}")]
    ClickHouse(#[from] clickhouse_client::ClickHouseError),
    #[error("failed to decode ledger row: {0}")]
    Decode(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LedgerMigrationRecord {
    pub version: u64,
    pub name: String,
    pub migration_type: MigrationType,
    pub status: MigrationStatus,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error_message: Option<String>,
    pub retry_count: u32,
}

pub struct MigrationLedger {
    client: ArrowClickHouseClient,
}

impl MigrationLedger {
    pub fn new(client: ArrowClickHouseClient) -> Self {
        Self { client }
    }

    pub async fn ensure_table(&self) -> Result<(), MigrationLedgerError> {
        self.client.execute(GKG_MIGRATIONS_TABLE_DDL).await?;
        Ok(())
    }

    pub async fn mark_pending(
        &self,
        migration: &dyn Migration,
    ) -> Result<(), MigrationLedgerError> {
        self.write_status(migration, MigrationStatus::Pending, None, 0)
            .await
    }

    pub async fn mark_preparing(
        &self,
        migration: &dyn Migration,
        retry_count: u32,
    ) -> Result<(), MigrationLedgerError> {
        self.write_status(migration, MigrationStatus::Preparing, None, retry_count)
            .await
    }

    pub async fn mark_completed(
        &self,
        migration: &dyn Migration,
        retry_count: u32,
    ) -> Result<(), MigrationLedgerError> {
        self.write_status(migration, MigrationStatus::Completed, None, retry_count)
            .await
    }

    pub async fn mark_failed(
        &self,
        migration: &dyn Migration,
        error_message: &str,
        retry_count: u32,
    ) -> Result<(), MigrationLedgerError> {
        self.write_status(
            migration,
            MigrationStatus::Failed,
            Some(error_message),
            retry_count,
        )
        .await
    }

    pub async fn list(&self) -> Result<Vec<LedgerMigrationRecord>, MigrationLedgerError> {
        let sql = "SELECT version, name, migration_type, status, started_at, completed_at, error_message, retry_count FROM gkg_migrations FINAL ORDER BY version";
        let batches = self.client.query_arrow(sql).await?;
        let mut records = Vec::new();

        for batch in batches {
            let versions = ArrowUtils::get_column_by_name::<UInt64Array>(&batch, "version")
                .ok_or_else(|| {
                    MigrationLedgerError::Decode("missing version column".to_string())
                })?;
            let names = ArrowUtils::get_column_by_name::<StringArray>(&batch, "name")
                .ok_or_else(|| MigrationLedgerError::Decode("missing name column".to_string()))?;
            let migration_types =
                ArrowUtils::get_column_by_name::<StringArray>(&batch, "migration_type")
                    .ok_or_else(|| {
                        MigrationLedgerError::Decode("missing migration_type column".to_string())
                    })?;
            let statuses = ArrowUtils::get_column_by_name::<StringArray>(&batch, "status")
                .ok_or_else(|| MigrationLedgerError::Decode("missing status column".to_string()))?;
            let retry_counts = ArrowUtils::get_column_by_name::<UInt32Array>(&batch, "retry_count")
                .ok_or_else(|| {
                    MigrationLedgerError::Decode("missing retry_count column".to_string())
                })?;
            let started_ats =
                ArrowUtils::get_column_by_name::<TimestampMicrosecondArray>(&batch, "started_at")
                    .ok_or_else(|| {
                    MigrationLedgerError::Decode("missing started_at column".to_string())
                })?;
            let completed_ats =
                ArrowUtils::get_column_by_name::<TimestampMicrosecondArray>(&batch, "completed_at")
                    .ok_or_else(|| {
                        MigrationLedgerError::Decode("missing completed_at column".to_string())
                    })?;
            let error_messages =
                ArrowUtils::get_column_by_name::<StringArray>(&batch, "error_message").ok_or_else(
                    || MigrationLedgerError::Decode("missing error_message column".to_string()),
                )?;

            for row in 0..batch.num_rows() {
                records.push(LedgerMigrationRecord {
                    version: versions.value(row),
                    name: names.value(row).to_string(),
                    migration_type: parse_migration_type(migration_types.value(row))?,
                    status: parse_status(statuses.value(row))?,
                    started_at: timestamp_value(started_ats, row),
                    completed_at: timestamp_value(completed_ats, row),
                    error_message: if error_messages.is_null(row) {
                        None
                    } else {
                        Some(error_messages.value(row).to_string())
                    },
                    retry_count: retry_counts.value(row),
                });
            }
        }

        Ok(records)
    }

    async fn write_status(
        &self,
        migration: &dyn Migration,
        status: MigrationStatus,
        error_message: Option<&str>,
        retry_count: u32,
    ) -> Result<(), MigrationLedgerError> {
        let started_at = match status {
            MigrationStatus::Pending => "NULL",
            _ => "now64(6)",
        };
        let completed_at = match status {
            MigrationStatus::Completed | MigrationStatus::Failed => "now64(6)",
            _ => "NULL",
        };
        let error_sql = error_message
            .map(|message| format!("'{}'", escape_string(message)))
            .unwrap_or_else(|| "NULL".to_string());

        let sql = format!(
            "INSERT INTO {table} (version, name, migration_type, status, started_at, completed_at, error_message, retry_count) VALUES ({version}, '{name}', '{migration_type}', '{status}', {started_at}, {completed_at}, {error_sql}, {retry_count})",
            table = GKG_MIGRATIONS_TABLE,
            version = migration.version(),
            name = escape_string(migration.name()),
            migration_type = migration.migration_type().as_str(),
            status = status.as_str(),
            started_at = started_at,
            completed_at = completed_at,
            error_sql = error_sql,
            retry_count = retry_count,
        );

        self.client.execute(&sql).await?;
        Ok(())
    }
}

fn timestamp_value(array: &TimestampMicrosecondArray, row: usize) -> Option<DateTime<Utc>> {
    if array.is_null(row) {
        return None;
    }

    DateTime::from_timestamp_micros(array.value(row))
}

fn escape_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

fn parse_migration_type(value: &str) -> Result<MigrationType, MigrationLedgerError> {
    match value {
        "additive" => Ok(MigrationType::Additive),
        "convergent" => Ok(MigrationType::Convergent),
        "finalization" => Ok(MigrationType::Finalization),
        other => Err(MigrationLedgerError::Decode(format!(
            "unknown migration type: {other}"
        ))),
    }
}

fn parse_status(value: &str) -> Result<MigrationStatus, MigrationLedgerError> {
    match value {
        "pending" => Ok(MigrationStatus::Pending),
        "preparing" => Ok(MigrationStatus::Preparing),
        "completed" => Ok(MigrationStatus::Completed),
        "failed" => Ok(MigrationStatus::Failed),
        other => Err(MigrationLedgerError::Decode(format!(
            "unknown migration status: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use async_trait::async_trait;

    use crate::{Migration, MigrationContext, MigrationLedger, MigrationStatus, MigrationType};

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

        async fn prepare(&self, _ctx: &MigrationContext) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn migration_type_string_values_match_contract() {
        assert_eq!(MigrationType::Additive.as_str(), "additive");
        assert_eq!(MigrationType::Convergent.as_str(), "convergent");
        assert_eq!(MigrationType::Finalization.as_str(), "finalization");
    }

    #[test]
    fn migration_status_string_values_match_contract() {
        assert_eq!(MigrationStatus::Pending.as_str(), "pending");
        assert_eq!(MigrationStatus::Preparing.as_str(), "preparing");
        assert_eq!(MigrationStatus::Completed.as_str(), "completed");
        assert_eq!(MigrationStatus::Failed.as_str(), "failed");
    }

    #[tokio::test]
    async fn ledger_round_trips_latest_status() {
        let ctx = integration_testkit::TestContext::new(&[]).await;
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
        let ctx = integration_testkit::TestContext::new(&[]).await;
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
}
