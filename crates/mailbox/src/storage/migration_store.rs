//! Schema migration tracking in ClickHouse.

use std::sync::Arc;

use chrono::Utc;
use clickhouse_client::ArrowClickHouseClient;

use crate::error::MailboxError;

#[derive(Debug, Clone)]
pub struct AppliedMigration {
    pub plugin_id: String,
    pub schema_version: i64,
    pub node_kind: String,
    pub table_name: String,
    pub ddl_hash: String,
    pub applied_at: chrono::DateTime<Utc>,
}

pub struct MigrationStore {
    client: Arc<ArrowClickHouseClient>,
}

impl MigrationStore {
    pub fn new(client: Arc<ArrowClickHouseClient>) -> Self {
        Self { client }
    }

    pub async fn execute_ddl(&self, ddl: &str) -> Result<(), MailboxError> {
        self.client
            .execute(ddl)
            .await
            .map_err(|e| MailboxError::storage(format!("failed to execute DDL: {}", e)))
    }

    pub async fn record_migration(
        &self,
        plugin_id: &str,
        schema_version: i64,
        node_kind: &str,
        table_name: &str,
        ddl_hash: &str,
    ) -> Result<(), MailboxError> {
        let applied_at = Utc::now().format("%Y-%m-%d %H:%M:%S%.6f").to_string();

        let sql = format!(
            r#"INSERT INTO {} (plugin_id, schema_version, node_kind, table_name, ddl_hash, applied_at)
            VALUES ('{}', {}, '{}', '{}', '{}', '{}')"#,
            super::MIGRATIONS_TABLE,
            escape_string(plugin_id),
            schema_version,
            escape_string(node_kind),
            escape_string(table_name),
            escape_string(ddl_hash),
            applied_at,
        );

        self.client
            .execute(&sql)
            .await
            .map_err(|e| MailboxError::storage(format!("failed to record migration: {}", e)))?;

        Ok(())
    }

    pub async fn get_latest_version(&self, plugin_id: &str) -> Result<Option<i64>, MailboxError> {
        let sql = format!(
            r#"SELECT max(schema_version) as version
            FROM {} FINAL
            WHERE plugin_id = '{}'"#,
            super::MIGRATIONS_TABLE,
            escape_string(plugin_id),
        );

        let batches = self
            .client
            .query_arrow(&sql)
            .await
            .map_err(|e| MailboxError::storage(format!("failed to query migrations: {}", e)))?;

        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok(None);
        }

        use arrow::array::{Array, Int64Array};
        let version = batches[0]
            .column_by_name("version")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .filter(|a| !a.is_null(0))
            .map(|a| a.value(0));

        Ok(version)
    }

    pub async fn get_ddl_hash(
        &self,
        plugin_id: &str,
        node_kind: &str,
    ) -> Result<Option<String>, MailboxError> {
        let sql = format!(
            r#"SELECT ddl_hash
            FROM {} FINAL
            WHERE plugin_id = '{}' AND node_kind = '{}'
            ORDER BY schema_version DESC
            LIMIT 1"#,
            super::MIGRATIONS_TABLE,
            escape_string(plugin_id),
            escape_string(node_kind),
        );

        let batches = self
            .client
            .query_arrow(&sql)
            .await
            .map_err(|e| MailboxError::storage(format!("failed to query ddl_hash: {}", e)))?;

        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok(None);
        }

        use arrow::array::{Array, StringArray};
        let hash = batches[0]
            .column_by_name("ddl_hash")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .filter(|a| !a.is_null(0))
            .map(|a| a.value(0).to_string());

        Ok(hash)
    }

    pub async fn list_by_plugin(
        &self,
        plugin_id: &str,
    ) -> Result<Vec<AppliedMigration>, MailboxError> {
        let sql = format!(
            r#"SELECT plugin_id, schema_version, node_kind, table_name, ddl_hash, applied_at
            FROM {} FINAL
            WHERE plugin_id = '{}'
            ORDER BY schema_version, node_kind"#,
            super::MIGRATIONS_TABLE,
            escape_string(plugin_id),
        );

        let batches = self
            .client
            .query_arrow(&sql)
            .await
            .map_err(|e| MailboxError::storage(format!("failed to list migrations: {}", e)))?;

        let mut migrations = Vec::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                let migration = parse_migration_from_batch(&batch, row)?;
                migrations.push(migration);
            }
        }

        Ok(migrations)
    }
}

fn parse_migration_from_batch(
    batch: &arrow::array::RecordBatch,
    row: usize,
) -> Result<AppliedMigration, MailboxError> {
    use arrow::array::{Array, Int64Array, StringArray, TimestampMicrosecondArray};

    let plugin_id = batch
        .column_by_name("plugin_id")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .and_then(|a| a.value(row).into())
        .ok_or_else(|| MailboxError::storage("missing plugin_id column"))?;

    let schema_version = batch
        .column_by_name("schema_version")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .map(|a| a.value(row))
        .ok_or_else(|| MailboxError::storage("missing schema_version column"))?;

    let node_kind = batch
        .column_by_name("node_kind")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .and_then(|a| a.value(row).into())
        .ok_or_else(|| MailboxError::storage("missing node_kind column"))?;

    let table_name = batch
        .column_by_name("table_name")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .and_then(|a| a.value(row).into())
        .ok_or_else(|| MailboxError::storage("missing table_name column"))?;

    let ddl_hash = batch
        .column_by_name("ddl_hash")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .and_then(|a| a.value(row).into())
        .ok_or_else(|| MailboxError::storage("missing ddl_hash column"))?;

    let applied_at_micros = batch
        .column_by_name("applied_at")
        .and_then(|c| c.as_any().downcast_ref::<TimestampMicrosecondArray>())
        .map(|a| a.value(row))
        .ok_or_else(|| MailboxError::storage("missing applied_at column"))?;

    let applied_at =
        chrono::DateTime::from_timestamp_micros(applied_at_micros).unwrap_or_else(Utc::now);

    Ok(AppliedMigration {
        plugin_id: plugin_id.to_string(),
        schema_version,
        node_kind: node_kind.to_string(),
        table_name: table_name.to_string(),
        ddl_hash: ddl_hash.to_string(),
        applied_at,
    })
}

fn escape_string(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}
