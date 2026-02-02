//! Plugin persistence in ClickHouse.

use std::sync::Arc;

use chrono::Utc;
use clickhouse_client::ArrowClickHouseClient;

use crate::error::MailboxError;
use crate::types::{Plugin, PluginInfo, PluginSchema};

pub struct PluginStore {
    client: Arc<ArrowClickHouseClient>,
}

impl PluginStore {
    pub fn new(client: Arc<ArrowClickHouseClient>) -> Self {
        Self { client }
    }

    pub async fn create_tables(&self) -> Result<(), MailboxError> {
        self.client
            .execute(super::plugins_table_ddl())
            .await
            .map_err(|e| MailboxError::storage(format!("failed to create plugins table: {}", e)))?;

        self.client
            .execute(super::migrations_table_ddl())
            .await
            .map_err(|e| {
                MailboxError::storage(format!("failed to create migrations table: {}", e))
            })?;

        Ok(())
    }

    pub async fn insert(&self, plugin: &Plugin) -> Result<(), MailboxError> {
        let schema_json = serde_json::to_string(&plugin.schema)?;
        let created_at = plugin
            .created_at
            .format("%Y-%m-%d %H:%M:%S%.6f")
            .to_string();

        let sql = format!(
            r#"INSERT INTO {} (plugin_id, namespace_id, api_key_hash, schema, schema_version, created_at)
            VALUES ('{}', {}, '{}', '{}', {}, '{}')"#,
            super::PLUGINS_TABLE,
            escape_string(&plugin.plugin_id),
            plugin.namespace_id,
            escape_string(&plugin.api_key_hash),
            escape_string(&schema_json),
            plugin.schema_version,
            created_at,
        );

        self.client
            .execute(&sql)
            .await
            .map_err(|e| MailboxError::storage(format!("failed to insert plugin: {}", e)))?;

        Ok(())
    }

    pub async fn get(&self, plugin_id: &str) -> Result<Option<Plugin>, MailboxError> {
        let sql = format!(
            r#"SELECT plugin_id, namespace_id, api_key_hash, schema, schema_version, created_at
            FROM {} FINAL
            WHERE plugin_id = '{}' AND NOT _deleted"#,
            super::PLUGINS_TABLE,
            escape_string(plugin_id),
        );

        let batches = self
            .client
            .query_arrow(&sql)
            .await
            .map_err(|e| MailboxError::storage(format!("failed to query plugin: {}", e)))?;

        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok(None);
        }

        let batch = &batches[0];
        let plugin = parse_plugin_from_batch(batch, 0)?;
        Ok(Some(plugin))
    }

    pub async fn get_by_namespace(
        &self,
        namespace_id: i64,
        plugin_id: &str,
    ) -> Result<Option<Plugin>, MailboxError> {
        let sql = format!(
            r#"SELECT plugin_id, namespace_id, api_key_hash, schema, schema_version, created_at
            FROM {} FINAL
            WHERE namespace_id = {} AND plugin_id = '{}' AND NOT _deleted"#,
            super::PLUGINS_TABLE,
            namespace_id,
            escape_string(plugin_id),
        );

        let batches = self
            .client
            .query_arrow(&sql)
            .await
            .map_err(|e| MailboxError::storage(format!("failed to query plugin: {}", e)))?;

        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok(None);
        }

        let batch = &batches[0];
        let plugin = parse_plugin_from_batch(batch, 0)?;
        Ok(Some(plugin))
    }

    pub async fn list_by_namespace(
        &self,
        namespace_id: i64,
    ) -> Result<Vec<PluginInfo>, MailboxError> {
        let sql = format!(
            r#"SELECT plugin_id, namespace_id, api_key_hash, schema, schema_version, created_at
            FROM {} FINAL
            WHERE namespace_id = {} AND NOT _deleted
            ORDER BY plugin_id"#,
            super::PLUGINS_TABLE,
            namespace_id,
        );

        let batches = self
            .client
            .query_arrow(&sql)
            .await
            .map_err(|e| MailboxError::storage(format!("failed to list plugins: {}", e)))?;

        let mut plugins = Vec::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                let plugin = parse_plugin_from_batch(&batch, row)?;
                plugins.push(PluginInfo::from(plugin));
            }
        }

        Ok(plugins)
    }

    pub async fn exists(&self, namespace_id: i64, plugin_id: &str) -> Result<bool, MailboxError> {
        let sql = format!(
            r#"SELECT 1 FROM {} FINAL
            WHERE namespace_id = {} AND plugin_id = '{}' AND NOT _deleted
            LIMIT 1"#,
            super::PLUGINS_TABLE,
            namespace_id,
            escape_string(plugin_id),
        );

        let batches = self.client.query_arrow(&sql).await.map_err(|e| {
            MailboxError::storage(format!("failed to check plugin existence: {}", e))
        })?;

        Ok(!batches.is_empty() && batches[0].num_rows() > 0)
    }

    pub async fn update_schema(
        &self,
        plugin_id: &str,
        schema: &PluginSchema,
        new_version: i64,
    ) -> Result<(), MailboxError> {
        let existing = self.get(plugin_id).await?;
        let existing = existing.ok_or_else(|| MailboxError::PluginNotFound {
            plugin_id: plugin_id.to_string(),
        })?;

        let schema_json = serde_json::to_string(schema)?;
        let created_at = existing
            .created_at
            .format("%Y-%m-%d %H:%M:%S%.6f")
            .to_string();

        let sql = format!(
            r#"INSERT INTO {} (plugin_id, namespace_id, api_key_hash, schema, schema_version, created_at)
            VALUES ('{}', {}, '{}', '{}', {}, '{}')"#,
            super::PLUGINS_TABLE,
            escape_string(plugin_id),
            existing.namespace_id,
            escape_string(&existing.api_key_hash),
            escape_string(&schema_json),
            new_version,
            created_at,
        );

        self.client
            .execute(&sql)
            .await
            .map_err(|e| MailboxError::storage(format!("failed to update plugin schema: {}", e)))?;

        Ok(())
    }

    pub async fn delete(&self, plugin_id: &str) -> Result<(), MailboxError> {
        let existing = self.get(plugin_id).await?;
        let existing = existing.ok_or_else(|| MailboxError::PluginNotFound {
            plugin_id: plugin_id.to_string(),
        })?;

        let schema_json = serde_json::to_string(&existing.schema)?;
        let created_at = existing
            .created_at
            .format("%Y-%m-%d %H:%M:%S%.6f")
            .to_string();

        let sql = format!(
            r#"INSERT INTO {} (plugin_id, namespace_id, api_key_hash, schema, schema_version, created_at, _deleted)
            VALUES ('{}', {}, '{}', '{}', {}, '{}', true)"#,
            super::PLUGINS_TABLE,
            escape_string(plugin_id),
            existing.namespace_id,
            escape_string(&existing.api_key_hash),
            escape_string(&schema_json),
            existing.schema_version,
            created_at,
        );

        self.client
            .execute(&sql)
            .await
            .map_err(|e| MailboxError::storage(format!("failed to delete plugin: {}", e)))?;

        Ok(())
    }
}

fn parse_plugin_from_batch(
    batch: &arrow::array::RecordBatch,
    row: usize,
) -> Result<Plugin, MailboxError> {
    use arrow::array::{Array, Int64Array, StringArray, TimestampMicrosecondArray};

    let plugin_id = batch
        .column_by_name("plugin_id")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .and_then(|a| a.value(row).into())
        .ok_or_else(|| MailboxError::storage("missing plugin_id column"))?;

    let namespace_id = batch
        .column_by_name("namespace_id")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .map(|a| a.value(row))
        .ok_or_else(|| MailboxError::storage("missing namespace_id column"))?;

    let api_key_hash = batch
        .column_by_name("api_key_hash")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .and_then(|a| a.value(row).into())
        .ok_or_else(|| MailboxError::storage("missing api_key_hash column"))?;

    let schema_json: &str = batch
        .column_by_name("schema")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .and_then(|a| a.value(row).into())
        .ok_or_else(|| MailboxError::storage("missing schema column"))?;

    let schema_version = batch
        .column_by_name("schema_version")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .map(|a| a.value(row))
        .ok_or_else(|| MailboxError::storage("missing schema_version column"))?;

    let created_at_micros = batch
        .column_by_name("created_at")
        .and_then(|c| c.as_any().downcast_ref::<TimestampMicrosecondArray>())
        .map(|a| a.value(row))
        .ok_or_else(|| MailboxError::storage("missing created_at column"))?;

    let schema: PluginSchema = serde_json::from_str(schema_json)?;

    let created_at =
        chrono::DateTime::from_timestamp_micros(created_at_micros).unwrap_or_else(Utc::now);

    Ok(Plugin {
        plugin_id: plugin_id.to_string(),
        namespace_id,
        api_key_hash: api_key_hash.to_string(),
        schema,
        schema_version,
        created_at,
    })
}

fn escape_string(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}
