use std::sync::Arc;

use crate::clickhouse::{ArrowClickHouseClient, ArrowQuery, TIMESTAMP_FORMAT};
use crate::durability::WriteDurability;
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use clickhouse_client::FromArrowColumn;
use serde::{Deserialize, Serialize};
use thiserror::Error;

const CHECKPOINT_TABLE: &str = "checkpoint";

/// The checkpoint key prefix for a given namespace, e.g. `ns.100`.
///
/// The pipeline appends `.{plan_name}` to form the full key, so all
/// checkpoints for a namespace share this prefix followed by a dot.
pub fn namespace_position_key(namespace_id: i64) -> String {
    format!("ns.{namespace_id}")
}

#[derive(Debug, Error)]
pub enum CheckpointError {
    #[error("checkpoint store operation failed: {0}")]
    Store(String),
}

/// Where a pipeline left off: the completed time boundary and an opaque source resume value.
///
/// State machine:
/// - No entry: first run
/// - `resume: None`: completed
/// - `resume: Some(...)`: interrupted during extraction
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Checkpoint {
    pub watermark: DateTime<Utc>,
    pub resume: Option<String>,
}

#[async_trait]
pub trait CheckpointStore: Send + Sync {
    async fn load(&self, key: &str) -> Result<Option<Checkpoint>, CheckpointError>;

    async fn save_progress(
        &self,
        key: &str,
        checkpoint: &Checkpoint,
    ) -> Result<(), CheckpointError>;

    async fn save_completed(
        &self,
        key: &str,
        watermark: &DateTime<Utc>,
        durability: WriteDurability,
    ) -> Result<(), CheckpointError>;

    async fn load_by_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, Checkpoint)>, CheckpointError>;

    async fn consolidate(
        &self,
        parent_key: &str,
        watermark: &DateTime<Utc>,
    ) -> Result<(), CheckpointError>;
}

pub struct ClickHouseCheckpointStore {
    client: Arc<ArrowClickHouseClient>,
}

impl ClickHouseCheckpointStore {
    pub fn new(client: Arc<ArrowClickHouseClient>) -> Self {
        Self { client }
    }

    async fn upsert(
        &self,
        key: &str,
        watermark: &DateTime<Utc>,
        resume: &Option<String>,
        durability: WriteDurability,
    ) -> Result<(), CheckpointError> {
        let table = prefixed_table_name(CHECKPOINT_TABLE, *SCHEMA_VERSION);
        let formatted_watermark = watermark.format(TIMESTAMP_FORMAT).to_string();
        let resume_value = resume.as_deref().unwrap_or("null");

        self.insert(
            &format!(
                "INSERT INTO {table} (key, watermark, cursor_values, _version) \
                 VALUES ({{key:String}}, {{watermark:String}}, {{cursor_values:String}}, {{version:String}})"
            ),
            durability,
        )
        .param("key", key)
        .param("watermark", formatted_watermark)
        .param("cursor_values", resume_value)
        .param("version", client_version())
        .execute()
        .await
        .map_err(checkpoint_store_error)?;

        Ok(())
    }

    async fn tombstone(&self, key: &str, watermark: &DateTime<Utc>) -> Result<(), CheckpointError> {
        let table = prefixed_table_name(CHECKPOINT_TABLE, *SCHEMA_VERSION);
        let formatted_watermark = watermark.format(TIMESTAMP_FORMAT).to_string();

        self.insert(
            &format!(
                "INSERT INTO {table} (key, watermark, cursor_values, _version, _deleted) \
                 VALUES ({{key:String}}, {{watermark:String}}, '', {{version:String}}, true)"
            ),
            WriteDurability::Durable,
        )
        .param("key", key)
        .param("watermark", formatted_watermark)
        .param("version", client_version())
        .execute()
        .await
        .map_err(checkpoint_store_error)?;

        Ok(())
    }

    // Single-row inserts must pin async batching regardless of durability, or per-row inserts
    // explode the part count.
    fn insert(&self, sql: &str, durability: WriteDurability) -> ArrowQuery {
        let wait_for_flush = match durability {
            WriteDurability::FireAndForget => "0",
            WriteDurability::Durable => "1",
        };
        self.client
            .query(sql)
            .with_setting("async_insert", "1")
            .with_setting("wait_for_async_insert", wait_for_flush)
    }
}

/// Flush-time `now64` defaults would let a buffered progress row outrank a later durable completion.
fn client_version() -> String {
    Utc::now().format(TIMESTAMP_FORMAT).to_string()
}

fn decode_resume_column(raw: &str) -> Option<String> {
    if raw.is_empty() || raw == "null" {
        None
    } else {
        Some(raw.to_string())
    }
}

fn checkpoint_store_error<E: std::fmt::Display>(err: E) -> CheckpointError {
    CheckpointError::Store(err.to_string())
}

#[async_trait]
impl CheckpointStore for ClickHouseCheckpointStore {
    async fn load(&self, key: &str) -> Result<Option<Checkpoint>, CheckpointError> {
        let table = prefixed_table_name(CHECKPOINT_TABLE, *SCHEMA_VERSION);
        let batches = self
            .client
            .query(&format!(
                "SELECT argMax(watermark, _version) AS watermark, \
                        argMax(cursor_values, _version) AS cursor_values, \
                        argMax(_deleted, _version) AS deleted \
                 FROM {table} \
                 WHERE key = {{key:String}}"
            ))
            .param("key", key)
            .fetch_arrow()
            .await
            .map_err(checkpoint_store_error)?;

        let watermarks =
            DateTime::<Utc>::extract_column(&batches, 0).map_err(checkpoint_store_error)?;
        let Some(watermark) = watermarks.into_iter().next() else {
            return Ok(None);
        };
        // argMax over an empty set returns the column's default value because
        // `watermark` is declared non-nullable in the checkpoint schema. A
        // genuine row never carries the epoch, so treat it as a missing entry.
        if watermark == DateTime::<Utc>::UNIX_EPOCH {
            return Ok(None);
        }

        let deleted = bool::extract_column(&batches, 2)
            .map_err(checkpoint_store_error)?
            .into_iter()
            .next()
            .unwrap_or(false);
        if deleted {
            return Ok(None);
        }

        let cursor_json = String::extract_column(&batches, 1)
            .map_err(checkpoint_store_error)?
            .into_iter()
            .next()
            .unwrap_or_default();

        Ok(Some(Checkpoint {
            watermark,
            resume: decode_resume_column(&cursor_json),
        }))
    }

    async fn save_progress(
        &self,
        key: &str,
        checkpoint: &Checkpoint,
    ) -> Result<(), CheckpointError> {
        self.upsert(
            key,
            &checkpoint.watermark,
            &checkpoint.resume,
            WriteDurability::FireAndForget,
        )
        .await
    }

    async fn save_completed(
        &self,
        key: &str,
        watermark: &DateTime<Utc>,
        durability: WriteDurability,
    ) -> Result<(), CheckpointError> {
        self.upsert(key, watermark, &None, durability).await
    }

    async fn load_by_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, Checkpoint)>, CheckpointError> {
        let table = prefixed_table_name(CHECKPOINT_TABLE, *SCHEMA_VERSION);
        let batches = self
            .client
            .query(&format!(
                "SELECT key, \
                        argMax(watermark, _version) AS watermark, \
                        argMax(cursor_values, _version) AS cursor_values, \
                        argMax(_deleted, _version) AS deleted \
                 FROM {table} \
                 WHERE startsWith(key, {{prefix:String}}) \
                 GROUP BY key"
            ))
            .param("prefix", prefix)
            .fetch_arrow()
            .await
            .map_err(checkpoint_store_error)?;

        let keys = String::extract_column(&batches, 0).map_err(checkpoint_store_error)?;
        let watermarks =
            DateTime::<Utc>::extract_column(&batches, 1).map_err(checkpoint_store_error)?;
        let cursor_jsons = String::extract_column(&batches, 2).map_err(checkpoint_store_error)?;
        let deleted = bool::extract_column(&batches, 3).map_err(checkpoint_store_error)?;

        keys.into_iter()
            .zip(watermarks)
            .zip(cursor_jsons)
            .zip(deleted)
            .filter_map(|(((key, watermark), cursor_json), is_deleted)| {
                if is_deleted {
                    return None;
                }
                Some(Ok((
                    key,
                    Checkpoint {
                        watermark,
                        resume: decode_resume_column(&cursor_json),
                    },
                )))
            })
            .collect()
    }

    async fn consolidate(
        &self,
        parent_key: &str,
        watermark: &DateTime<Utc>,
    ) -> Result<(), CheckpointError> {
        let partition_prefix = format!("{parent_key}.p");
        let partition_keys: Vec<String> = self
            .load_by_prefix(&partition_prefix)
            .await?
            .into_iter()
            .map(|(key, _)| key)
            .collect();

        self.save_completed(parent_key, watermark, WriteDurability::Durable)
            .await?;

        for key in partition_keys {
            self.tombstone(&key, watermark).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialization_roundtrip_completed() {
        let checkpoint = Checkpoint {
            watermark: "2024-06-15T12:00:00Z".parse().unwrap(),
            resume: None,
        };

        let json = serde_json::to_string(&checkpoint).unwrap();
        let deserialized: Checkpoint = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized, checkpoint);
        assert!(deserialized.resume.is_none());
    }

    #[test]
    fn serialization_roundtrip_in_progress() {
        let checkpoint = Checkpoint {
            watermark: "2024-06-15T12:00:00Z".parse().unwrap(),
            resume: Some("{\"source\":\"clickhouse\"}".to_string()),
        };

        let json = serde_json::to_string(&checkpoint).unwrap();
        let deserialized: Checkpoint = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized, checkpoint);
        assert_eq!(
            deserialized.resume.as_deref(),
            Some("{\"source\":\"clickhouse\"}")
        );
    }

    #[test]
    fn completed_resume_column_decodes_as_none() {
        assert_eq!(decode_resume_column("null"), None);
        assert_eq!(decode_resume_column(""), None);
    }

    #[test]
    fn opaque_resume_column_roundtrips_without_interpretation() {
        let resume = "{\"source\":\"git\",\"version\":1}";
        assert_eq!(decode_resume_column(resume).as_deref(), Some(resume));
    }
}
