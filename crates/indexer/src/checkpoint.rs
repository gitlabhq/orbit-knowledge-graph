use std::sync::Arc;

use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
use arrow::array::{Array, StringArray, TimestampMicrosecondArray};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use gkg_utils::arrow::ArrowUtils;
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

/// Where a pipeline left off: both time-position (watermark) and page-position (cursor).
///
/// State machine:
/// - No entry: first run, start from epoch, no cursor
/// - `cursor_values: None`: completed, `watermark` becomes the next `last_watermark`
/// - `cursor_values: Some(...)`: interrupted mid-pagination, resume from cursor
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Checkpoint {
    pub watermark: DateTime<Utc>,
    pub cursor_values: Option<Vec<String>>,
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
        cursor_values: &Option<Vec<String>>,
    ) -> Result<(), CheckpointError> {
        let table = prefixed_table_name(CHECKPOINT_TABLE, *SCHEMA_VERSION);
        let formatted_watermark = watermark.format(TIMESTAMP_FORMAT).to_string();
        let cursor_json = serde_json::to_string(cursor_values)
            .map_err(|err| CheckpointError::Store(err.to_string()))?;

        self.client
            .query(&format!(
                "INSERT INTO {table} (key, watermark, cursor_values) \
                 VALUES ({{key:String}}, {{watermark:String}}, {{cursor_values:String}})"
            ))
            .param("key", key)
            .param("watermark", formatted_watermark)
            .param("cursor_values", cursor_json)
            .execute()
            .await
            .map_err(|err| CheckpointError::Store(err.to_string()))?;

        Ok(())
    }
}

#[async_trait]
impl CheckpointStore for ClickHouseCheckpointStore {
    async fn load(&self, key: &str) -> Result<Option<Checkpoint>, CheckpointError> {
        let table = prefixed_table_name(CHECKPOINT_TABLE, *SCHEMA_VERSION);
        let batches = self
            .client
            .query(&format!(
                "SELECT argMax(watermark, _version) AS watermark, \
                        argMax(cursor_values, _version) AS cursor_values \
                 FROM {table} \
                 WHERE key = {{key:String}}"
            ))
            .param("key", key)
            .fetch_arrow()
            .await
            .map_err(|err| CheckpointError::Store(err.to_string()))?;

        let batch = match batches.into_iter().next() {
            Some(batch) if batch.num_rows() > 0 => batch,
            _ => return Ok(None),
        };

        let timestamps: &TimestampMicrosecondArray = ArrowUtils::get_column_by_index(&batch, 0)
            .ok_or_else(|| CheckpointError::Store("invalid watermark type".to_string()))?;

        if timestamps.is_null(0) {
            return Ok(None);
        }

        let micros = timestamps.value(0);
        if micros == 0 {
            return Ok(None);
        }

        let watermark = Utc
            .timestamp_micros(micros)
            .single()
            .ok_or_else(|| CheckpointError::Store("invalid timestamp".to_string()))?;

        let cursor_json =
            ArrowUtils::get_column_by_index::<StringArray>(&batch, 1).and_then(|arr| {
                if arr.is_null(0) || arr.value(0).is_empty() {
                    None
                } else {
                    Some(arr.value(0).to_string())
                }
            });

        let cursor_values: Option<Vec<String>> = match cursor_json {
            Some(json) => serde_json::from_str(&json)
                .map_err(|err| CheckpointError::Store(err.to_string()))?,
            None => None,
        };

        Ok(Some(Checkpoint {
            watermark,
            cursor_values,
        }))
    }

    async fn save_progress(
        &self,
        key: &str,
        checkpoint: &Checkpoint,
    ) -> Result<(), CheckpointError> {
        self.upsert(key, &checkpoint.watermark, &checkpoint.cursor_values)
            .await
    }

    async fn save_completed(
        &self,
        key: &str,
        watermark: &DateTime<Utc>,
    ) -> Result<(), CheckpointError> {
        self.upsert(key, watermark, &None).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialization_roundtrip_completed() {
        let checkpoint = Checkpoint {
            watermark: "2024-06-15T12:00:00Z".parse().unwrap(),
            cursor_values: None,
        };

        let json = serde_json::to_string(&checkpoint).unwrap();
        let deserialized: Checkpoint = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized, checkpoint);
        assert!(deserialized.cursor_values.is_none());
    }

    #[test]
    fn serialization_roundtrip_in_progress() {
        let checkpoint = Checkpoint {
            watermark: "2024-06-15T12:00:00Z".parse().unwrap(),
            cursor_values: Some(vec!["1/2/".to_string(), "42".to_string()]),
        };

        let json = serde_json::to_string(&checkpoint).unwrap();
        let deserialized: Checkpoint = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized, checkpoint);
        assert_eq!(deserialized.cursor_values.unwrap(), vec!["1/2/", "42"]);
    }
}
