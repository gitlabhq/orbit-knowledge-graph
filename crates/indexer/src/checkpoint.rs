use std::sync::Arc;

use std::collections::HashMap;

use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
use arrow::array::{Array, StringArray, TimestampMicrosecondArray};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use gkg_utils::arrow::ArrowUtils;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::topic::IndexingScope;

const CHECKPOINT_TABLE: &str = "checkpoint";

/// The checkpoint key prefix for a given namespace, e.g. `ns.100`.
///
/// The pipeline appends `.{plan_name}` to form the full key, so all
/// checkpoints for a namespace share this prefix followed by a dot.
pub fn namespace_position_key(namespace_id: i64) -> String {
    format!("ns.{namespace_id}")
}

pub struct EntityCheckpointKey {
    scope_prefix: String,
    partition_suffix: Option<String>,
}

impl EntityCheckpointKey {
    pub fn new(scope: &IndexingScope) -> Self {
        let scope_prefix = match scope {
            IndexingScope::Global => "global".to_string(),
            IndexingScope::Namespace { namespace_id, .. } => namespace_position_key(*namespace_id),
        };
        Self {
            scope_prefix,
            partition_suffix: None,
        }
    }

    pub fn with_partition(mut self, index: u32, total: u32) -> Self {
        self.partition_suffix = Some(format!("p{index}of{total}"));
        self
    }

    pub fn prefix(&self) -> &str {
        &self.scope_prefix
    }

    pub fn full_key(&self, plan_name: &str) -> String {
        match &self.partition_suffix {
            None => format!("{}.{plan_name}", self.scope_prefix),
            Some(suffix) => format!("{}.{plan_name}.{suffix}", self.scope_prefix),
        }
    }
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

impl Checkpoint {
    pub fn is_completed(&self) -> bool {
        self.cursor_values.is_none()
    }
}

#[async_trait]
pub trait CheckpointStore: Send + Sync {
    async fn load(&self, key: &str) -> Result<Option<Checkpoint>, CheckpointError>;

    async fn load_by_prefix(
        &self,
        prefix: &str,
    ) -> Result<HashMap<String, Checkpoint>, CheckpointError>;

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
    async fn load_by_prefix(
        &self,
        prefix: &str,
    ) -> Result<HashMap<String, Checkpoint>, CheckpointError> {
        let table = prefixed_table_name(CHECKPOINT_TABLE, *SCHEMA_VERSION);
        let batches = self
            .client
            .query(&format!(
                "SELECT key, \
                        argMax(watermark, _version) AS watermark, \
                        argMax(cursor_values, _version) AS cursor_values \
                 FROM {table} \
                 WHERE startsWith(key, {{prefix:String}}) \
                 GROUP BY key"
            ))
            .param("prefix", prefix)
            .fetch_arrow()
            .await
            .map_err(|err| CheckpointError::Store(err.to_string()))?;

        let mut result = HashMap::new();

        for batch in batches {
            let keys: &StringArray = ArrowUtils::get_column_by_index(&batch, 0)
                .ok_or_else(|| CheckpointError::Store("invalid key column".to_string()))?;
            let timestamps: &TimestampMicrosecondArray = ArrowUtils::get_column_by_index(&batch, 1)
                .ok_or_else(|| CheckpointError::Store("invalid watermark type".to_string()))?;
            let cursors: Option<&StringArray> = ArrowUtils::get_column_by_index(&batch, 2);

            for row in 0..batch.num_rows() {
                if timestamps.is_null(row) || timestamps.value(row) == 0 {
                    continue;
                }

                let key = keys.value(row).to_string();
                let watermark = Utc
                    .timestamp_micros(timestamps.value(row))
                    .single()
                    .ok_or_else(|| CheckpointError::Store("invalid timestamp".to_string()))?;

                let cursor_values: Option<Vec<String>> = cursors
                    .and_then(|arr| {
                        if arr.is_null(row) || arr.value(row).is_empty() {
                            None
                        } else {
                            Some(arr.value(row).to_string())
                        }
                    })
                    .map(|json| serde_json::from_str(&json))
                    .transpose()
                    .map_err(|err| CheckpointError::Store(err.to_string()))?;

                result.insert(
                    key,
                    Checkpoint {
                        watermark,
                        cursor_values,
                    },
                );
            }
        }

        Ok(result)
    }

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

    #[test]
    fn entity_key_global_prefix() {
        let key = EntityCheckpointKey::new(&IndexingScope::Global);
        assert_eq!(key.prefix(), "global");
    }

    #[test]
    fn entity_key_global_full_key() {
        let key = EntityCheckpointKey::new(&IndexingScope::Global);
        assert_eq!(key.full_key("User"), "global.User");
    }

    #[test]
    fn entity_key_namespaced_prefix() {
        let scope = IndexingScope::Namespace {
            namespace_id: 100,
            traversal_path: "42/100/".to_string(),
        };
        let key = EntityCheckpointKey::new(&scope);
        assert_eq!(key.prefix(), "ns.100");
    }

    #[test]
    fn entity_key_namespaced_with_partition() {
        let scope = IndexingScope::Namespace {
            namespace_id: 100,
            traversal_path: "42/100/".to_string(),
        };
        let key = EntityCheckpointKey::new(&scope).with_partition(2, 4);
        assert_eq!(key.full_key("MergeRequest"), "ns.100.MergeRequest.p2of4");
    }
}
