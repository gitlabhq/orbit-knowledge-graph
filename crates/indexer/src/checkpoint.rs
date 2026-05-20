use std::sync::Arc;

use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
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

pub struct EntityCheckpointKey {
    prefix: String,
}

impl EntityCheckpointKey {
    pub fn new(scope: &crate::topic::IndexingScope, entity_kind: &str) -> Self {
        let scope_prefix = match scope {
            crate::topic::IndexingScope::Global => "global".to_string(),
            crate::topic::IndexingScope::Namespace { namespace_id, .. } => {
                format!("ns.{namespace_id}")
            }
        };
        Self {
            prefix: format!("{scope_prefix}.{entity_kind}"),
        }
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn partition_key(&self, index: u32, total: u32) -> String {
        format!("{}.p{index}of{total}", self.prefix)
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

impl Default for Checkpoint {
    fn default() -> Self {
        Self {
            watermark: DateTime::<Utc>::UNIX_EPOCH,
            cursor_values: None,
        }
    }
}

#[async_trait]
pub trait CheckpointStore: Send + Sync {
    async fn load(&self, key: &str) -> Result<Option<Checkpoint>, CheckpointError>;

    async fn load_by_prefix(
        &self,
        entity_prefix: &str,
    ) -> Result<Vec<(String, Checkpoint)>, CheckpointError>;

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

    async fn delete(&self, key: &str) -> Result<(), CheckpointError>;

    async fn load_consolidated(&self, key: &str) -> Result<Option<Checkpoint>, CheckpointError> {
        let checkpoints = self.load_by_prefix(key).await?;
        if checkpoints.is_empty() {
            return Ok(None);
        }

        let (unified, partitions): (Vec<_>, Vec<_>) =
            checkpoints.into_iter().partition(|(k, _)| k == key);

        if partitions.is_empty() {
            return Ok(unified.into_iter().next().map(|(_, cp)| cp));
        }

        let min_watermark = partitions
            .iter()
            .map(|(_, cp)| cp.watermark)
            .min()
            .expect("partitions is non-empty");

        self.save_completed(key, &min_watermark).await?;
        for (partition_key, _) in &partitions {
            self.delete(partition_key).await?;
        }

        Ok(Some(Checkpoint {
            watermark: min_watermark,
            cursor_values: None,
        }))
    }
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

#[derive(Deserialize)]
struct CheckpointRow {
    key: String,
    watermark: i64,
    cursor_values: Option<String>,
}

#[derive(Deserialize)]
struct CheckpointRowNoKey {
    watermark: i64,
    cursor_values: Option<String>,
}

fn parse_watermark(micros: i64) -> Result<Option<DateTime<Utc>>, CheckpointError> {
    if micros == 0 {
        return Ok(None);
    }
    Utc.timestamp_micros(micros)
        .single()
        .map(Some)
        .ok_or_else(|| CheckpointError::Store("invalid timestamp".to_string()))
}

fn parse_cursor_values(raw: Option<String>) -> Result<Option<Vec<String>>, CheckpointError> {
    match raw {
        Some(json) if !json.is_empty() => {
            serde_json::from_str(&json).map_err(|err| CheckpointError::Store(err.to_string()))
        }
        _ => Ok(None),
    }
}

#[async_trait]
impl CheckpointStore for ClickHouseCheckpointStore {
    async fn load_by_prefix(
        &self,
        entity_prefix: &str,
    ) -> Result<Vec<(String, Checkpoint)>, CheckpointError> {
        let table = prefixed_table_name(CHECKPOINT_TABLE, *SCHEMA_VERSION);
        let batches = self
            .client
            .query(&format!(
                "SELECT key, \
                        argMax(watermark, _version) AS watermark, \
                        argMax(cursor_values, _version) AS cursor_values \
                 FROM {table} \
                 WHERE _deleted = false \
                   AND (key = {{prefix:String}} \
                    OR startsWith(key, concat({{prefix:String}}, '.p'))) \
                 GROUP BY key"
            ))
            .param("prefix", entity_prefix)
            .fetch_arrow()
            .await
            .map_err(|err| CheckpointError::Store(err.to_string()))?;

        let mut results = Vec::new();

        for batch in &batches {
            let rows: Vec<CheckpointRow> = serde_arrow::from_record_batch(batch)
                .map_err(|err| CheckpointError::Store(err.to_string()))?;

            for row in rows {
                let Some(watermark) = parse_watermark(row.watermark)? else {
                    continue;
                };
                let cursor_values = parse_cursor_values(row.cursor_values)?;
                results.push((
                    row.key,
                    Checkpoint {
                        watermark,
                        cursor_values,
                    },
                ));
            }
        }

        Ok(results)
    }

    async fn load(&self, key: &str) -> Result<Option<Checkpoint>, CheckpointError> {
        let table = prefixed_table_name(CHECKPOINT_TABLE, *SCHEMA_VERSION);
        let batches = self
            .client
            .query(&format!(
                "SELECT argMax(watermark, _version) AS watermark, \
                        argMax(cursor_values, _version) AS cursor_values \
                 FROM {table} \
                 WHERE _deleted = false \
                   AND key = {{key:String}}"
            ))
            .param("key", key)
            .fetch_arrow()
            .await
            .map_err(|err| CheckpointError::Store(err.to_string()))?;

        let batch = match batches.into_iter().next() {
            Some(batch) if batch.num_rows() > 0 => batch,
            _ => return Ok(None),
        };

        let rows: Vec<CheckpointRowNoKey> = serde_arrow::from_record_batch(&batch)
            .map_err(|err| CheckpointError::Store(err.to_string()))?;

        let Some(row) = rows.into_iter().next() else {
            return Ok(None);
        };

        let Some(watermark) = parse_watermark(row.watermark)? else {
            return Ok(None);
        };
        let cursor_values = parse_cursor_values(row.cursor_values)?;

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

    async fn delete(&self, key: &str) -> Result<(), CheckpointError> {
        let table = prefixed_table_name(CHECKPOINT_TABLE, *SCHEMA_VERSION);
        self.client
            .query(&format!(
                "INSERT INTO {table} (key, watermark, cursor_values, _deleted) \
                 VALUES ({{key:String}}, '1970-01-01 00:00:00.000000', '', true)"
            ))
            .param("key", key)
            .execute()
            .await
            .map_err(|err| CheckpointError::Store(err.to_string()))?;
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
    fn entity_checkpoint_key_prefix_global() {
        let key = EntityCheckpointKey::new(&crate::topic::IndexingScope::Global, "User");
        assert_eq!(key.prefix(), "global.User");
    }

    #[test]
    fn entity_checkpoint_key_prefix_namespaced() {
        let scope = crate::topic::IndexingScope::Namespace {
            namespace_id: 100,
            traversal_path: "42/100/".to_string(),
        };
        let key = EntityCheckpointKey::new(&scope, "MergeRequest");
        assert_eq!(key.prefix(), "ns.100.MergeRequest");
    }

    #[test]
    fn entity_checkpoint_key_partition() {
        let scope = crate::topic::IndexingScope::Namespace {
            namespace_id: 100,
            traversal_path: "42/100/".to_string(),
        };
        let key = EntityCheckpointKey::new(&scope, "MR");
        assert_eq!(key.partition_key(0, 4), "ns.100.MR.p0of4");
        assert_eq!(key.partition_key(3, 4), "ns.100.MR.p3of4");
    }
}
