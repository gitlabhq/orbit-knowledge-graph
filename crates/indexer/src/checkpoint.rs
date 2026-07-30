use std::sync::Arc;

use crate::clickhouse::{
    ArrowClickHouseClient, ArrowQuery, QUORUM_RETRY_MAX_ATTEMPTS, TIMESTAMP_FORMAT,
    is_unsatisfied_quorum, quorum_retry_backoff,
};
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
    format!("{NAMESPACE_KEY_PREFIX}{namespace_id}")
}

pub const NAMESPACE_KEY_PREFIX: &str = "ns.";

/// Inverse of [`namespace_position_key`], tolerating the `.{plan_name}` and
/// partition suffixes the pipeline appends.
pub fn namespace_id_from_key(key: &str) -> Option<i64> {
    key.strip_prefix(NAMESPACE_KEY_PREFIX)?
        .split('.')
        .next()?
        .parse()
        .ok()
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
    #[serde(default)]
    pub resume_floor: Option<DateTime<Utc>>,
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
        cursor_values: &Option<Vec<String>>,
        resume_floor: &Option<DateTime<Utc>>,
        durability: WriteDurability,
    ) -> Result<(), CheckpointError> {
        let table = prefixed_table_name(CHECKPOINT_TABLE, *SCHEMA_VERSION);
        let formatted_watermark = watermark.format(TIMESTAMP_FORMAT).to_string();
        let cursor_json = encode_cursor_column(cursor_values, resume_floor)?;

        let version = client_version();
        self.execute_with_quorum_retry(|| {
            self.insert(
                &format!(
                    "INSERT INTO {table} (key, watermark, cursor_values, _version) \
                     VALUES ({{key:String}}, {{watermark:String}}, {{cursor_values:String}}, {{version:String}})"
                ),
                durability,
            )
            .param("key", key)
            .param("watermark", formatted_watermark.clone())
            .param("cursor_values", cursor_json.clone())
            .param("version", version.clone())
        })
        .await
        .map_err(checkpoint_store_error)?;

        Ok(())
    }

    async fn tombstone(&self, key: &str, watermark: &DateTime<Utc>) -> Result<(), CheckpointError> {
        let table = prefixed_table_name(CHECKPOINT_TABLE, *SCHEMA_VERSION);
        let formatted_watermark = watermark.format(TIMESTAMP_FORMAT).to_string();

        let version = client_version();
        self.execute_with_quorum_retry(|| {
            self.insert(
                &format!(
                    "INSERT INTO {table} (key, watermark, cursor_values, _version, _deleted) \
                     VALUES ({{key:String}}, {{watermark:String}}, '', {{version:String}}, true)"
                ),
                WriteDurability::Durable,
            )
            .param("key", key)
            .param("watermark", formatted_watermark.clone())
            .param("version", version.clone())
        })
        .await
        .map_err(checkpoint_store_error)?;

        Ok(())
    }

    /// See the quorum-retry rationale on [`crate::clickhouse::writer`]'s write path:
    /// serialized quorum inserts make transient 286s part of the protocol.
    async fn execute_with_quorum_retry(
        &self,
        build: impl Fn() -> ArrowQuery,
    ) -> Result<(), clickhouse_client::ClickHouseError> {
        let mut attempt = 0u32;
        loop {
            match build().execute().await {
                Ok(_) => return Ok(()),
                Err(error)
                    if self.client.has_quorum_writes()
                        && attempt < QUORUM_RETRY_MAX_ATTEMPTS
                        && is_unsatisfied_quorum(&error) =>
                {
                    attempt += 1;
                    tokio::time::sleep(quorum_retry_backoff(attempt)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Sequential-consistency reads fail loud (289) on a replica outside the last
    /// write's quorum; the load balancer rotates replicas between attempts.
    async fn fetch_with_quorum_retry(
        &self,
        build: impl Fn() -> ArrowQuery,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, clickhouse_client::ClickHouseError> {
        let mut attempt = 0u32;
        loop {
            match build().fetch_arrow().await {
                Ok(batches) => return Ok(batches),
                Err(error)
                    if self.client.has_quorum_writes()
                        && attempt < QUORUM_RETRY_MAX_ATTEMPTS
                        && is_unsatisfied_quorum(&error) =>
                {
                    attempt += 1;
                    tokio::time::sleep(quorum_retry_backoff(attempt)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    // Single-row inserts must pin async batching regardless of durability, or per-row inserts
    // explode the part count. ClickHouse rejects async inserts on a quorum-write cluster, so
    // those deployments take the part-count hit instead.
    fn insert(&self, sql: &str, durability: WriteDurability) -> ArrowQuery {
        let query = self.client.query(sql);
        if self.client.has_quorum_writes() {
            return query;
        }
        let wait_for_flush = match durability {
            WriteDurability::FireAndForget => "0",
            WriteDurability::Durable => "1",
        };
        query
            .with_setting("async_insert", "1")
            .with_setting("wait_for_async_insert", wait_for_flush)
    }
}

/// Flush-time `now64` defaults would let a buffered progress row outrank a later durable completion.
fn client_version() -> String {
    Utc::now().format(TIMESTAMP_FORMAT).to_string()
}

/// The `cursor_values` column as JSON: the sort-key cursor plus the window floor.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct CursorColumn {
    #[serde(rename = "c")]
    cursor: Vec<String>,
    #[serde(rename = "f", default, skip_serializing_if = "Option::is_none")]
    floor: Option<DateTime<Utc>>,
}

fn encode_cursor_column(
    cursor_values: &Option<Vec<String>>,
    resume_floor: &Option<DateTime<Utc>>,
) -> Result<String, CheckpointError> {
    match cursor_values {
        None => Ok("null".to_string()),
        Some(cursor) => serde_json::to_string(&CursorColumn {
            cursor: cursor.clone(),
            floor: *resume_floor,
        })
        .map_err(checkpoint_store_error),
    }
}

fn decode_cursor_column(raw: &str) -> Result<Option<CursorColumn>, CheckpointError> {
    if raw.is_empty() {
        return Ok(None);
    }
    serde_json::from_str(raw).map_err(checkpoint_store_error)
}

fn checkpoint_store_error<E: std::fmt::Display>(err: E) -> CheckpointError {
    CheckpointError::Store(err.to_string())
}

#[async_trait]
impl CheckpointStore for ClickHouseCheckpointStore {
    async fn load(&self, key: &str) -> Result<Option<Checkpoint>, CheckpointError> {
        let table = prefixed_table_name(CHECKPOINT_TABLE, *SCHEMA_VERSION);
        let batches = self
            .fetch_with_quorum_retry(|| {
                self.client
                    .query(&format!(
                        "SELECT argMax(watermark, _version) AS watermark, \
                                argMax(cursor_values, _version) AS cursor_values, \
                                argMax(_deleted, _version) AS deleted \
                         FROM {table} \
                         WHERE key = {{key:String}}"
                    ))
                    .param("key", key)
            })
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

        let decoded = decode_cursor_column(&cursor_json)?;
        Ok(Some(Checkpoint {
            watermark,
            cursor_values: decoded.as_ref().map(|c| c.cursor.clone()),
            resume_floor: decoded.and_then(|c| c.floor),
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
            &checkpoint.cursor_values,
            &checkpoint.resume_floor,
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
        self.upsert(key, watermark, &None, &None, durability).await
    }

    async fn load_by_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, Checkpoint)>, CheckpointError> {
        let table = prefixed_table_name(CHECKPOINT_TABLE, *SCHEMA_VERSION);
        let batches = self
            .fetch_with_quorum_retry(|| {
                self.client
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
            })
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
                Some(decode_cursor_column(&cursor_json).map(|decoded| {
                    (
                        key,
                        Checkpoint {
                            watermark,
                            cursor_values: decoded.as_ref().map(|c| c.cursor.clone()),
                            resume_floor: decoded.and_then(|c| c.floor),
                        },
                    )
                }))
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
            cursor_values: None,
            resume_floor: None,
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
            resume_floor: Some("2024-06-15T11:59:30Z".parse().unwrap()),
        };

        let json = serde_json::to_string(&checkpoint).unwrap();
        let deserialized: Checkpoint = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized, checkpoint);
        assert_eq!(deserialized.cursor_values.unwrap(), vec!["1/2/", "42"]);
    }

    #[test]
    fn cursor_column_completed_encodes_as_null() {
        assert_eq!(encode_cursor_column(&None, &None).unwrap(), "null");
        assert_eq!(decode_cursor_column("null").unwrap(), None);
        assert_eq!(decode_cursor_column("").unwrap(), None);
    }

    #[test]
    fn cursor_column_roundtrips_cursor_and_floor() {
        let cursor = Some(vec!["1/2/".to_string(), "42".to_string()]);
        let floor: Option<DateTime<Utc>> = Some("2024-06-15T11:59:30Z".parse().unwrap());

        let encoded = encode_cursor_column(&cursor, &floor).unwrap();
        let decoded = decode_cursor_column(&encoded).unwrap().unwrap();
        assert_eq!(Some(decoded.cursor), cursor);
        assert_eq!(decoded.floor, floor);
    }
}

#[cfg(test)]
mod namespace_key_tests {
    use super::*;

    #[test]
    fn namespace_id_from_key_tolerates_plan_and_partition_suffixes() {
        assert_eq!(namespace_id_from_key("ns.42.MergeRequest"), Some(42));
        assert_eq!(namespace_id_from_key("ns.7.Job.p1of3"), Some(7));
        assert_eq!(namespace_id_from_key("maintenance.something"), None);
    }

    #[test]
    fn namespace_id_from_key_inverts_namespace_position_key() {
        assert_eq!(namespace_id_from_key(&namespace_position_key(99)), Some(99));
    }
}
