use std::sync::Arc;

use crate::clickhouse::{ArrowClickHouseClient, ArrowQuery, TIMESTAMP_FORMAT};
use crate::durability::WriteDurability;
use crate::observer::IndexingMode;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use clickhouse_client::FromArrowColumn;
use orbit_migrations::version::{SCHEMA_VERSION, prefixed_table_name};
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
/// - No entry, or no cursor and no `indexed_at`: first pass, start from epoch;
///   `attempts` counts the first-pass runs that started
/// - No cursor and `indexed_at`: completed, `watermark` becomes the next `last_watermark`
/// - `cursor_values: Some(...)`: interrupted mid-pagination, resume from cursor
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Checkpoint {
    pub watermark: DateTime<Utc>,
    pub cursor_values: Option<Vec<String>>,
    #[serde(default)]
    pub resume_floor: Option<DateTime<Utc>>,
    #[serde(default)]
    pub attempts: i64,
    #[serde(default)]
    pub indexed_at: Option<DateTime<Utc>>,
}

impl Checkpoint {
    pub fn new(watermark: DateTime<Utc>) -> Self {
        Self {
            watermark,
            cursor_values: None,
            resume_floor: None,
            attempts: 0,
            indexed_at: None,
        }
    }

    pub fn indexing_mode(&self) -> IndexingMode {
        match self.indexed_at {
            Some(_) => IndexingMode::Incremental,
            None => IndexingMode::Full,
        }
    }

    pub fn completed_watermark(&self) -> Option<DateTime<Utc>> {
        self.indexed_at.map(|_| self.watermark)
    }

    pub fn is_first_pass_before_paging(&self) -> bool {
        self.indexed_at.is_none() && self.cursor_values.is_none()
    }

    pub fn start_attempt(&mut self) {
        self.attempts += 1;
    }

    pub fn record_page(
        &mut self,
        window_target: DateTime<Utc>,
        window_floor: Option<DateTime<Utc>>,
        cursor_values: Option<Vec<String>>,
    ) {
        self.watermark = window_target;
        self.resume_floor = window_floor;
        self.cursor_values = cursor_values;
    }

    pub fn complete(&mut self, watermark: DateTime<Utc>) {
        self.watermark = watermark;
        self.cursor_values = None;
        self.resume_floor = None;
        self.attempts = 0;
        self.indexed_at = Some(Utc::now());
    }
}

#[async_trait]
pub trait CheckpointStore: Send + Sync {
    async fn load(&self, key: &str) -> Result<Option<Checkpoint>, CheckpointError>;

    async fn save(
        &self,
        key: &str,
        checkpoint: &Checkpoint,
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

enum KeyFilter {
    Exact(String),
    Prefix(String),
}

impl KeyFilter {
    fn value(&self) -> &str {
        match self {
            KeyFilter::Exact(key) | KeyFilter::Prefix(key) => key,
        }
    }
}

impl ClickHouseCheckpointStore {
    pub fn new(client: Arc<ArrowClickHouseClient>) -> Self {
        Self { client }
    }

    // The newest row wins per key, except `indexed_at`: a page write from an overlapping run must
    // not hide a completion, so it is the newest value since the last tombstone.
    async fn load_current_checkpoints(
        &self,
        keys: KeyFilter,
    ) -> Result<Vec<(String, Checkpoint)>, CheckpointError> {
        let table = prefixed_table_name(CHECKPOINT_TABLE, *SCHEMA_VERSION);
        let key_condition = match keys {
            KeyFilter::Exact(_) => "key = {key:String}",
            KeyFilter::Prefix(_) => "startsWith(key, {key:String})",
        };
        let batches = self
            .client
            .query(&format!(
                "SELECT key, \
                        argMax(watermark, _version) AS watermark, \
                        argMax(cursor_values, _version) AS cursor_values, \
                        argMax(attempts, _version) AS attempts, \
                        maxIf(indexed_at, _version > tombstoned_at) AS indexed_at \
                 FROM (SELECT *, maxIf(_version, _deleted) OVER (PARTITION BY key) AS tombstoned_at \
                       FROM {table} WHERE {key_condition}) \
                 GROUP BY key \
                 HAVING argMax(_deleted, _version) = false"
            ))
            .param("key", keys.value())
            .fetch_arrow()
            .await
            .map_err(checkpoint_store_error)?;

        let keys = String::extract_column(&batches, 0).map_err(checkpoint_store_error)?;
        let watermarks =
            DateTime::<Utc>::extract_column(&batches, 1).map_err(checkpoint_store_error)?;
        let cursor_jsons = String::extract_column(&batches, 2).map_err(checkpoint_store_error)?;
        let attempts = i64::extract_column(&batches, 3).map_err(checkpoint_store_error)?;
        let indexed_ats =
            Option::<DateTime<Utc>>::extract_column(&batches, 4).map_err(checkpoint_store_error)?;

        keys.into_iter()
            .zip(watermarks)
            .zip(cursor_jsons)
            .zip(attempts)
            .zip(indexed_ats)
            .map(
                |((((key, watermark), cursor_json), attempts), indexed_at)| {
                    decode_checkpoint(watermark, &cursor_json, attempts, indexed_at)
                        .map(|checkpoint| (key, checkpoint))
                },
            )
            .collect()
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

fn decode_checkpoint(
    watermark: DateTime<Utc>,
    cursor_json: &str,
    attempts: i64,
    indexed_at: Option<DateTime<Utc>>,
) -> Result<Checkpoint, CheckpointError> {
    let decoded = decode_cursor_column(cursor_json)?;
    Ok(Checkpoint {
        watermark,
        cursor_values: decoded.as_ref().map(|c| c.cursor.clone()),
        resume_floor: decoded.and_then(|c| c.floor),
        attempts,
        indexed_at,
    })
}

fn checkpoint_store_error<E: std::fmt::Display>(err: E) -> CheckpointError {
    CheckpointError::Store(err.to_string())
}

#[async_trait]
impl CheckpointStore for ClickHouseCheckpointStore {
    async fn load(&self, key: &str) -> Result<Option<Checkpoint>, CheckpointError> {
        let mut rows = self
            .load_current_checkpoints(KeyFilter::Exact(key.to_string()))
            .await?;
        Ok(rows.pop().map(|(_, checkpoint)| checkpoint))
    }

    async fn save(
        &self,
        key: &str,
        checkpoint: &Checkpoint,
        durability: WriteDurability,
    ) -> Result<(), CheckpointError> {
        let table = prefixed_table_name(CHECKPOINT_TABLE, *SCHEMA_VERSION);
        let formatted_watermark = checkpoint.watermark.format(TIMESTAMP_FORMAT).to_string();
        let cursor_json =
            encode_cursor_column(&checkpoint.cursor_values, &checkpoint.resume_floor)?;

        self.insert(
            &format!(
                "INSERT INTO {table} (key, watermark, cursor_values, attempts, indexed_at, _version) \
                 VALUES ({{key:String}}, {{watermark:String}}, {{cursor_values:String}}, {{attempts:Int64}}, \
                         {{indexed_at:Nullable(String)}}, {{version:String}})"
            ),
            durability,
        )
        .param("key", key)
        .param("watermark", formatted_watermark)
        .param("cursor_values", cursor_json)
        .param("attempts", checkpoint.attempts)
        .param(
            "indexed_at",
            checkpoint
                .indexed_at
                .map(|indexed_at| indexed_at.format(TIMESTAMP_FORMAT).to_string()),
        )
        .param("version", client_version())
        .execute()
        .await
        .map_err(checkpoint_store_error)?;

        Ok(())
    }

    async fn load_by_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, Checkpoint)>, CheckpointError> {
        self.load_current_checkpoints(KeyFilter::Prefix(prefix.to_string()))
            .await
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

        let mut parent = self
            .load(parent_key)
            .await?
            .unwrap_or_else(|| Checkpoint::new(*watermark));
        parent.complete(*watermark);
        self.save(parent_key, &parent, WriteDurability::Durable)
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
        let checkpoint = Checkpoint::new("2024-06-15T12:00:00Z".parse().unwrap());

        let json = serde_json::to_string(&checkpoint).unwrap();
        let deserialized: Checkpoint = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized, checkpoint);
        assert!(deserialized.cursor_values.is_none());
    }

    #[test]
    fn serialization_roundtrip_in_progress() {
        let checkpoint = Checkpoint {
            cursor_values: Some(vec!["1/2/".to_string(), "42".to_string()]),
            resume_floor: Some("2024-06-15T11:59:30Z".parse().unwrap()),
            ..Checkpoint::new("2024-06-15T12:00:00Z".parse().unwrap())
        };

        let json = serde_json::to_string(&checkpoint).unwrap();
        let deserialized: Checkpoint = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized, checkpoint);
        assert_eq!(deserialized.cursor_values.unwrap(), vec!["1/2/", "42"]);
    }

    #[test]
    fn start_attempt_keeps_indexed_at_and_complete_resets_attempts() {
        let mut checkpoint = Checkpoint::new("2024-06-15T12:00:00Z".parse().unwrap());
        checkpoint.start_attempt();
        checkpoint.start_attempt();
        assert_eq!(checkpoint.attempts, 2);
        assert!(checkpoint.indexed_at.is_none());

        checkpoint.complete("2024-06-15T13:00:00Z".parse().unwrap());
        let indexed_at = checkpoint.indexed_at;
        assert_eq!(checkpoint.attempts, 0);
        assert!(indexed_at.is_some());

        checkpoint.start_attempt();
        assert_eq!(checkpoint.attempts, 1);
        assert_eq!(checkpoint.indexed_at, indexed_at);
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
