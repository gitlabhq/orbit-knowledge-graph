use std::sync::Arc;

use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};
use arrow::array::{Array, StringArray, TimestampMicrosecondArray};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const POSITION_TABLE: &str = "sdlc_indexing_position";

#[derive(Debug, Error)]
pub(super) enum IndexingPositionError {
    #[error("position store operation failed: {0}")]
    Store(String),
}

/// Where a pipeline left off: both time-position (watermark) and page-position (cursor).
///
/// The state machine is:
///
/// - No entry: first run, start from epoch, no cursor
/// - `cursor_values: None`: completed, `watermark` becomes the next `last_watermark`
/// - `cursor_values: Some(...)`: interrupted mid-pagination, resume from cursor
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(super) struct IndexingPosition {
    pub watermark: DateTime<Utc>,
    pub cursor_values: Option<Vec<String>>,
}

#[async_trait]
pub(super) trait IndexingPositionStore: Send + Sync {
    /// Load position by opaque key. Returns None if never indexed.
    async fn load(&self, key: &str) -> Result<Option<IndexingPosition>, IndexingPositionError>;

    /// Save cursor progress mid-pagination.
    async fn save_progress(
        &self,
        key: &str,
        position: &IndexingPosition,
    ) -> Result<(), IndexingPositionError>;

    /// Mark watermark as completed (cursor_values becomes None).
    async fn save_completed(
        &self,
        key: &str,
        watermark: &DateTime<Utc>,
    ) -> Result<(), IndexingPositionError>;
}

pub(super) struct ClickHousePositionStore {
    client: Arc<ArrowClickHouseClient>,
}

impl ClickHousePositionStore {
    pub fn new(client: Arc<ArrowClickHouseClient>) -> Self {
        Self { client }
    }

    async fn upsert(
        &self,
        key: &str,
        watermark: &DateTime<Utc>,
        cursor_values: &Option<Vec<String>>,
    ) -> Result<(), IndexingPositionError> {
        let formatted_watermark = watermark.format(TIMESTAMP_FORMAT).to_string();
        let cursor_json = serde_json::to_string(cursor_values)
            .map_err(|err| IndexingPositionError::Store(err.to_string()))?;

        self.client
            .query(
                "INSERT INTO sdlc_indexing_position (key, watermark, cursor_values) \
                 VALUES ({key:String}, {watermark:String}, {cursor_values:String})",
            )
            .param("key", key)
            .param("watermark", formatted_watermark)
            .param("cursor_values", cursor_json)
            .execute()
            .await
            .map_err(|err| IndexingPositionError::Store(err.to_string()))?;

        Ok(())
    }
}

#[async_trait]
impl IndexingPositionStore for ClickHousePositionStore {
    async fn load(&self, key: &str) -> Result<Option<IndexingPosition>, IndexingPositionError> {
        let batches = self
            .client
            .query(
                "SELECT argMax(watermark, _version) AS watermark, \
                        argMax(cursor_values, _version) AS cursor_values \
                 FROM sdlc_indexing_position \
                 WHERE key = {key:String}",
            )
            .param("key", key)
            .fetch_arrow()
            .await
            .map_err(|err| IndexingPositionError::Store(err.to_string()))?;

        let batch = match batches.into_iter().next() {
            Some(batch) if batch.num_rows() > 0 => batch,
            _ => return Ok(None),
        };

        let timestamps = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| IndexingPositionError::Store("invalid watermark type".to_string()))?;

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
            .ok_or_else(|| IndexingPositionError::Store("invalid timestamp".to_string()))?;

        let cursor_json = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .and_then(|arr| {
                if arr.is_null(0) || arr.value(0).is_empty() {
                    None
                } else {
                    Some(arr.value(0).to_string())
                }
            });

        let cursor_values: Option<Vec<String>> = match cursor_json {
            Some(json) => serde_json::from_str(&json)
                .map_err(|err| IndexingPositionError::Store(err.to_string()))?,
            None => None,
        };

        Ok(Some(IndexingPosition {
            watermark,
            cursor_values,
        }))
    }

    async fn save_progress(
        &self,
        key: &str,
        position: &IndexingPosition,
    ) -> Result<(), IndexingPositionError> {
        self.upsert(key, &position.watermark, &position.cursor_values)
            .await
    }

    async fn save_completed(
        &self,
        key: &str,
        watermark: &DateTime<Utc>,
    ) -> Result<(), IndexingPositionError> {
        self.upsert(key, watermark, &None).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn position_serialization_roundtrip_completed() {
        let position = IndexingPosition {
            watermark: "2024-06-15T12:00:00Z".parse().unwrap(),
            cursor_values: None,
        };

        let json = serde_json::to_string(&position).unwrap();
        let deserialized: IndexingPosition = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized, position);
        assert!(deserialized.cursor_values.is_none());
    }

    #[test]
    fn position_serialization_roundtrip_in_progress() {
        let position = IndexingPosition {
            watermark: "2024-06-15T12:00:00Z".parse().unwrap(),
            cursor_values: Some(vec!["1/2/".to_string(), "42".to_string()]),
        };

        let json = serde_json::to_string(&position).unwrap();
        let deserialized: IndexingPosition = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized, position);
        assert_eq!(deserialized.cursor_values.unwrap(), vec!["1/2/", "42"]);
    }
}
