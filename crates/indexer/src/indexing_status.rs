use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use nats_client::KvPutOptions;
use orbit_utils::traversal_path::TraversalPath;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;

mod backfill;

pub use backfill::{InitialBackfill, InitialBackfillState};

pub const INDEXING_PROGRESS_BUCKET: &str = "orbit_indexing_progress";
const KEY_PREFIX: &str = "status";

#[derive(Debug, Error)]
pub enum Error {
    #[error("traversal path is empty")]
    EmptyTraversalPath,

    #[error("NATS KV operation failed: {0}")]
    Nats(#[from] nats_client::NatsError),

    #[error("failed to (de)serialize indexing status: {0}")]
    Serde(#[from] serde_json::Error),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexingProgress {
    pub last_started_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_completed_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_rows_read: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_rows_written: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RunRows {
    pub read: Option<u64>,
    pub written: Option<u64>,
}

#[derive(Clone)]
pub struct IndexingStatusStore {
    kv: Arc<dyn nats_client::KvServices>,
}

impl IndexingStatusStore {
    pub fn new(kv: Arc<dyn nats_client::KvServices>) -> Self {
        Self { kv }
    }

    /// Read-modify-write — a concurrent call on the same path could lose the
    /// previous completion fields. Safe here because NATS message deduping and
    /// per-path locks already serialize runs for a given traversal path.
    pub async fn record_start(&self, traversal_path: &TraversalPath, started_at: DateTime<Utc>) {
        self.record_started(progress_key(traversal_path), started_at)
            .await;
    }

    pub async fn record_entity_start(
        &self,
        traversal_path: &TraversalPath,
        entity_kind: &str,
        started_at: DateTime<Utc>,
    ) {
        self.record_started(entity_key(traversal_path, entity_kind), started_at)
            .await;
    }

    pub async fn record_completion(
        &self,
        traversal_path: &TraversalPath,
        started_at: DateTime<Utc>,
        completed_at: DateTime<Utc>,
        error: Option<String>,
        rows: RunRows,
    ) {
        self.record_progress(
            progress_key(traversal_path),
            completed_progress(started_at, completed_at, error, rows),
        )
        .await;
    }

    pub async fn record_entity_completion(
        &self,
        traversal_path: &TraversalPath,
        entity_kind: &str,
        started_at: DateTime<Utc>,
        completed_at: DateTime<Utc>,
        error: Option<String>,
        rows: RunRows,
    ) {
        self.record_progress(
            entity_key(traversal_path, entity_kind),
            completed_progress(started_at, completed_at, error, rows),
        )
        .await;
    }

    pub async fn get(
        &self,
        traversal_path: &TraversalPath,
    ) -> Result<Option<IndexingProgress>, Error> {
        self.read_key(&progress_key(traversal_path)?).await
    }

    pub async fn get_entity(
        &self,
        traversal_path: &TraversalPath,
        entity_kind: &str,
    ) -> Result<Option<IndexingProgress>, Error> {
        self.read_key(&entity_key(traversal_path, entity_kind)?)
            .await
    }

    pub async fn forget_namespace(&self, traversal_path: &TraversalPath) -> Result<(), Error> {
        let prefix = progress_key(traversal_path)?;
        for key in self.kv.kv_keys(INDEXING_PROGRESS_BUCKET).await? {
            if key == prefix || key.starts_with(&format!("{prefix}.")) {
                self.kv.kv_delete(INDEXING_PROGRESS_BUCKET, &key).await?;
            }
        }
        if traversal_path.root_prefix().as_ref() == Some(traversal_path) {
            self.kv
                .kv_delete(
                    INDEXING_PROGRESS_BUCKET,
                    &backfill::initial_backfill_key(traversal_path)?,
                )
                .await?;
        }
        Ok(())
    }

    async fn record_started(&self, key: Result<String, Error>, started_at: DateTime<Utc>) {
        let key = match key {
            Ok(key) => key,
            Err(error) => {
                warn!(%error, "skipping indexing status record");
                return;
            }
        };
        let previous: Option<IndexingProgress> =
            self.read_key(&key).await.unwrap_or_else(|error| {
                warn!(key, %error, "failed to read previous progress; starting from scratch");
                None
            });
        let progress = match previous {
            Some(mut previous) => {
                previous.last_started_at = started_at;
                previous
            }
            None => IndexingProgress {
                last_started_at: started_at,
                last_completed_at: None,
                last_duration_ms: None,
                last_error: None,
                last_rows_read: None,
                last_rows_written: None,
            },
        };
        self.record_progress(Ok(key), progress).await;
    }

    async fn record_progress(&self, key: Result<String, Error>, progress: IndexingProgress) {
        let written = match key {
            Ok(key) => self.write_key(&key, &progress).await,
            Err(error) => Err(error),
        };
        if let Err(error) = written {
            warn!(%error, "failed to write indexing progress");
        }
    }

    async fn read_key<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, Error> {
        self.kv
            .kv_get(INDEXING_PROGRESS_BUCKET, key)
            .await?
            .map(|entry| serde_json::from_slice(&entry.value))
            .transpose()
            .map_err(Error::from)
    }

    async fn write_key<T: Serialize>(&self, key: &str, value: &T) -> Result<(), Error> {
        let payload = Bytes::from(serde_json::to_vec(value)?);
        self.kv
            .kv_put(
                INDEXING_PROGRESS_BUCKET,
                key,
                payload,
                KvPutOptions::default(),
            )
            .await?;
        Ok(())
    }
}

fn completed_progress(
    started_at: DateTime<Utc>,
    completed_at: DateTime<Utc>,
    error: Option<String>,
    rows: RunRows,
) -> IndexingProgress {
    let duration_ms = completed_at
        .signed_duration_since(started_at)
        .num_milliseconds()
        .max(0) as u64;
    IndexingProgress {
        last_started_at: started_at,
        last_completed_at: Some(completed_at),
        last_duration_ms: Some(duration_ms),
        last_error: error,
        last_rows_read: rows.read,
        last_rows_written: rows.written,
    }
}

/// `"42/9970/12345/"` → `"status.42.9970.12345"`.
fn progress_key(traversal_path: &TraversalPath) -> Result<String, Error> {
    let dotted = traversal_path.to_dotted();
    if dotted.is_empty() {
        return Err(Error::EmptyTraversalPath);
    }
    Ok(format!("{KEY_PREFIX}.{dotted}"))
}

/// `("42/9970/", "MergeRequest")` → `"status.42.9970.MergeRequest"`.
fn entity_key(traversal_path: &TraversalPath, entity_kind: &str) -> Result<String, Error> {
    let base = progress_key(traversal_path)?;
    Ok(format!("{base}.{entity_kind}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_key_formats_paths() {
        let cases = [
            ("42/9970/", "status.42.9970"),
            ("42/9970/12345/", "status.42.9970.12345"),
            ("42/9970", "status.42.9970"),
            ("42//9970", "status.42.9970"),
        ];
        for (input, expected) in cases {
            assert_eq!(
                progress_key(&TraversalPath::new_unchecked(input)).unwrap(),
                expected,
                "input: {input:?}"
            );
        }

        for empty in ["", "/", "//"] {
            assert!(
                matches!(
                    progress_key(&TraversalPath::new_unchecked(empty)),
                    Err(Error::EmptyTraversalPath)
                ),
                "input: {empty:?}"
            );
        }
    }

    #[test]
    fn entity_key_appends_kind() {
        assert_eq!(
            entity_key(&TraversalPath::new_unchecked("42/9970/"), "MergeRequest").unwrap(),
            "status.42.9970.MergeRequest"
        );
        assert_eq!(
            entity_key(&TraversalPath::new_unchecked("42/9970/12345/"), "Issue").unwrap(),
            "status.42.9970.12345.Issue"
        );
        assert!(matches!(
            entity_key(&TraversalPath::new_unchecked(""), "MergeRequest"),
            Err(Error::EmptyTraversalPath)
        ));
    }

    #[test]
    fn progress_omits_completion_fields_when_absent() {
        let progress = IndexingProgress {
            last_started_at: Utc::now(),
            last_completed_at: None,
            last_duration_ms: None,
            last_error: None,
            last_rows_read: None,
            last_rows_written: None,
        };
        let json: serde_json::Value = serde_json::to_value(&progress).unwrap();
        assert!(json.get("last_completed_at").is_none());
        assert!(json.get("last_duration_ms").is_none());
        assert!(json.get("last_error").is_none());
        assert!(json.get("last_rows_read").is_none());
        assert!(json.get("last_rows_written").is_none());
    }

    #[test]
    fn completion_serializes_success_and_failure() {
        let started_at = Utc::now();
        let success = completed_progress(
            started_at,
            started_at + chrono::Duration::milliseconds(300),
            None,
            RunRows {
                read: Some(120),
                written: Some(80),
            },
        );
        let json = serde_json::to_value(&success).unwrap();
        assert_eq!(json["last_duration_ms"], 300);
        assert_eq!(json["last_rows_read"], 120);
        assert_eq!(json["last_rows_written"], 80);
        assert!(json.get("last_error").is_none());

        let failure = completed_progress(
            started_at,
            started_at,
            Some("deadline exceeded".to_string()),
            RunRows::default(),
        );
        let json = serde_json::to_value(&failure).unwrap();
        assert_eq!(json["last_error"], "deadline exceeded");
        assert!(json.get("last_rows_read").is_none());
        assert!(json.get("last_rows_written").is_none());
    }

    #[test]
    fn progress_deserializes_legacy_json_without_rows() {
        let progress: IndexingProgress = serde_json::from_str(
            r#"{"last_started_at":"2026-08-01T00:00:00Z","last_completed_at":"2026-08-01T00:00:05Z","last_duration_ms":5000}"#,
        )
        .unwrap();
        assert_eq!(progress.last_rows_read, None);
        assert_eq!(progress.last_rows_written, None);
    }
}
