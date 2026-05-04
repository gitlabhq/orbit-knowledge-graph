use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use nats_client::KvPutOptions;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;

pub const INDEXING_PROGRESS_BUCKET: &str = "orbit_indexing_progress";
const KEY_PREFIX: &str = "status";

#[derive(Debug, Error)]
pub enum Error {
    #[error("traversal path is empty")]
    EmptyTraversalPath,

    #[error("NATS KV operation failed: {0}")]
    Nats(#[from] nats_client::NatsError),

    #[error("failed to deserialize indexing progress: {0}")]
    Deserialize(#[from] serde_json::Error),
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
}

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
    pub async fn record_start(&self, traversal_path: &str, started_at: DateTime<Utc>) {
        let previous = self.get(traversal_path).await.unwrap_or_else(|error| {
            warn!(traversal_path, %error, "failed to read previous progress; starting from scratch");
            None
        });
        let progress = match previous {
            Some(mut prev) => {
                prev.last_started_at = started_at;
                prev
            }
            None => IndexingProgress {
                last_started_at: started_at,
                last_completed_at: None,
                last_duration_ms: None,
                last_error: None,
            },
        };
        self.write(traversal_path, progress).await;
    }

    pub async fn record_completion(
        &self,
        traversal_path: &str,
        started_at: DateTime<Utc>,
        completed_at: DateTime<Utc>,
        error: Option<String>,
    ) {
        let duration_ms = completed_at
            .signed_duration_since(started_at)
            .num_milliseconds()
            .max(0) as u64;
        self.write(
            traversal_path,
            IndexingProgress {
                last_started_at: started_at,
                last_completed_at: Some(completed_at),
                last_duration_ms: Some(duration_ms),
                last_error: error,
            },
        )
        .await;
    }

    pub async fn get(&self, traversal_path: &str) -> Result<Option<IndexingProgress>, Error> {
        let key = normalize_key(traversal_path)?;
        let Some(entry) = self.kv.kv_get(INDEXING_PROGRESS_BUCKET, &key).await? else {
            return Ok(None);
        };
        let progress = serde_json::from_slice::<IndexingProgress>(&entry.value)?;
        Ok(Some(progress))
    }

    async fn write(&self, traversal_path: &str, progress: IndexingProgress) {
        let key = match normalize_key(traversal_path) {
            Ok(key) => key,
            Err(error) => {
                warn!(traversal_path, %error, "skipping indexing status record");
                return;
            }
        };

        let payload = match serde_json::to_vec(&progress) {
            Ok(bytes) => Bytes::from(bytes),
            Err(error) => {
                warn!(%error, key, "failed to serialize indexing progress");
                return;
            }
        };

        if let Err(error) = self
            .kv
            .kv_put(
                INDEXING_PROGRESS_BUCKET,
                &key,
                payload,
                KvPutOptions::default(),
            )
            .await
        {
            warn!(%error, key, "failed to write indexing progress");
        }
    }
}

/// `"42/9970/12345/"` → `"status.42.9970.12345"`.
fn normalize_key(traversal_path: &str) -> Result<String, Error> {
    let dotted = gkg_utils::traversal_path::to_dotted(traversal_path);
    if dotted.is_empty() {
        return Err(Error::EmptyTraversalPath);
    }
    Ok(format!("{KEY_PREFIX}.{dotted}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_key_formats_paths() {
        let cases = [
            ("42/9970/", "status.42.9970"),
            ("42/9970/12345/", "status.42.9970.12345"),
            ("42/9970", "status.42.9970"),
            ("42//9970", "status.42.9970"),
        ];
        for (input, expected) in cases {
            assert_eq!(normalize_key(input).unwrap(), expected, "input: {input:?}");
        }

        for empty in ["", "/", "//"] {
            assert!(
                matches!(normalize_key(empty), Err(Error::EmptyTraversalPath)),
                "input: {empty:?}"
            );
        }
    }

    #[test]
    fn progress_omits_completion_fields_when_absent() {
        let progress = IndexingProgress {
            last_started_at: Utc::now(),
            last_completed_at: None,
            last_duration_ms: None,
            last_error: None,
        };
        let json: serde_json::Value = serde_json::to_value(&progress).unwrap();
        assert!(json.get("last_completed_at").is_none());
        assert!(json.get("last_duration_ms").is_none());
        assert!(json.get("last_error").is_none());
    }

    #[test]
    fn completion_serializes_success_and_failure() {
        let started_at = Utc::now();
        let success = IndexingProgress {
            last_started_at: started_at,
            last_completed_at: Some(started_at + chrono::Duration::milliseconds(300)),
            last_duration_ms: Some(300),
            last_error: None,
        };
        let json = serde_json::to_value(&success).unwrap();
        assert_eq!(json["last_duration_ms"], 300);
        assert!(json.get("last_error").is_none());

        let failure = IndexingProgress {
            last_started_at: started_at,
            last_completed_at: Some(started_at),
            last_duration_ms: Some(0),
            last_error: Some("deadline exceeded".to_string()),
        };
        let json = serde_json::to_value(&failure).unwrap();
        assert_eq!(json["last_error"], "deadline exceeded");
    }
}
