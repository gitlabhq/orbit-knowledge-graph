use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;

use crate::nats::{KvPutOptions, NatsServices};

pub const INDEXING_PROGRESS_BUCKET: &str = "indexing_progress";
const KEY_PREFIX: &str = "status";

#[derive(Debug, Error)]
pub enum Error {
    #[error("traversal path is empty")]
    EmptyTraversalPath,

    #[error("NATS KV operation failed: {0}")]
    Nats(#[from] crate::nats::NatsError),

    #[error("failed to deserialize indexing progress: {0}")]
    Deserialize(#[from] serde_json::Error),
}

#[derive(Debug, Clone)]
pub struct RunOutcome {
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexingProgress {
    pub last_started_at: DateTime<Utc>,
    pub last_completed_at: DateTime<Utc>,
    pub last_duration_ms: u64,
    pub last_error: String,
}

impl IndexingProgress {
    fn from_outcome(outcome: &RunOutcome) -> Self {
        let duration_ms = outcome
            .completed_at
            .signed_duration_since(outcome.started_at)
            .num_milliseconds()
            .max(0) as u64;
        Self {
            last_started_at: outcome.started_at,
            last_completed_at: outcome.completed_at,
            last_duration_ms: duration_ms,
            last_error: outcome.error.clone().unwrap_or_default(),
        }
    }
}

pub struct IndexingStatusStore {
    nats: Arc<dyn NatsServices>,
}

impl IndexingStatusStore {
    pub fn new(nats: Arc<dyn NatsServices>) -> Self {
        Self { nats }
    }

    pub async fn record(&self, traversal_path: &str, outcome: RunOutcome) {
        let key = match normalize_key(traversal_path) {
            Ok(key) => key,
            Err(error) => {
                warn!(traversal_path, %error, "skipping indexing status record");
                return;
            }
        };

        let progress = IndexingProgress::from_outcome(&outcome);
        let payload = match serde_json::to_vec(&progress) {
            Ok(bytes) => Bytes::from(bytes),
            Err(error) => {
                warn!(%error, key, "failed to serialize indexing progress");
                return;
            }
        };

        if let Err(error) = self
            .nats
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

    pub async fn get(&self, traversal_path: &str) -> Result<Option<IndexingProgress>, Error> {
        let key = normalize_key(traversal_path)?;
        let Some(entry) = self.nats.kv_get(INDEXING_PROGRESS_BUCKET, &key).await? else {
            return Ok(None);
        };
        let progress = serde_json::from_slice::<IndexingProgress>(&entry.value)?;
        Ok(Some(progress))
    }
}

/// `"42/9970/12345/"` → `"status.42.9970.12345"`.
fn normalize_key(traversal_path: &str) -> Result<String, Error> {
    let joined = traversal_path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join(".");
    if joined.is_empty() {
        return Err(Error::EmptyTraversalPath);
    }
    Ok(format!("{KEY_PREFIX}.{joined}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_group_path() {
        assert_eq!(normalize_key("42/9970/").unwrap(), "status.42.9970");
    }

    #[test]
    fn normalize_project_path() {
        assert_eq!(
            normalize_key("42/9970/12345/").unwrap(),
            "status.42.9970.12345"
        );
    }

    #[test]
    fn normalize_collapses_empty_segments() {
        assert_eq!(normalize_key("42//9970").unwrap(), "status.42.9970");
    }

    #[test]
    fn normalize_handles_missing_trailing_slash() {
        assert_eq!(normalize_key("42/9970").unwrap(), "status.42.9970");
    }

    #[test]
    fn normalize_rejects_empty_paths() {
        assert!(matches!(normalize_key(""), Err(Error::EmptyTraversalPath)));
        assert!(matches!(normalize_key("/"), Err(Error::EmptyTraversalPath)));
        assert!(matches!(
            normalize_key("//"),
            Err(Error::EmptyTraversalPath)
        ));
    }

    #[test]
    fn progress_represents_success_as_empty_error() {
        let started_at = Utc::now();
        let progress = IndexingProgress::from_outcome(&RunOutcome {
            started_at,
            completed_at: started_at + chrono::Duration::milliseconds(300),
            error: None,
        });
        assert_eq!(progress.last_duration_ms, 300);
        assert_eq!(progress.last_error, "");
    }

    #[test]
    fn progress_preserves_failure_message() {
        let now = Utc::now();
        let progress = IndexingProgress::from_outcome(&RunOutcome {
            started_at: now,
            completed_at: now,
            error: Some("deadline exceeded".to_string()),
        });
        assert_eq!(progress.last_error, "deadline exceeded");
        assert_eq!(progress.last_duration_ms, 0);
    }

    #[test]
    fn progress_clamps_negative_duration_to_zero() {
        let completed_at = Utc::now();
        let progress = IndexingProgress::from_outcome(&RunOutcome {
            started_at: completed_at + chrono::Duration::seconds(1),
            completed_at,
            error: None,
        });
        assert_eq!(progress.last_duration_ms, 0);
    }

    #[test]
    fn progress_roundtrips_through_json() {
        let started_at = Utc::now();
        let progress = IndexingProgress::from_outcome(&RunOutcome {
            started_at,
            completed_at: started_at + chrono::Duration::seconds(5),
            error: Some("boom".to_string()),
        });
        let bytes = serde_json::to_vec(&progress).unwrap();
        let roundtripped: IndexingProgress = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(roundtripped, progress);
    }
}
