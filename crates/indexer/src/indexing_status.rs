use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use nats_client::KvPutOptions;
use orbit_utils::traversal_path::TraversalPath;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;
use tracing::warn;

pub const INDEXING_PROGRESS_BUCKET: &str = "orbit_indexing_progress";
mod backfill;
pub use backfill::{InitialBackfillState, NamespaceBackfill};
const KEY_PREFIX: &str = "status";
const STATUS_UPDATE_ATTEMPTS: usize = 3;

#[derive(Debug, Error)]
pub enum Error {
    #[error("traversal path is empty")]
    EmptyTraversalPath,

    #[error("NATS KV operation failed: {0}")]
    Nats(#[from] nats_client::NatsError),

    #[error("failed to deserialize indexing progress: {0}")]
    Deserialize(#[from] serde_json::Error),

    #[error("indexing status changed concurrently")]
    ConcurrentUpdate,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
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

pub struct IndexingStatusStore {
    kv: Arc<dyn nats_client::KvServices>,
}

impl IndexingStatusStore {
    pub fn new(kv: Arc<dyn nats_client::KvServices>) -> Self {
        Self { kv }
    }

    pub async fn record_start(&self, path: &TraversalPath, started_at: DateTime<Utc>) {
        self.record(path, None, |progress| {
            progress.last_started_at = progress.last_started_at.max(started_at)
        })
        .await;
    }

    pub async fn record_entity_start(
        &self,
        path: &TraversalPath,
        entity: &str,
        started_at: DateTime<Utc>,
    ) {
        self.record(path, Some(entity), |progress| {
            progress.last_started_at = progress.last_started_at.max(started_at)
        })
        .await;
    }

    pub async fn record_completion(
        &self,
        path: &TraversalPath,
        started_at: DateTime<Utc>,
        completed_at: DateTime<Utc>,
        error: Option<String>,
        rows: RunRows,
    ) {
        self.record_run_completion(
            path,
            None,
            completed_progress(started_at, completed_at, error, rows),
        )
        .await;
    }

    pub async fn record_entity_completion(
        &self,
        path: &TraversalPath,
        entity: &str,
        started_at: DateTime<Utc>,
        completed_at: DateTime<Utc>,
        error: Option<String>,
        rows: RunRows,
    ) {
        self.record_run_completion(
            path,
            Some(entity),
            completed_progress(started_at, completed_at, error, rows),
        )
        .await;
    }

    async fn record_run_completion(
        &self,
        path: &TraversalPath,
        entity: Option<&str>,
        next: IndexingProgress,
    ) {
        self.record(path, entity, |progress| {
            if next.last_started_at >= progress.last_started_at {
                *progress = next.clone();
            }
        })
        .await;
        if next.last_error.is_some() {
            self.record_backfill_error(path, *orbit_migrations::version::SCHEMA_VERSION)
                .await;
        }
    }

    async fn record(
        &self,
        path: &TraversalPath,
        entity: Option<&str>,
        update: impl Fn(&mut IndexingProgress),
    ) {
        let result = async {
            let key = match entity {
                Some(entity) => entity_key(path, entity)?,
                None => normalize_key(path)?,
            };
            self.update_key(&key, Some(IndexingProgress::default()), update)
                .await
        }
        .await;
        if let Err(error) = result {
            warn!(%path, entity, %error, "failed to record indexing progress");
        }
    }

    async fn update_key<T>(
        &self,
        key: &str,
        initial: Option<T>,
        update: impl Fn(&mut T),
    ) -> Result<Option<T>, Error>
    where
        T: Clone + PartialEq + Serialize + DeserializeOwned,
    {
        for _ in 0..STATUS_UPDATE_ATTEMPTS {
            let entry = self.kv.kv_get(INDEXING_PROGRESS_BUCKET, key).await?;
            let (mut current, options) = match entry {
                Some(entry) => (
                    serde_json::from_slice::<T>(&entry.value)?,
                    KvPutOptions::update_revision(entry.revision),
                ),
                None => match initial.clone() {
                    Some(initial) => (initial, KvPutOptions::create_only()),
                    None => return Ok(None),
                },
            };
            let previous = current.clone();
            update(&mut current);
            if !options.create_only && current == previous {
                return Ok(Some(current));
            }
            let payload = Bytes::from(serde_json::to_vec(&current)?);
            if self
                .kv
                .kv_put(INDEXING_PROGRESS_BUCKET, key, payload, options)
                .await?
                .is_success()
            {
                return Ok(Some(current));
            }
        }
        Err(Error::ConcurrentUpdate)
    }

    pub async fn get(&self, path: &TraversalPath) -> Result<Option<IndexingProgress>, Error> {
        self.read_key(&normalize_key(path)?).await
    }

    pub async fn get_entity(
        &self,
        path: &TraversalPath,
        entity: &str,
    ) -> Result<Option<IndexingProgress>, Error> {
        self.read_key(&entity_key(path, entity)?).await
    }

    async fn read_key<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>, Error> {
        self.kv
            .kv_get(INDEXING_PROGRESS_BUCKET, key)
            .await?
            .map(|entry| serde_json::from_slice(&entry.value))
            .transpose()
            .map_err(Error::from)
    }

    pub async fn forget_namespace(&self, path: &TraversalPath) -> Result<(), Error> {
        let suffix = path.to_dotted();
        let prefix = format!("status.{suffix}");
        for key in self.kv.kv_keys(INDEXING_PROGRESS_BUCKET).await? {
            if key == prefix || key.starts_with(&format!("{prefix}.")) {
                self.kv.kv_delete(INDEXING_PROGRESS_BUCKET, &key).await?;
            }
        }
        if path.root_prefix().as_ref() == Some(path) {
            self.kv
                .kv_delete(INDEXING_PROGRESS_BUCKET, &backfill::namespace_key(path)?)
                .await?;
        }
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
fn normalize_key(traversal_path: &TraversalPath) -> Result<String, Error> {
    let dotted = traversal_path.to_dotted();
    if dotted.is_empty() {
        return Err(Error::EmptyTraversalPath);
    }
    Ok(format!("{KEY_PREFIX}.{dotted}"))
}

/// `("42/9970/", "MergeRequest")` → `"status.42.9970.MergeRequest"`.
fn entity_key(traversal_path: &TraversalPath, entity_kind: &str) -> Result<String, Error> {
    let base = normalize_key(traversal_path)?;
    Ok(format!("{base}.{entity_kind}"))
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
            assert_eq!(
                normalize_key(&TraversalPath::new_unchecked(input)).unwrap(),
                expected,
                "input: {input:?}"
            );
        }

        for empty in ["", "/", "//"] {
            assert!(
                matches!(
                    normalize_key(&TraversalPath::new_unchecked(empty)),
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
