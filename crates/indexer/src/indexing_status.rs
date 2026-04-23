//! Records when each indexing run started, finished, and whether it failed.
//!
//! Writes land in a NATS JetStream KV bucket (`indexing_progress`) keyed by
//! `status.<dot-joined-traversal-path>`. The `GetGraphStatus` gRPC handler in
//! `gkg-server` reads from this bucket to populate its response.
//!
//! See `docs/design-documents/decisions/010_graph_status_endpoint.md` (Phase 2).

use std::path::PathBuf;

use async_nats::jetstream::kv::{Config as KvConfig, Store};
use chrono::{DateTime, Utc};
use gkg_server_config::NatsConfiguration;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;

const BUCKET_NAME: &str = "indexing_progress";
const KEY_PREFIX: &str = "status";

#[derive(Debug, Error)]
pub enum Error {
    #[error("NATS connection failed: {0}")]
    Connection(String),

    #[error("invalid TLS configuration: {0}")]
    TlsConfig(String),

    #[error("KV bucket operation failed: {0}")]
    Bucket(String),

    #[error("KV get failed: {0}")]
    Get(String),

    #[error("traversal path is empty")]
    EmptyTraversalPath,

    #[error("failed to deserialize indexing progress: {0}")]
    Deserialize(#[from] serde_json::Error),
}

/// Wall-clock bounds of a single indexing run, as observed by the handler.
#[derive(Debug, Clone)]
pub struct RunOutcome {
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    /// `None` means the run succeeded.
    pub error: Option<String>,
}

/// On-the-wire shape stored in the KV bucket. An empty `last_error` is the
/// success sentinel — matches the response shape described in ADR 010.
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

/// Handle to the `indexing_progress` NATS KV bucket.
///
/// Writes are fire-and-forget: `record()` logs and swallows errors so indexing
/// success is never gated on telemetry.
pub struct IndexingStatusStore {
    kv: Option<Store>,
}

impl IndexingStatusStore {
    /// Connects to NATS and ensures the `indexing_progress` bucket exists.
    /// Idempotent across pods via `create_or_update_key_value`.
    pub async fn connect(config: &NatsConfiguration) -> Result<Self, Error> {
        config.validate_tls_config().map_err(Error::TlsConfig)?;

        let options = build_connect_options(config);
        let client = async_nats::connect_with_options(config.connection_url(), options)
            .await
            .map_err(|error| Error::Connection(error.to_string()))?;

        let jetstream = async_nats::jetstream::new(client);
        let kv = jetstream
            .create_or_update_key_value(KvConfig {
                bucket: BUCKET_NAME.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .map_err(|error| Error::Bucket(error.to_string()))?;

        Ok(Self { kv: Some(kv) })
    }

    /// A store that drops every write and returns `None` on reads. For tests
    /// that exercise handler code without a NATS connection.
    pub fn noop() -> Self {
        Self { kv: None }
    }

    /// Writes the outcome for `traversal_path`. KV failures are logged, not
    /// propagated: telemetry must never fail an indexing run.
    pub async fn record(&self, traversal_path: &str, outcome: RunOutcome) {
        let Some(kv) = &self.kv else { return };

        let key = match normalize_key(traversal_path) {
            Ok(key) => key,
            Err(error) => {
                warn!(traversal_path, %error, "skipping indexing status record");
                return;
            }
        };

        let progress = IndexingProgress::from_outcome(&outcome);
        let payload = match serde_json::to_vec(&progress) {
            Ok(bytes) => bytes,
            Err(error) => {
                warn!(%error, key, "failed to serialize indexing progress");
                return;
            }
        };

        if let Err(error) = kv.put(&key, payload.into()).await {
            warn!(%error, key, "failed to write indexing progress");
        }
    }

    /// Reads the most recent progress entry for `traversal_path`, if any.
    pub async fn get(&self, traversal_path: &str) -> Result<Option<IndexingProgress>, Error> {
        let Some(kv) = &self.kv else { return Ok(None) };
        let key = normalize_key(traversal_path)?;
        let entry = kv
            .get(&key)
            .await
            .map_err(|error| Error::Get(error.to_string()))?;
        entry
            .map(|bytes| serde_json::from_slice::<IndexingProgress>(&bytes).map_err(Error::from))
            .transpose()
    }
}

/// Turns a GKG traversal path (`"42/9970/12345/"`) into a NATS KV key
/// (`"status.42.9970.12345"`). Empty segments are filtered defensively.
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

fn build_connect_options(config: &NatsConfiguration) -> async_nats::ConnectOptions {
    let mut options = async_nats::ConnectOptions::new()
        .connection_timeout(config.connection_timeout())
        .request_timeout(Some(config.request_timeout()));

    if let (Some(user), Some(pass)) = (&config.username, &config.password) {
        options = options.user_and_password(user.clone(), pass.clone());
    }

    if config.tls_enabled() {
        options = options.require_tls(true);
    }

    if let Some(ca_path) = &config.tls_ca_cert_path {
        options = options.add_root_certificates(PathBuf::from(ca_path));
    }

    if let (Some(cert), Some(key)) = (&config.tls_cert_path, &config.tls_key_path) {
        options = options.add_client_certificate(PathBuf::from(cert), PathBuf::from(key));
    }

    options
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
