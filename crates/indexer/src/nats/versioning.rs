use std::sync::LazyLock;

use async_nats::jetstream::ErrorCode;
use async_nats::jetstream::context::DeleteStreamErrorKind;
use tracing::{debug, info, warn};

use crate::dead_letter::DEAD_LETTER_STREAM;
use crate::indexing_status::INDEXING_PROGRESS_BUCKET;
use crate::locking::INDEXING_LOCKS_BUCKET;
use crate::schema::version::SCHEMA_VERSION;
use crate::topic::INDEXER_STREAM;
use crate::types::Subscription;

pub static NATS_VERSIONER: LazyLock<NatsVersioner> =
    LazyLock::new(|| NatsVersioner::new(*SCHEMA_VERSION));

const MANAGED_STREAMS: &[&str] = &[INDEXER_STREAM, DEAD_LETTER_STREAM];
const MANAGED_BUCKETS: &[&str] = &[INDEXING_LOCKS_BUCKET, INDEXING_PROGRESS_BUCKET];

pub struct NatsVersioner {
    version: u32,
}

impl NatsVersioner {
    pub fn new(version: u32) -> Self {
        Self { version }
    }

    pub fn stream(&self, base: &str) -> String {
        format!("{base}_V{}", self.version)
    }

    pub fn bucket(&self, base: &str) -> String {
        format!("{base}_v{}", self.version)
    }

    pub fn subject(&self, base: &str) -> String {
        format!("v{}.{base}", self.version)
    }

    pub fn tag(&self) -> String {
        format!("v{}", self.version)
    }

    pub fn resolve_stream_and_subject(&self, subscription: &Subscription) -> (String, String) {
        if subscription.manage_stream {
            (
                self.stream(&subscription.stream),
                self.subject(&subscription.subject),
            )
        } else {
            (
                subscription.stream.to_string(),
                subscription.subject.to_string(),
            )
        }
    }
}

pub async fn cleanup_version(
    nats_client: &async_nats::Client,
    version: u32,
) -> Result<(), CleanupError> {
    let v = NatsVersioner::new(version);
    let jetstream = async_nats::jetstream::new(nats_client.clone());
    let mut errors: Vec<String> = Vec::new();

    for base in MANAGED_STREAMS {
        let name = v.stream(base);
        match jetstream.delete_stream(&name).await {
            Ok(_) => info!(version, stream = %name, "deleted versioned stream"),
            Err(e)
                if matches!(
                    e.kind(),
                    DeleteStreamErrorKind::JetStream(js_err)
                        if js_err.kind() == ErrorCode::STREAM_NOT_FOUND
                ) =>
            {
                debug!(version, stream = %name, "versioned stream does not exist, skipping");
            }
            Err(e) => {
                warn!(version, stream = %name, error = %e, "failed to delete versioned stream");
                errors.push(format!("stream {name}: {e}"));
            }
        }
    }

    for base in MANAGED_BUCKETS {
        // KV buckets are stored as JetStream streams with a KV_ prefix (NATS convention).
        // We delete the underlying stream directly to reuse the not-found error handling.
        let name = format!("KV_{}", v.bucket(base));
        match jetstream.delete_stream(&name).await {
            Ok(_) => info!(version, bucket = %name, "deleted versioned KV bucket"),
            Err(e)
                if matches!(
                    e.kind(),
                    DeleteStreamErrorKind::JetStream(js_err)
                        if js_err.kind() == ErrorCode::STREAM_NOT_FOUND
                ) =>
            {
                debug!(version, bucket = %name, "versioned KV bucket does not exist, skipping");
            }
            Err(e) => {
                warn!(version, bucket = %name, error = %e, "failed to delete versioned KV bucket");
                errors.push(format!("bucket {name}: {e}"));
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(CleanupError(errors))
    }
}

#[derive(Debug)]
pub struct CleanupError(Vec<String>);

impl std::fmt::Display for CleanupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "failed to clean up NATS entities: {}", self.0.join(", "))
    }
}

impl std::error::Error for CleanupError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Subscription;

    fn check_versioner(version: u32) {
        let v = NatsVersioner::new(version);

        assert_eq!(v.stream("GKG_INDEXER"), format!("GKG_INDEXER_V{version}"));
        assert_eq!(
            v.stream("GKG_DEAD_LETTERS"),
            format!("GKG_DEAD_LETTERS_V{version}")
        );

        assert_eq!(
            v.bucket("indexing_locks"),
            format!("indexing_locks_v{version}")
        );
        assert_eq!(
            v.bucket("orbit_indexing_progress"),
            format!("orbit_indexing_progress_v{version}")
        );

        assert_eq!(
            v.subject("sdlc.global.indexing.requested"),
            format!("v{version}.sdlc.global.indexing.requested")
        );
        assert_eq!(
            v.subject("code.task.indexing.requested.278964.bWFzdGVy"),
            format!("v{version}.code.task.indexing.requested.278964.bWFzdGVy")
        );
        assert_eq!(v.subject("dlq.>"), format!("v{version}.dlq.>"));

        assert_eq!(v.tag(), format!("v{version}"));
    }

    #[test]
    fn versioner_formats_all_entity_types() {
        check_versioner(67);
        check_versioner(69);
    }

    #[test]
    fn global_versioner_uses_schema_version() {
        let v = *SCHEMA_VERSION;
        check_versioner(v);

        assert_eq!(
            NATS_VERSIONER.stream("GKG_INDEXER"),
            format!("GKG_INDEXER_V{v}")
        );
    }

    #[test]
    fn managed_streams_contains_all_stream_constants() {
        let required = [INDEXER_STREAM, DEAD_LETTER_STREAM];
        for stream in &required {
            assert!(
                MANAGED_STREAMS.contains(stream),
                "MANAGED_STREAMS is missing {stream:?} — add it so cleanup_version deletes it"
            );
        }
    }

    #[test]
    fn managed_buckets_contains_all_bucket_constants() {
        let required = [INDEXING_LOCKS_BUCKET, INDEXING_PROGRESS_BUCKET];
        for bucket in &required {
            assert!(
                MANAGED_BUCKETS.contains(bucket),
                "MANAGED_BUCKETS is missing {bucket:?} — add it so cleanup_version deletes it"
            );
        }
    }

    #[test]
    fn resolve_stream_and_subject_versions_managed_subscriptions() {
        let v = NatsVersioner::new(69);
        let subscription = Subscription::new("GKG_INDEXER", "sdlc.global.indexing.requested");

        let (stream, subject) = v.resolve_stream_and_subject(&subscription);

        assert_eq!(stream, "GKG_INDEXER_V69");
        assert_eq!(subject, "v69.sdlc.global.indexing.requested");
    }

    #[test]
    fn resolve_stream_and_subject_preserves_unmanaged_subscriptions() {
        let v = NatsVersioner::new(69);
        let mut subscription = Subscription::new("siphon_db", "tables.merge_requests");
        subscription.manage_stream = false;

        let (stream, subject) = v.resolve_stream_and_subject(&subscription);

        assert_eq!(stream, "siphon_db");
        assert_eq!(subject, "tables.merge_requests");
    }
}
