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

pub const MANAGED_STREAMS: &[&str] = &[INDEXER_STREAM, DEAD_LETTER_STREAM];

pub const MANAGED_BUCKETS: &[&str] = &[INDEXING_LOCKS_BUCKET, INDEXING_PROGRESS_BUCKET];

pub static NATS_VERSIONER: LazyLock<NatsVersioner> =
    LazyLock::new(|| NatsVersioner::new(*SCHEMA_VERSION));

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

fn versioned_entity_names(version: u32) -> Vec<String> {
    let versioner = NatsVersioner::new(version);
    let mut names: Vec<String> = MANAGED_STREAMS
        .iter()
        .map(|base| versioner.stream(base))
        .collect();
    names.extend(
        MANAGED_BUCKETS
            .iter()
            .map(|base| format!("KV_{}", versioner.bucket(base))),
    );
    names
}

pub async fn cleanup_version(
    nats_client: &async_nats::Client,
    version: u32,
) -> Result<(), CleanupError> {
    let jetstream = async_nats::jetstream::new(nats_client.clone());
    let mut errors: Vec<String> = Vec::new();

    for name in &versioned_entity_names(version) {
        match jetstream.delete_stream(name).await {
            Ok(_) => info!(version, stream = %name, "deleted versioned stream"),
            Err(e)
                if matches!(
                    e.kind(),
                    DeleteStreamErrorKind::JetStream(js_err)
                        if js_err.kind() == ErrorCode::STREAM_NOT_FOUND
                ) =>
            {
                debug!(version, stream = %name, "versioned stream already deleted, skipping");
            }
            Err(e) => {
                warn!(version, stream = %name, error = %e, "failed to delete versioned stream");
                errors.push(format!("stream {name}: {e}"));
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

    #[test]
    fn versioner_formats_all_entity_types() {
        for version in [67, 69] {
            let v = NatsVersioner::new(version);

            assert_eq!(v.stream("GKG_INDEXER"), format!("GKG_INDEXER_V{version}"));
            assert_eq!(
                v.bucket("indexing_locks"),
                format!("indexing_locks_v{version}")
            );
            assert_eq!(
                v.subject("sdlc.global.indexing.requested"),
                format!("v{version}.sdlc.global.indexing.requested")
            );
            assert_eq!(v.tag(), format!("v{version}"));
        }
    }

    #[test]
    fn global_versioner_uses_schema_version() {
        let v = *SCHEMA_VERSION;
        assert_eq!(
            NATS_VERSIONER.stream(INDEXER_STREAM),
            format!("{INDEXER_STREAM}_V{v}")
        );
    }

    #[test]
    fn versioned_entity_names_covers_all_owned_entities() {
        let names = versioned_entity_names(62);

        assert!(names.contains(&"GKG_INDEXER_V62".to_string()));
        assert!(names.contains(&"GKG_DEAD_LETTERS_V62".to_string()));
        assert!(names.contains(&"KV_indexing_locks_v62".to_string()));
        assert!(names.contains(&"KV_orbit_indexing_progress_v62".to_string()));
        assert_eq!(names.len(), MANAGED_STREAMS.len() + MANAGED_BUCKETS.len());
    }

    #[test]
    fn versioned_entity_names_excludes_foreign_entities() {
        let names = versioned_entity_names(54);

        assert!(!names.contains(&"OTHER_APP_V54".to_string()));
        assert!(!names.contains(&"KV_someone_else_v54".to_string()));
        assert!(!names.contains(&"siphon_db".to_string()));
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
