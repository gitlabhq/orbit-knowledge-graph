use std::sync::LazyLock;

use tracing::{info, warn};

use crate::dead_letter::DEAD_LETTER_STREAM;
use crate::indexing_status::INDEXING_PROGRESS_BUCKET;
use crate::locking::INDEXING_LOCKS_BUCKET;
use crate::schema::version::SCHEMA_VERSION;
use crate::topic::INDEXER_STREAM;

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
}

pub async fn cleanup_version(nats_client: &async_nats::Client, version: u32) {
    let v = NatsVersioner::new(version);
    let jetstream = async_nats::jetstream::new(nats_client.clone());

    for base in MANAGED_STREAMS {
        let name = v.stream(base);
        match jetstream.delete_stream(&name).await {
            Ok(_) => info!(version, stream = %name, "deleted versioned stream"),
            Err(e) => {
                warn!(version, stream = %name, error = %e, "failed to delete versioned stream")
            }
        }
    }

    for base in MANAGED_BUCKETS {
        let name = v.bucket(base);
        match jetstream.delete_key_value(&name).await {
            Ok(_) => info!(version, bucket = %name, "deleted versioned KV bucket"),
            Err(e) => {
                warn!(version, bucket = %name, error = %e, "failed to delete versioned KV bucket")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn managed_streams_covers_all_gkg_streams() {
        assert_eq!(MANAGED_STREAMS, &["GKG_INDEXER", "GKG_DEAD_LETTERS"]);
    }

    #[test]
    fn managed_buckets_covers_all_gkg_buckets() {
        assert_eq!(
            MANAGED_BUCKETS,
            &["indexing_locks", "orbit_indexing_progress"]
        );
    }
}
