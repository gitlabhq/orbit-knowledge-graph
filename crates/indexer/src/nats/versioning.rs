use std::sync::LazyLock;

use crate::schema::version::SCHEMA_VERSION;

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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_names_v67() {
        let v = NatsVersioner::new(67);
        assert_eq!(v.stream("GKG_INDEXER"), "GKG_INDEXER_V67");
        assert_eq!(v.stream("GKG_DEAD_LETTERS"), "GKG_DEAD_LETTERS_V67");
    }

    #[test]
    fn stream_names_v69() {
        let v = NatsVersioner::new(69);
        assert_eq!(v.stream("GKG_INDEXER"), "GKG_INDEXER_V69");
        assert_eq!(v.stream("GKG_DEAD_LETTERS"), "GKG_DEAD_LETTERS_V69");
    }

    #[test]
    fn bucket_names_v67() {
        let v = NatsVersioner::new(67);
        assert_eq!(v.bucket("indexing_locks"), "indexing_locks_v67");
        assert_eq!(
            v.bucket("orbit_indexing_progress"),
            "orbit_indexing_progress_v67"
        );
    }

    #[test]
    fn bucket_names_v69() {
        let v = NatsVersioner::new(69);
        assert_eq!(v.bucket("indexing_locks"), "indexing_locks_v69");
        assert_eq!(
            v.bucket("orbit_indexing_progress"),
            "orbit_indexing_progress_v69"
        );
    }

    #[test]
    fn subjects_v67() {
        let v = NatsVersioner::new(67);
        assert_eq!(
            v.subject("sdlc.global.indexing.requested"),
            "v67.sdlc.global.indexing.requested"
        );
        assert_eq!(
            v.subject("code.task.indexing.requested.278964.bWFzdGVy"),
            "v67.code.task.indexing.requested.278964.bWFzdGVy"
        );
        assert_eq!(v.subject("dlq.>"), "v67.dlq.>");
    }

    #[test]
    fn subjects_v69() {
        let v = NatsVersioner::new(69);
        assert_eq!(
            v.subject("sdlc.global.indexing.requested"),
            "v69.sdlc.global.indexing.requested"
        );
        assert_eq!(
            v.subject("code.task.indexing.requested.278964.bWFzdGVy"),
            "v69.code.task.indexing.requested.278964.bWFzdGVy"
        );
        assert_eq!(v.subject("dlq.>"), "v69.dlq.>");
    }

    #[test]
    fn tag_v67() {
        assert_eq!(NatsVersioner::new(67).tag(), "v67");
    }

    #[test]
    fn tag_v69() {
        assert_eq!(NatsVersioner::new(69).tag(), "v69");
    }

    #[test]
    fn global_versioner_uses_schema_version() {
        let v = *SCHEMA_VERSION;
        assert_eq!(
            NATS_VERSIONER.stream("GKG_INDEXER"),
            format!("GKG_INDEXER_V{v}")
        );
        assert_eq!(
            NATS_VERSIONER.bucket("indexing_locks"),
            format!("indexing_locks_v{v}")
        );
        assert_eq!(
            NATS_VERSIONER.subject("sdlc.global.indexing.requested"),
            format!("v{v}.sdlc.global.indexing.requested")
        );
        assert_eq!(NATS_VERSIONER.tag(), format!("v{v}"));
    }
}
