use crate::schema::version::SCHEMA_VERSION;

pub fn versioned_stream(base: &str) -> String {
    format!("GKG_{}_{base}", *SCHEMA_VERSION)
}

pub fn versioned_bucket(base: &str) -> String {
    format!("GKG_{}_{base}", *SCHEMA_VERSION)
}

pub fn versioned_subject(base: &str) -> String {
    format!("v{}.{base}", *SCHEMA_VERSION)
}

pub fn versioned_consumer(base: &str) -> String {
    format!("v{}-{base}", *SCHEMA_VERSION)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_name_includes_version_and_base() {
        let v = *SCHEMA_VERSION;
        assert_eq!(versioned_stream("INDEXER"), format!("GKG_{v}_INDEXER"));
        assert_eq!(
            versioned_stream("DEAD_LETTERS"),
            format!("GKG_{v}_DEAD_LETTERS")
        );
    }

    #[test]
    fn bucket_name_includes_version_and_base() {
        let v = *SCHEMA_VERSION;
        assert_eq!(
            versioned_bucket("indexing_locks"),
            format!("GKG_{v}_indexing_locks")
        );
        assert_eq!(
            versioned_bucket("orbit_indexing_progress"),
            format!("GKG_{v}_orbit_indexing_progress")
        );
    }

    #[test]
    fn subject_prefixes_with_version() {
        let v = *SCHEMA_VERSION;
        assert_eq!(
            versioned_subject("sdlc.global.indexing.requested"),
            format!("v{v}.sdlc.global.indexing.requested")
        );
        assert_eq!(versioned_subject("dlq.>"), format!("v{v}.dlq.>"));
    }

    #[test]
    fn consumer_prefixes_with_version() {
        let v = *SCHEMA_VERSION;
        assert_eq!(
            versioned_consumer("dispatch-sdlc-global-indexing-requested"),
            format!("v{v}-dispatch-sdlc-global-indexing-requested")
        );
    }
}
