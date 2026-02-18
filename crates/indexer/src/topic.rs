use crate::types::{Event, Topic};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const INDEXER_STREAM: &str = "GKG_INDEXER";

pub const GLOBAL_INDEXING_SUBJECT: &str = "sdlc.global.indexing.requested";
pub const NAMESPACE_INDEXING_SUBJECT: &str = "sdlc.namespace.indexing.requested";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalIndexingRequest {
    pub watermark: DateTime<Utc>,
}

impl Event for GlobalIndexingRequest {
    fn topic() -> Topic {
        Topic::new(INDEXER_STREAM, GLOBAL_INDEXING_SUBJECT)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceIndexingRequest {
    pub organization: i64,
    pub namespace: i64,
    pub watermark: DateTime<Utc>,
}

impl Event for NamespaceIndexingRequest {
    fn topic() -> Topic {
        Topic::new(INDEXER_STREAM, NAMESPACE_INDEXING_SUBJECT)
    }
}
