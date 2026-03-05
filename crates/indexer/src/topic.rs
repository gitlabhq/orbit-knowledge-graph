use crate::types::{Event, Topic};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const INDEXER_STREAM: &str = "GKG_INDEXER";

pub const GLOBAL_INDEXING_SUBJECT: &str = "sdlc.global.indexing.requested";

pub const NAMESPACE_INDEXING_SUBJECT_PREFIX: &str = "sdlc.namespace.indexing.requested";
pub const NAMESPACE_INDEXING_SUBJECT_PATTERN: &str = "sdlc.namespace.indexing.requested.*.*";

pub const PROJECT_CODE_INDEXING_SUBJECT_PREFIX: &str = "code.project.indexing.requested";
pub const PROJECT_CODE_INDEXING_SUBJECT_PATTERN: &str = "code.project.indexing.requested.*";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalIndexingRequest {
    pub watermark: DateTime<Utc>,
}

impl Event for GlobalIndexingRequest {
    fn topic() -> Topic {
        Topic::owned(INDEXER_STREAM, GLOBAL_INDEXING_SUBJECT)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceIndexingRequest {
    pub organization: i64,
    pub namespace: i64,
    pub watermark: DateTime<Utc>,
}

impl NamespaceIndexingRequest {
    pub fn publish_topic(&self) -> Topic {
        Topic::owned(
            INDEXER_STREAM,
            format!(
                "{}.{}.{}",
                NAMESPACE_INDEXING_SUBJECT_PREFIX, self.organization, self.namespace
            ),
        )
    }
}

impl Event for NamespaceIndexingRequest {
    fn topic() -> Topic {
        Topic::owned(INDEXER_STREAM, NAMESPACE_INDEXING_SUBJECT_PATTERN)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectCodeIndexingRequest {
    pub project_id: i64,
}

impl ProjectCodeIndexingRequest {
    pub fn publish_topic(&self) -> Topic {
        Topic::owned(
            INDEXER_STREAM,
            format!(
                "{}.{}",
                PROJECT_CODE_INDEXING_SUBJECT_PREFIX, self.project_id
            ),
        )
    }
}

impl Event for ProjectCodeIndexingRequest {
    fn topic() -> Topic {
        Topic::owned(INDEXER_STREAM, PROJECT_CODE_INDEXING_SUBJECT_PATTERN)
    }
}
