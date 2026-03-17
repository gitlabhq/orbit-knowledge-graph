use crate::types::{Event, Subscription};
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
    fn subscription() -> Subscription {
        Subscription::new(INDEXER_STREAM, GLOBAL_INDEXING_SUBJECT)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceIndexingRequest {
    pub organization: i64,
    pub namespace: i64,
    pub watermark: DateTime<Utc>,
}

impl NamespaceIndexingRequest {
    pub fn publish_subscription(&self) -> Subscription {
        Subscription::new(
            INDEXER_STREAM,
            format!(
                "{}.{}.{}",
                NAMESPACE_INDEXING_SUBJECT_PREFIX, self.organization, self.namespace
            ),
        )
    }
}

impl Event for NamespaceIndexingRequest {
    fn subscription() -> Subscription {
        Subscription::new(INDEXER_STREAM, NAMESPACE_INDEXING_SUBJECT_PATTERN)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectCodeIndexingRequest {
    pub project_id: i64,
}

impl ProjectCodeIndexingRequest {
    pub fn publish_subscription(&self) -> Subscription {
        Subscription::new(
            INDEXER_STREAM,
            format!(
                "{}.{}",
                PROJECT_CODE_INDEXING_SUBJECT_PREFIX, self.project_id
            ),
        )
    }
}

impl Event for ProjectCodeIndexingRequest {
    fn subscription() -> Subscription {
        Subscription::new(INDEXER_STREAM, PROJECT_CODE_INDEXING_SUBJECT_PATTERN)
    }
}

pub const CODE_INDEXING_TASK_SUBJECT_PREFIX: &str = "code.task.indexing.requested";
pub const CODE_INDEXING_TASK_SUBJECT_PATTERN: &str = "code.task.indexing.requested.*.*";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeIndexingTaskRequest {
    pub task_id: i64,
    pub project_id: i64,
    pub branch: String,
    pub commit_sha: String,
    pub traversal_path: String,
}

impl CodeIndexingTaskRequest {
    pub fn publish_subscription(&self) -> Subscription {
        use base64::Engine;
        let encoded_branch = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&self.branch);
        Subscription::new(
            INDEXER_STREAM,
            format!(
                "{}.{}.{}",
                CODE_INDEXING_TASK_SUBJECT_PREFIX, self.project_id, encoded_branch
            ),
        )
    }
}

impl Event for CodeIndexingTaskRequest {
    fn subscription() -> Subscription {
        Subscription::new(INDEXER_STREAM, CODE_INDEXING_TASK_SUBJECT_PATTERN)
    }
}

pub const NAMESPACE_DELETION_SUBJECT_PREFIX: &str = "sdlc.namespace.deletion.requested";
pub const NAMESPACE_DELETION_SUBJECT_PATTERN: &str = "sdlc.namespace.deletion.requested.*";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceDeletionRequest {
    pub namespace_id: i64,
    pub traversal_path: String,
}

impl NamespaceDeletionRequest {
    pub fn publish_subscription(&self) -> Subscription {
        Subscription::new(
            INDEXER_STREAM,
            format!(
                "{}.{}",
                NAMESPACE_DELETION_SUBJECT_PREFIX, self.namespace_id
            ),
        )
    }
}

impl Event for NamespaceDeletionRequest {
    fn subscription() -> Subscription {
        Subscription::new(INDEXER_STREAM, NAMESPACE_DELETION_SUBJECT_PATTERN)
    }
}
