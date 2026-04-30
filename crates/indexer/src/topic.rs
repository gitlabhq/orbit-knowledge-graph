use crate::types::{Event, Subscription};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const INDEXER_STREAM: &str = "GKG_INDEXER";

pub fn all_managed_subscriptions() -> Vec<Subscription> {
    vec![
        Subscription::new(INDEXER_STREAM, GLOBAL_INDEXING_SUBJECT),
        Subscription::new(INDEXER_STREAM, NAMESPACE_INDEXING_SUBJECT_PATTERN),
        Subscription::new(INDEXER_STREAM, CODE_INDEXING_TASK_SUBJECT_PATTERN),
        Subscription::new(INDEXER_STREAM, NAMESPACE_DELETION_SUBJECT_PATTERN),
    ]
}

pub const GLOBAL_INDEXING_SUBJECT: &str = "sdlc.global.indexing.requested";

pub const NAMESPACE_INDEXING_SUBJECT_PREFIX: &str = "sdlc.namespace.indexing.requested";
pub const NAMESPACE_INDEXING_SUBJECT_PATTERN: &str = "sdlc.namespace.indexing.requested.*.*";

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
    pub namespace: i64,
    pub traversal_path: String,
    pub watermark: DateTime<Utc>,
}

impl NamespaceIndexingRequest {
    pub fn publish_subscription(&self) -> Subscription {
        let suffix = gkg_utils::traversal_path::to_dotted(&self.traversal_path);
        Subscription::new(
            INDEXER_STREAM,
            format!("{NAMESPACE_INDEXING_SUBJECT_PREFIX}.{suffix}"),
        )
    }
}

impl Event for NamespaceIndexingRequest {
    fn subscription() -> Subscription {
        Subscription::new(INDEXER_STREAM, NAMESPACE_INDEXING_SUBJECT_PATTERN)
    }
}

pub const CODE_INDEXING_TASK_SUBJECT_PREFIX: &str = "code.task.indexing.requested";
pub const CODE_INDEXING_TASK_SUBJECT_PATTERN: &str = "code.task.indexing.requested.*.*";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeIndexingTaskRequest {
    pub task_id: i64,
    pub project_id: i64,
    pub branch: Option<String>,
    pub commit_sha: Option<String>,
    pub traversal_path: String,
}

impl CodeIndexingTaskRequest {
    pub fn publish_subscription(&self) -> Subscription {
        use base64::Engine;
        let branch_component = match &self.branch {
            Some(branch) => base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(branch),
            None => "_".to_string(),
        };
        Subscription::new(
            INDEXER_STREAM,
            format!(
                "{}.{}.{}",
                CODE_INDEXING_TASK_SUBJECT_PREFIX, self.project_id, branch_component
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
