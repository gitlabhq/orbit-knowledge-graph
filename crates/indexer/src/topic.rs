use crate::types::{Event, Subscription};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const INDEXER_STREAM: &str = "GKG_INDEXER";

pub const GLOBAL_HANDLER_TOPIC: &str = "global-handler";
pub const NAMESPACE_HANDLER_TOPIC: &str = "namespace-handler";
pub const CODE_INDEXING_TASK_TOPIC: &str = "code-indexing-task";
pub const NAMESPACE_DELETION_TOPIC: &str = "namespace-deletion";

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
    #[serde(default)]
    pub dispatch_id: Uuid,
    #[serde(default)]
    pub campaign_id: Option<String>,
    #[serde(default)]
    pub targets: Vec<String>,
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
    #[serde(default)]
    pub dispatch_id: Uuid,
    #[serde(default)]
    pub campaign_id: Option<String>,
    #[serde(default)]
    pub targets: Vec<String>,
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
    #[serde(default)]
    pub dispatch_id: Uuid,
    #[serde(default)]
    pub campaign_id: Option<String>,
    /// When set to `"external_repository"`, the task targets an external repo
    /// and `external_repository_id` carries the owning entity ID. `None`
    /// (the default) means a regular GitLab project.
    #[serde(default)]
    pub source_type: Option<String>,
    /// ID of the `Analytics::KnowledgeGraph::ExternalRepository` record when
    /// `source_type` is `"external_repository"`.
    #[serde(default)]
    pub external_repository_id: Option<i64>,
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
    #[serde(default)]
    pub dispatch_id: Uuid,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_indexing_task_request_deserializes_without_external_fields() {
        let json = r#"{
            "task_id": 42,
            "project_id": 123,
            "branch": "main",
            "commit_sha": "abc123",
            "traversal_path": "1/42/"
        }"#;

        let request: CodeIndexingTaskRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.task_id, 42);
        assert_eq!(request.project_id, 123);
        assert!(request.source_type.is_none());
        assert!(request.external_repository_id.is_none());
    }

    #[test]
    fn code_indexing_task_request_deserializes_with_external_fields() {
        let json = r#"{
            "task_id": 1,
            "project_id": 0,
            "branch": "main",
            "commit_sha": "def456",
            "traversal_path": "1/42/",
            "source_type": "external_repository",
            "external_repository_id": 99
        }"#;

        let request: CodeIndexingTaskRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.project_id, 0);
        assert_eq!(request.source_type.as_deref(), Some("external_repository"));
        assert_eq!(request.external_repository_id, Some(99));
    }

    #[test]
    fn code_indexing_task_request_serializes_round_trip() {
        let request = CodeIndexingTaskRequest {
            task_id: 1,
            project_id: 0,
            branch: Some("main".to_string()),
            commit_sha: Some("abc".to_string()),
            traversal_path: "1/42/".to_string(),
            dispatch_id: Uuid::nil(),
            campaign_id: None,
            source_type: Some("external_repository".to_string()),
            external_repository_id: Some(77),
        };

        let json = serde_json::to_string(&request).unwrap();
        let deserialized: CodeIndexingTaskRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.external_repository_id, Some(77));
        assert_eq!(deserialized.source_type.as_deref(), Some("external_repository"));
    }
}
