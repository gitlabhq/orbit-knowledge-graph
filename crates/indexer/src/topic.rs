use crate::types::{Event, Subscription};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const INDEXER_STREAM: &str = "GKG_INDEXER";

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

/// Extract the top-level namespace id from a Rails-emitted `traversal_path`.
///
/// `traversal_path` is documented in `docs/design-documents/security.md` as a
/// `/`-delimited string starting with the organization id, then the top-level
/// namespace id, then any deeper namespaces (e.g. `"42/9970/"` or
/// `"42/9970/12345/"`). Returns `None` when the segment is missing or not a
/// valid `i64` so callers can emit a sentinel and log; we do not want to drop
/// the metric label conditionally because Prometheus splits heterogeneous
/// label sets into incompatible series.
pub fn top_level_namespace_id(traversal_path: &str) -> Option<i64> {
    traversal_path.split('/').nth(1)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::top_level_namespace_id;

    #[test]
    fn parses_top_level_namespace_from_three_segment_path() {
        assert_eq!(top_level_namespace_id("42/9970/12345/"), Some(9970));
    }

    #[test]
    fn parses_top_level_namespace_from_two_segment_path() {
        assert_eq!(top_level_namespace_id("42/9970/"), Some(9970));
    }

    #[test]
    fn returns_none_when_segment_missing() {
        assert_eq!(top_level_namespace_id("42"), None);
        assert_eq!(top_level_namespace_id(""), None);
    }

    #[test]
    fn returns_none_when_segment_not_numeric() {
        assert_eq!(top_level_namespace_id("42/abc/"), None);
        assert_eq!(top_level_namespace_id("/org/project-123"), None);
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
