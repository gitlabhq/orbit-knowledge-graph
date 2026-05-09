use crate::types::{Event, Subscription};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const INDEXER_STREAM: &str = "GKG_INDEXER";

pub fn all_managed_subscriptions() -> Vec<Subscription> {
    vec![
        Subscription::new(INDEXER_STREAM, GLOBAL_INDEXING_SUBJECT),
        Subscription::new(INDEXER_STREAM, NAMESPACE_INDEXING_SUBJECT_PATTERN),
        Subscription::new(INDEXER_STREAM, ENTITY_INDEXING_SUBJECT_PATTERN),
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

pub const ENTITY_INDEXING_SUBJECT_PREFIX: &str = "sdlc.entity.indexing.requested";
pub const ENTITY_INDEXING_SUBJECT_PATTERN: &str = "sdlc.entity.indexing.requested.>";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityIndexingRequest {
    pub entity_kind: String,
    pub watermark: DateTime<Utc>,
    pub scope: IndexingScope,
    pub partition: Option<PartitionSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexingScope {
    Global,
    Namespace {
        namespace_id: i64,
        traversal_path: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionSpec {
    pub partition_index: u32,
    pub total_partitions: u32,
    pub strategy: PartitionStrategy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PartitionStrategy {
    Range {
        lower_bound: String,
        upper_bound: String,
    },
}

impl Event for EntityIndexingRequest {
    fn subscription() -> Subscription {
        Subscription::new(INDEXER_STREAM, ENTITY_INDEXING_SUBJECT_PATTERN)
    }
}

impl EntityIndexingRequest {
    pub fn publish_subject(&self) -> String {
        let scope_suffix = match &self.scope {
            IndexingScope::Global => "global".to_string(),
            IndexingScope::Namespace { traversal_path, .. } => {
                gkg_utils::traversal_path::to_dotted(traversal_path)
            }
        };

        let base = format!(
            "{}.{}.{}",
            ENTITY_INDEXING_SUBJECT_PREFIX, self.entity_kind, scope_suffix
        );

        match &self.partition {
            Some(spec) => format!("{base}.p{}", spec.partition_index),
            None => base,
        }
    }

    pub fn publish_subscription(&self) -> Subscription {
        Subscription::new(INDEXER_STREAM, self.publish_subject())
    }
}

// --- Namespace deletion ---

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

#[cfg(test)]
mod tests {
    use super::*;

    fn namespace_request(
        entity_kind: &str,
        partition: Option<PartitionSpec>,
    ) -> EntityIndexingRequest {
        EntityIndexingRequest {
            entity_kind: entity_kind.to_string(),
            watermark: "2024-01-01T00:00:00Z".parse().unwrap(),
            scope: IndexingScope::Namespace {
                namespace_id: 9970,
                traversal_path: "42/9970/".to_string(),
            },
            partition,
        }
    }

    fn global_request(entity_kind: &str) -> EntityIndexingRequest {
        EntityIndexingRequest {
            entity_kind: entity_kind.to_string(),
            watermark: "2024-01-01T00:00:00Z".parse().unwrap(),
            scope: IndexingScope::Global,
            partition: None,
        }
    }

    fn range_partition(index: u32, total: u32) -> PartitionSpec {
        PartitionSpec {
            partition_index: index,
            total_partitions: total,
            strategy: PartitionStrategy::Range {
                lower_bound: "0".to_string(),
                upper_bound: "25000000".to_string(),
            },
        }
    }

    #[test]
    fn publish_subject_namespaced_no_partition() {
        let request = namespace_request("MergeRequest", None);
        assert_eq!(
            request.publish_subject(),
            "sdlc.entity.indexing.requested.MergeRequest.42.9970"
        );
    }

    #[test]
    fn publish_subject_namespaced_with_partition() {
        let request = namespace_request("MergeRequest", Some(range_partition(2, 4)));
        assert_eq!(
            request.publish_subject(),
            "sdlc.entity.indexing.requested.MergeRequest.42.9970.p2"
        );
    }

    #[test]
    fn publish_subject_global() {
        let request = global_request("User");
        assert_eq!(
            request.publish_subject(),
            "sdlc.entity.indexing.requested.User.global"
        );
    }

    #[test]
    fn serialization_roundtrip() {
        let request = namespace_request("MergeRequest", Some(range_partition(0, 4)));
        let json = serde_json::to_string(&request).unwrap();
        let deserialized: EntityIndexingRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.entity_kind, "MergeRequest");
        assert!(deserialized.partition.is_some());
        let spec = deserialized.partition.unwrap();
        assert_eq!(spec.partition_index, 0);
        assert_eq!(spec.total_partitions, 4);
        match &spec.strategy {
            PartitionStrategy::Range {
                lower_bound,
                upper_bound,
            } => {
                assert_eq!(lower_bound, "0");
                assert_eq!(upper_bound, "25000000");
            }
        }
    }

    #[test]
    fn serialization_without_partition() {
        let request = global_request("User");
        let json = serde_json::to_string(&request).unwrap();
        let deserialized: EntityIndexingRequest = serde_json::from_str(&json).unwrap();
        assert!(deserialized.partition.is_none());
    }
}
