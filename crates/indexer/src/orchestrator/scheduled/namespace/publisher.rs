use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tracing::{debug, warn};
use uuid::Uuid;

use super::change_detection::ChangedNamespace;
use crate::campaign::CampaignState;
use crate::nats::NatsServices;
use crate::orchestrator::scheduled::TaskError;
use crate::topic::NamespaceIndexingRequest;
use crate::types::Envelope;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct PublishReport {
    pub dispatched: u64,
    pub skipped: u64,
}

#[async_trait]
pub(super) trait NamespacePublisher: Send + Sync {
    async fn publish(
        &self,
        namespaces: &[ChangedNamespace],
        watermark: DateTime<Utc>,
    ) -> Result<PublishReport, TaskError>;
}

pub(super) struct NamespaceRequestPublisher {
    nats: Arc<dyn NatsServices>,
    campaign: Arc<CampaignState>,
}

impl NamespaceRequestPublisher {
    pub(super) fn new(nats: Arc<dyn NatsServices>, campaign: Arc<CampaignState>) -> Self {
        Self { nats, campaign }
    }
}

#[async_trait]
impl NamespacePublisher for NamespaceRequestPublisher {
    async fn publish(
        &self,
        namespaces: &[ChangedNamespace],
        watermark: DateTime<Utc>,
    ) -> Result<PublishReport, TaskError> {
        let campaign_id = self.campaign.current();
        let mut report = PublishReport::default();

        for namespace in namespaces {
            if !is_dispatchable_traversal_path(&namespace.traversal_path) {
                warn!(
                    namespace_id = namespace.namespace_id,
                    traversal_path = %namespace.traversal_path,
                    "skipping changed namespace with invalid traversal_path"
                );
                continue;
            }

            let request = NamespaceIndexingRequest {
                namespace: namespace.namespace_id,
                traversal_path: namespace.traversal_path.clone(),
                watermark,
                dispatch_id: Uuid::new_v4(),
                campaign_id: campaign_id.clone(),
                targets: Vec::new(),
            };

            let subscription = request.publish_subscription();
            let envelope = Envelope::new(&request).map_err(TaskError::new)?;

            match self.nats.publish(&subscription, &envelope).await {
                Ok(()) => {
                    report.dispatched += 1;
                    debug!(
                        namespace_id = namespace.namespace_id,
                        traversal_path = %namespace.traversal_path,
                        target_keys = ?namespace.target_keys,
                        "dispatched namespace indexing request"
                    );
                }
                Err(crate::nats::NatsError::PublishDuplicate) => {
                    report.skipped += 1;
                    debug!(
                        namespace_id = namespace.namespace_id,
                        traversal_path = %namespace.traversal_path,
                        target_keys = ?namespace.target_keys,
                        "skipped namespace indexing request, already in-flight"
                    );
                }
                Err(error) => return Err(TaskError::new(error)),
            }
        }

        Ok(report)
    }
}

fn is_dispatchable_traversal_path(path: &str) -> bool {
    gkg_utils::traversal_path::is_valid(path)
}

#[cfg(test)]
mod tests {
    use super::is_dispatchable_traversal_path;

    #[test]
    fn dispatchable_traversal_paths_require_org_and_namespace_segments() {
        assert!(is_dispatchable_traversal_path("1/9/"));
        assert!(!is_dispatchable_traversal_path(""));
        assert!(!is_dispatchable_traversal_path("0/"));
        assert!(!is_dispatchable_traversal_path("1/"));
    }
}
