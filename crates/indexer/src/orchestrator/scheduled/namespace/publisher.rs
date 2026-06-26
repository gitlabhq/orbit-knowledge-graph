use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use super::DispatchNamespace;
use crate::campaign::CampaignState;
use crate::nats::NatsServices;
use crate::orchestrator::dispatch::NamespaceIndexingDispatch;
use crate::orchestrator::scheduled::TaskError;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct PublishReport {
    pub dispatched: u64,
    pub skipped: u64,
}

#[async_trait]
pub(super) trait NamespacePublisher: Send + Sync {
    async fn publish(
        &self,
        namespaces: &[DispatchNamespace],
        watermark: DateTime<Utc>,
    ) -> Result<PublishReport, TaskError>;
}

pub(super) struct NamespaceRequestPublisher {
    dispatch: NamespaceIndexingDispatch,
    campaign: Arc<CampaignState>,
}

impl NamespaceRequestPublisher {
    pub(super) fn new(nats: Arc<dyn NatsServices>, campaign: Arc<CampaignState>) -> Self {
        Self {
            dispatch: NamespaceIndexingDispatch::new(nats),
            campaign,
        }
    }
}

#[async_trait]
impl NamespacePublisher for NamespaceRequestPublisher {
    async fn publish(
        &self,
        namespaces: &[DispatchNamespace],
        watermark: DateTime<Utc>,
    ) -> Result<PublishReport, TaskError> {
        let pairs: Vec<(i64, String)> = namespaces
            .iter()
            .map(|n| (n.namespace_id, n.traversal_path.clone()))
            .collect();
        let outcome = self
            .dispatch
            .dispatch_for_namespaces(&pairs, watermark, self.campaign.current())
            .await?;
        Ok(PublishReport {
            dispatched: outcome.dispatched,
            skipped: outcome.skipped,
        })
    }
}
