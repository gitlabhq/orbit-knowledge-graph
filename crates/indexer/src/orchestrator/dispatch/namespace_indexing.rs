use std::sync::Arc;

use chrono::{DateTime, Utc};
use tracing::{debug, warn};
use uuid::Uuid;

use super::DispatchOutcome;
use crate::nats::{NatsError, NatsServices};
use crate::orchestrator::scheduled::TaskError;
use crate::topic::NamespaceIndexingRequest;
use crate::types::Envelope;

pub struct NamespaceIndexingDispatch {
    nats: Arc<dyn NatsServices>,
}

impl NamespaceIndexingDispatch {
    pub fn new(nats: Arc<dyn NatsServices>) -> Self {
        Self { nats }
    }

    pub async fn dispatch_for_namespaces(
        &self,
        namespaces: &[(i64, String)],
        watermark: DateTime<Utc>,
        campaign_id: Option<String>,
    ) -> Result<DispatchOutcome, TaskError> {
        let mut outcome = DispatchOutcome {
            dispatched: 0,
            skipped: 0,
        };

        for (namespace_id, traversal_path) in namespaces {
            if !gkg_utils::traversal_path::is_valid(traversal_path) {
                warn!(
                    namespace_id = *namespace_id,
                    %traversal_path,
                    "skipping namespace with invalid traversal_path"
                );
                continue;
            }

            let request = NamespaceIndexingRequest {
                namespace: *namespace_id,
                traversal_path: traversal_path.clone(),
                watermark,
                dispatch_id: Uuid::new_v4(),
                campaign_id: campaign_id.clone(),
                targets: Vec::new(),
            };
            let subscription = request.publish_subscription();
            let envelope = Envelope::new(&request).map_err(TaskError::new)?;

            match self.nats.publish(&subscription, &envelope).await {
                Ok(()) => {
                    outcome.dispatched += 1;
                    debug!(
                        namespace_id = *namespace_id,
                        %traversal_path,
                        "dispatched namespace indexing request"
                    );
                }
                Err(NatsError::PublishDuplicate) => outcome.skipped += 1,
                Err(error) => return Err(TaskError::new(error)),
            }
        }

        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::mocks::MockNatsServices;

    #[tokio::test]
    async fn publishes_one_request_per_valid_namespace() {
        let nats = Arc::new(MockNatsServices::new());
        let dispatch = NamespaceIndexingDispatch::new(nats.clone());

        let outcome = dispatch
            .dispatch_for_namespaces(
                &[(100, "1/100/".to_string()), (200, "2/200/".to_string())],
                Utc::now(),
                None,
            )
            .await
            .unwrap();

        assert_eq!(outcome.dispatched, 2);
        assert_eq!(nats.get_published().len(), 2);
    }

    #[tokio::test]
    async fn skips_invalid_traversal_paths() {
        let nats = Arc::new(MockNatsServices::new());
        let dispatch = NamespaceIndexingDispatch::new(nats.clone());

        let outcome = dispatch
            .dispatch_for_namespaces(&[(1, "0/".to_string())], Utc::now(), None)
            .await
            .unwrap();

        assert_eq!(outcome.dispatched, 0);
        assert!(nats.get_published().is_empty());
    }
}
