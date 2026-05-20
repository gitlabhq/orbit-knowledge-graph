use async_trait::async_trait;
use gkg_server_config::HandlerConfiguration;
use tracing::debug;

use crate::handler::{Handler, HandlerContext, HandlerError};
use crate::topic::EntityIndexingRequest;
use crate::types::{Envelope, Event, SerializationError, Subscription};

pub struct EntityIndexingHandler {
    subscription: Subscription,
    config: HandlerConfiguration,
}

impl EntityIndexingHandler {
    pub fn new(config: HandlerConfiguration) -> Self {
        let subscription = EntityIndexingRequest::subscription();
        Self {
            subscription,
            config,
        }
    }
}

#[async_trait]
impl Handler for EntityIndexingHandler {
    fn name(&self) -> &str {
        "entity_handler"
    }

    fn subscription(&self) -> Subscription {
        self.subscription.clone()
    }

    fn engine_config(&self) -> &HandlerConfiguration {
        &self.config
    }

    async fn handle(
        &self,
        _context: HandlerContext,
        message: Envelope,
    ) -> Result<(), HandlerError> {
        let request: EntityIndexingRequest = message.to_event().map_err(|error| match error {
            SerializationError::Json(err) => HandlerError::Deserialization(err),
        })?;

        debug!(
            entity_kind = %request.entity_kind,
            scope = ?request.scope,
            "received entity indexing request (no pipelines registered yet)"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::nats::ProgressNotifier;
    use crate::testkit::{MockDestination, MockLockService, MockNatsServices, TestEnvelopeFactory};

    fn test_handler_context() -> HandlerContext {
        let destination = Arc::new(MockDestination::new());
        let mock_nats = Arc::new(MockNatsServices::new());
        HandlerContext::new(
            destination,
            mock_nats.clone(),
            Arc::new(MockLockService::new()),
            ProgressNotifier::noop(),
            Arc::new(crate::indexing_status::IndexingStatusStore::new(mock_nats)),
        )
    }

    #[tokio::test]
    async fn handle_deserializes_and_returns_ok() {
        let handler = EntityIndexingHandler::new(HandlerConfiguration::default());

        let payload = serde_json::json!({
            "dispatch_id": "20240121T000000",
            "entity_kind": "User",
            "watermark": "2024-01-21T00:00:00Z",
            "scope": "Global"
        })
        .to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let result = handler.handle(test_handler_context(), envelope).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn handle_namespaced_request() {
        let handler = EntityIndexingHandler::new(HandlerConfiguration::default());

        let payload = serde_json::json!({
            "dispatch_id": "20240121T000000",
            "entity_kind": "MergeRequest",
            "watermark": "2024-01-21T00:00:00Z",
            "scope": { "Namespace": { "namespace_id": 100, "traversal_path": "42/100/" } }
        })
        .to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let result = handler.handle(test_handler_context(), envelope).await;
        assert!(result.is_ok());
    }
}
