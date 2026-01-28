use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use etl_engine::module::{Handler, HandlerContext, HandlerError};
use etl_engine::types::{Envelope, Event, SerializationError, Topic};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::indexer::modules::INDEXER_TOPIC;
use crate::indexer::modules::sdlc::watermark_store::{WatermarkError, WatermarkStore};

const SUBJECT: &str = "sdlc.namespace.indexing.requested";

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NamespaceHandlerPayload {
    pub organization: i64,
    pub namespace: i64,
    pub watermark: DateTime<Utc>,
}

impl Event for NamespaceHandlerPayload {
    fn topic() -> Topic {
        Topic::new(INDEXER_TOPIC, SUBJECT)
    }
}

pub struct NamespacedEntityContext {
    pub handler_context: HandlerContext,
    pub payload: NamespaceHandlerPayload,
    pub last_watermark: DateTime<Utc>,
}

#[async_trait]
pub trait NamespacedEntityHandler: Send + Sync {
    fn name(&self) -> &str;
    async fn handle(&self, context: &NamespacedEntityContext) -> Result<(), HandlerError>;
}

pub struct NamespaceHandler {
    watermark_store: Arc<dyn WatermarkStore>,
    entity_handlers: Vec<Box<dyn NamespacedEntityHandler>>,
}

impl NamespaceHandler {
    pub fn new(
        entity_handlers: Vec<Box<dyn NamespacedEntityHandler>>,
        watermark_store: Arc<dyn WatermarkStore>,
    ) -> Self {
        Self {
            watermark_store,
            entity_handlers,
        }
    }
}

#[async_trait]
impl Handler for NamespaceHandler {
    fn name(&self) -> &str {
        "namespace-handler"
    }

    fn topic(&self) -> Topic {
        Topic::new(INDEXER_TOPIC, SUBJECT)
    }

    async fn handle(
        &self,
        handler_context: HandlerContext,
        message: Envelope,
    ) -> Result<(), HandlerError> {
        let payload: NamespaceHandlerPayload = message.to_event().map_err(|error| match error {
            SerializationError::Json(e) => HandlerError::Deserialization(e),
        })?;

        let last_watermark = match self
            .watermark_store
            .get_namespace_watermark(payload.namespace)
            .await
        {
            Ok(w) => w,
            Err(WatermarkError::NoData) => DateTime::<Utc>::UNIX_EPOCH,
            Err(error) => {
                warn!(%error, "failed to fetch watermark, using epoch");
                DateTime::<Utc>::UNIX_EPOCH
            }
        };

        let context = NamespacedEntityContext {
            handler_context,
            payload: payload.clone(),
            last_watermark,
        };

        let mut errors = Vec::new();

        for handler in &self.entity_handlers {
            if let Err(error) = handler.handle(&context).await {
                warn!(handler = handler.name(), %error, "entity handler failed");
                errors.push((handler.name(), error));
            }
        }

        if errors.is_empty() {
            self.watermark_store
                .set_namespace_watermark(payload.namespace, &payload.watermark)
                .await
                .map_err(|e| {
                    HandlerError::Processing(format!("failed to update watermark: {e}"))
                })?;
        }

        if !errors.is_empty() {
            let error_details: Vec<_> = errors
                .iter()
                .map(|(name, err)| format!("{name}: {err}"))
                .collect();
            return Err(HandlerError::Processing(format!(
                "entity handlers failed: {}",
                error_details.join("; ")
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use etl_engine::testkit::{
        MockDestination, MockMetricCollector, MockNatsServices, TestEnvelopeFactory,
    };

    struct CountingHandler {
        call_count: AtomicUsize,
    }

    impl CountingHandler {
        fn new() -> Self {
            Self {
                call_count: AtomicUsize::new(0),
            }
        }

        fn count(&self) -> usize {
            self.call_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl NamespacedEntityHandler for CountingHandler {
        fn name(&self) -> &str {
            "counting-handler"
        }

        async fn handle(&self, _context: &NamespacedEntityContext) -> Result<(), HandlerError> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct MockWatermarkStore;

    #[async_trait]
    impl WatermarkStore for MockWatermarkStore {
        async fn get_namespace_watermark(&self, _: i64) -> Result<DateTime<Utc>, WatermarkError> {
            Ok(DateTime::<Utc>::UNIX_EPOCH)
        }

        async fn set_namespace_watermark(
            &self,
            _: i64,
            _: &DateTime<Utc>,
        ) -> Result<(), WatermarkError> {
            Ok(())
        }

        async fn get_global_watermark(&self) -> Result<DateTime<Utc>, WatermarkError> {
            Ok(DateTime::<Utc>::UNIX_EPOCH)
        }

        async fn set_global_watermark(&self, _: &DateTime<Utc>) -> Result<(), WatermarkError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn handle_delegates_to_all_entity_handlers() {
        let handler1 = Arc::new(CountingHandler::new());
        let handler2 = Arc::new(CountingHandler::new());

        let namespace_handler = NamespaceHandler {
            watermark_store: Arc::new(MockWatermarkStore),
            entity_handlers: vec![
                Box::new(CountingHandlerWrapper(Arc::clone(&handler1))),
                Box::new(CountingHandlerWrapper(Arc::clone(&handler2))),
            ],
        };

        let payload = serde_json::json!({
            "organization": 1,
            "namespace": 2,
            "watermark": "2024-01-21T00:00:00Z"
        })
        .to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let destination = Arc::new(MockDestination::new());
        let context = HandlerContext::new(
            destination,
            Arc::new(MockMetricCollector::new()),
            Arc::new(MockNatsServices::new()),
        );

        namespace_handler
            .handle(context, envelope)
            .await
            .expect("handler should succeed");

        assert_eq!(handler1.count(), 1, "handler1 should be called once");
        assert_eq!(handler2.count(), 1, "handler2 should be called once");
    }

    struct CountingHandlerWrapper(Arc<CountingHandler>);

    #[async_trait]
    impl NamespacedEntityHandler for CountingHandlerWrapper {
        fn name(&self) -> &str {
            self.0.name()
        }

        async fn handle(&self, context: &NamespacedEntityContext) -> Result<(), HandlerError> {
            self.0.handle(context).await
        }
    }
}
