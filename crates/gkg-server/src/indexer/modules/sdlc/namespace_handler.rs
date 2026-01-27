use std::sync::Arc;

use crate::indexer::modules::INDEXER_TOPIC;
use crate::indexer::modules::sdlc::datalake::{Datalake, DatalakeClient};
use crate::indexer::modules::sdlc::group_handler::GroupChildHandler;
use crate::indexer::modules::sdlc::project_handler::ProjectChildHandler;
use crate::indexer::modules::sdlc::watermark_store::{
    ClickHouseWatermarkStore, WatermarkClient, WatermarkError, WatermarkStore,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use etl_engine::module::{Handler, HandlerContext, HandlerError};
use etl_engine::types::{Envelope, Event, SerializationError, Topic};
use serde::{Deserialize, Serialize};
use tracing::warn;

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

pub struct ChildHandlerContext {
    pub handler_context: HandlerContext,
    pub payload: NamespaceHandlerPayload,
    pub last_watermark: DateTime<Utc>,
}

#[async_trait]
pub trait NamespaceChildHandler: Send + Sync {
    fn name(&self) -> &str;

    async fn handle(&self, context: &ChildHandlerContext) -> Result<(), HandlerError>;
}

pub struct NamespaceHandler {
    watermark_store: Arc<dyn WatermarkStore>,
    children: Vec<Box<dyn NamespaceChildHandler>>,
}

impl NamespaceHandler {
    pub fn new(datalake_client: DatalakeClient, watermark_client: WatermarkClient) -> Self {
        let watermark_store: Arc<dyn WatermarkStore> = Arc::new(ClickHouseWatermarkStore::new(Arc::clone(&watermark_client)));

        Self {
            watermark_store,
            children: vec![
                Box::new(GroupChildHandler::new(Arc::new(Datalake::new(Arc::clone(&datalake_client))))),
                Box::new(ProjectChildHandler::new(Arc::new(Datalake::new(Arc::clone(&datalake_client))))),
            ],
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
            .get_namespaces_watermark(payload.namespace)
            .await
        {
            Ok(w) => w,
            Err(WatermarkError::NoData) => DateTime::<Utc>::UNIX_EPOCH,
            Err(error) => {
                warn!(%error, "failed to fetch watermark, using epoch");
                DateTime::<Utc>::UNIX_EPOCH
            }
        };

        let context = ChildHandlerContext {
            handler_context,
            payload: payload.clone(),
            last_watermark,
        };

        let mut errors = Vec::new();

        for child in &self.children {
            if let Err(error) = child.handle(&context).await {
                warn!(handler = child.name(), %error, "child handler failed");
                errors.push((child.name().to_string(), error));
            }
        }

        if errors.is_empty() {
            self.watermark_store
                .set_namespaces_watermark(payload.namespace, &payload.watermark)
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
                "child handlers failed: {}",
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

    struct CountingChildHandler {
        call_count: AtomicUsize,
    }

    impl CountingChildHandler {
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
    impl NamespaceChildHandler for CountingChildHandler {
        fn name(&self) -> &str {
            "counting-child-handler"
        }

        async fn handle(&self, _context: &ChildHandlerContext) -> Result<(), HandlerError> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct MockWatermarkStore;

    #[async_trait]
    impl WatermarkStore for MockWatermarkStore {
        async fn get_users_watermark(&self) -> Result<DateTime<Utc>, WatermarkError> {
            Ok(DateTime::<Utc>::UNIX_EPOCH)
        }

        async fn set_users_watermark(&self, _: &DateTime<Utc>) -> Result<(), WatermarkError> {
            Ok(())
        }

        async fn get_namespaces_watermark(&self, _: i64) -> Result<DateTime<Utc>, WatermarkError> {
            Ok(DateTime::<Utc>::UNIX_EPOCH)
        }

        async fn set_namespaces_watermark(
            &self,
            _: i64,
            _: &DateTime<Utc>,
        ) -> Result<(), WatermarkError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn handle_delegates_to_all_children() {
        let child1 = Arc::new(CountingChildHandler::new());
        let child2 = Arc::new(CountingChildHandler::new());

        let handler = NamespaceHandler {
            watermark_store: Arc::new(MockWatermarkStore),
            children: vec![
                Box::new(CountingChildHandlerWrapper(Arc::clone(&child1))),
                Box::new(CountingChildHandlerWrapper(Arc::clone(&child2))),
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

        handler
            .handle(context, envelope)
            .await
            .expect("handler should succeed");

        assert_eq!(child1.count(), 1, "child1 should be called once");
        assert_eq!(child2.count(), 1, "child2 should be called once");
    }

    struct CountingChildHandlerWrapper(Arc<CountingChildHandler>);

    #[async_trait]
    impl NamespaceChildHandler for CountingChildHandlerWrapper {
        fn name(&self) -> &str {
            self.0.name()
        }

        async fn handle(&self, context: &ChildHandlerContext) -> Result<(), HandlerError> {
            self.0.handle(context).await
        }
    }
}
