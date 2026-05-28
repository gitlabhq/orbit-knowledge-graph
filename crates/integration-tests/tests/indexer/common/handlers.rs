use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use indexer::IndexerConfig;
use indexer::clickhouse::ClickHouseDestination;
use indexer::handler::{Handler, HandlerContext, HandlerError, HandlerRegistry};
use indexer::metrics::EngineMetrics;
use indexer::nats::ProgressNotifier;
use indexer::testkit::{
    MockLockService, MockNatsServices, TestEnvelopeFactory, create_test_indexer_config,
};
use indexer::topic::{GlobalIndexingRequest, NamespaceIndexingRequest};
use indexer::types::{Envelope, Event, Subscription};
use integration_testkit::TestContext;

pub fn handler_context(ctx: &TestContext) -> HandlerContext {
    let destination =
        ClickHouseDestination::new(ctx.config.clone(), Arc::new(EngineMetrics::default()))
            .expect("failed to create destination");
    let mock_nats = Arc::new(MockNatsServices::new());
    HandlerContext::new(
        Arc::new(destination),
        mock_nats.clone(),
        Arc::new(MockLockService::new()),
        ProgressNotifier::noop(),
        Arc::new(indexer::indexing_status::IndexingStatusStore::new(
            mock_nats,
        )),
    )
}

struct FanOutHandler {
    name: String,
    subscription: Subscription,
    handlers: Vec<Arc<dyn Handler>>,
}

#[async_trait]
impl Handler for FanOutHandler {
    fn name(&self) -> &str {
        &self.name
    }

    fn subscription(&self) -> Subscription {
        self.subscription.clone()
    }

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        let mut errors = Vec::new();
        for handler in &self.handlers {
            if let Err(err) = handler.handle(context.clone(), message.clone()).await {
                errors.push(format!("{}: {err}", handler.name()));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(HandlerError::Processing(format!(
                "fan-out failures: {}",
                errors.join("; ")
            )))
        }
    }
}

async fn build_fan_out(
    ctx: &TestContext,
    name: &str,
    subscription: Subscription,
) -> Arc<dyn Handler> {
    let config = create_test_indexer_config(&ctx.config);
    let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
    let registry = HandlerRegistry::default();
    indexer::modules::sdlc::register_handlers(&registry, &config, &ontology)
        .await
        .expect("failed to register SDLC handlers");
    let handlers = registry.handlers_for(&subscription);
    assert!(!handlers.is_empty(), "no handlers for {name}");
    Arc::new(FanOutHandler {
        name: name.to_string(),
        subscription,
        handlers,
    })
}

pub async fn namespace_handler(ctx: &TestContext) -> Arc<dyn Handler> {
    build_fan_out(
        ctx,
        "namespace_fan_out",
        NamespaceIndexingRequest::subscription(),
    )
    .await
}

pub async fn global_handler(ctx: &TestContext) -> Arc<dyn Handler> {
    build_fan_out(ctx, "global_fan_out", GlobalIndexingRequest::subscription()).await
}

pub async fn entity_handler_with_partitions(
    ctx: &TestContext,
    entity_name: &str,
    partitions: u32,
) -> Arc<dyn Handler> {
    let mut config: IndexerConfig = create_test_indexer_config(&ctx.config);
    config.engine.handlers.entity_handler.partition_overrides =
        HashMap::from([(entity_name.to_string(), partitions)]);

    let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
    let registry = HandlerRegistry::default();
    indexer::modules::sdlc::register_handlers(&registry, &config, &ontology)
        .await
        .expect("failed to register SDLC handlers");

    let handler_name = format!("entity.{}", entity_name.to_lowercase());
    registry
        .find_by_name(&handler_name)
        .unwrap_or_else(|| panic!("handler not found: {handler_name}"))
}

pub fn default_test_watermark() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2024-01-21T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc)
}

pub fn namespace_envelope(org_id: i64, namespace_id: i64) -> Envelope {
    TestEnvelopeFactory::simple(
        &serde_json::json!({
            "namespace": namespace_id,
            "traversal_path": format!("{org_id}/{namespace_id}/"),
            "watermark": default_test_watermark().to_rfc3339(),
            "dispatch_id": uuid::Uuid::new_v4(),
        })
        .to_string(),
    )
}

pub fn global_envelope() -> Envelope {
    TestEnvelopeFactory::simple(
        &serde_json::json!({
            "watermark": default_test_watermark().to_rfc3339(),
            "dispatch_id": uuid::Uuid::new_v4(),
        })
        .to_string(),
    )
}
