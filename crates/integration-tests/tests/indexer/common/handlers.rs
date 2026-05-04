use std::sync::Arc;

use chrono::{DateTime, Utc};
use indexer::clickhouse::ClickHouseDestination;
use indexer::handler::{Handler, HandlerContext, HandlerRegistry};
use indexer::metrics::EngineMetrics;
use indexer::nats::ProgressNotifier;
use indexer::testkit::{
    MockLockService, MockNatsServices, TestEnvelopeFactory, create_test_indexer_config,
};
use indexer::types::Envelope;
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

pub async fn namespace_handler(ctx: &TestContext) -> Arc<dyn Handler> {
    let config = create_test_indexer_config(&ctx.config);
    let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
    let registry = HandlerRegistry::default();
    indexer::modules::sdlc::register_handlers(&registry, &config, &ontology)
        .await
        .expect("failed to register SDLC handlers");
    registry
        .find_by_name("namespace_handler")
        .expect("namespace_handler not found")
}

pub async fn global_handler(ctx: &TestContext) -> Arc<dyn Handler> {
    let config = create_test_indexer_config(&ctx.config);
    let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
    let registry = HandlerRegistry::default();
    indexer::modules::sdlc::register_handlers(&registry, &config, &ontology)
        .await
        .expect("failed to register SDLC handlers");
    registry
        .find_by_name("global_handler")
        .expect("global_handler not found")
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
            "watermark": default_test_watermark().to_rfc3339()
        })
        .to_string(),
    )
}

pub fn global_envelope() -> Envelope {
    TestEnvelopeFactory::simple(
        &serde_json::json!({
            "watermark": default_test_watermark().to_rfc3339()
        })
        .to_string(),
    )
}
