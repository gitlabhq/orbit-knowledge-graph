#![allow(dead_code, unused_imports)]

use std::sync::Arc;

use chrono::{DateTime, Utc};
use indexer::clickhouse::ClickHouseDestination;
use indexer::metrics::EngineMetrics;
use indexer::module::{Handler, HandlerContext, Module};
use indexer::modules::SdlcModule;
use indexer::testkit::{MockLockService, MockNatsServices};
use std::collections::HashMap;

pub use integration_testkit::{
    TestContext, get_boolean_column, get_int64_column, get_string_column, get_uint64_column,
};

pub const SIPHON_SCHEMA_SQL: &str = include_str!("../fixtures/siphon.sql");
pub const GRAPH_SCHEMA_SQL: &str = include_str!("../../../../fixtures/schema/graph.sql");

pub trait GkgServerTestExt {
    fn create_destination(&self) -> ClickHouseDestination;
    fn create_handler_context(&self) -> HandlerContext;
    async fn get_namespace_handler(&self) -> Box<dyn Handler>;
    async fn assert_edge_count(
        &self,
        relationship_kind: &str,
        source_kind: &str,
        target_kind: &str,
        expected_count: usize,
    );
}

impl GkgServerTestExt for TestContext {
    fn create_destination(&self) -> ClickHouseDestination {
        ClickHouseDestination::new(self.config.clone(), Arc::new(EngineMetrics::default()))
            .expect("failed to create destination")
    }

    fn create_handler_context(&self) -> HandlerContext {
        HandlerContext::new(
            Arc::new(self.create_destination()),
            Arc::new(MockNatsServices::new()),
            Arc::new(MockLockService::new()),
        )
    }

    async fn get_namespace_handler(&self) -> Box<dyn Handler> {
        let handler_configs = HashMap::from([
            (
                "global-handler".to_string(),
                serde_json::json!({ "datalake_batch_size": 1 }),
            ),
            (
                "namespace-handler".to_string(),
                serde_json::json!({ "datalake_batch_size": 1 }),
            ),
        ]);
        let sdlc_module = SdlcModule::new(&self.config, &self.config, &handler_configs)
            .await
            .expect("failed to create SDLC module");
        let handlers = sdlc_module.handlers();
        handlers
            .into_iter()
            .find(|h| h.name() == "namespace-handler")
            .expect("namespace-handler not found")
    }

    async fn assert_edge_count(
        &self,
        relationship_kind: &str,
        source_kind: &str,
        target_kind: &str,
        expected_count: usize,
    ) {
        let query = format!(
            "SELECT source_id, target_id FROM gl_edge WHERE relationship_kind = '{relationship_kind}' \
             AND source_kind = '{source_kind}' AND target_kind = '{target_kind}'"
        );
        let result = self.query(&query).await;
        assert!(
            !result.is_empty(),
            "{relationship_kind} edges from {source_kind} to {target_kind} should exist"
        );
        assert_eq!(
            result[0].num_rows(),
            expected_count,
            "expected {expected_count} {relationship_kind} edges from {source_kind} to {target_kind}"
        );
    }
}

pub fn create_user_payload(watermark: DateTime<Utc>) -> String {
    serde_json::json!({
        "watermark": watermark.to_rfc3339()
    })
    .to_string()
}

pub fn create_namespace_payload(
    organization: i64,
    namespace: i64,
    watermark: DateTime<Utc>,
) -> String {
    serde_json::json!({
        "organization": organization,
        "namespace": namespace,
        "watermark": watermark.to_rfc3339()
    })
    .to_string()
}

pub fn default_test_watermark() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2024-01-21T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc)
}
