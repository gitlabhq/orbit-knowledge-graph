#![allow(dead_code, unused_imports)]

use std::sync::Arc;

use chrono::{DateTime, Utc};
use indexer::clickhouse::ClickHouseDestination;
use indexer::handler::{Handler, HandlerContext, HandlerRegistry};
use indexer::metrics::EngineMetrics;
use indexer::testkit::{MockLockService, MockNatsServices, create_test_indexer_config};

pub use integration_testkit::{
    TestContext, get_boolean_column, get_int64_column, get_string_column, get_uint64_column,
};

pub const SIPHON_SCHEMA_SQL: &str = include_str!("../fixtures/siphon.sql");
pub const GRAPH_SCHEMA_SQL: &str = include_str!("../../../../fixtures/schema/graph.sql");

pub trait IndexerTestExt {
    fn create_destination(&self) -> ClickHouseDestination;
    fn create_handler_context(&self) -> HandlerContext;
    async fn get_namespace_handler(&self) -> Arc<dyn Handler>;
    async fn get_global_handler(&self) -> Arc<dyn Handler>;
    async fn assert_edge_count(
        &self,
        relationship_kind: &str,
        source_kind: &str,
        target_kind: &str,
        expected_count: usize,
    );
    async fn assert_edge_count_for_traversal_path(
        &self,
        relationship_kind: &str,
        source_kind: &str,
        target_kind: &str,
        traversal_path: &str,
        expected_count: usize,
    );
    async fn assert_edges_have_traversal_path(
        &self,
        relationship_kind: &str,
        source_kind: &str,
        target_kind: &str,
        expected_traversal_path: &str,
        expected_count: usize,
    );
}

impl IndexerTestExt for TestContext {
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

    async fn get_namespace_handler(&self) -> Arc<dyn Handler> {
        let indexer_config = create_test_indexer_config(&self.config);
        let registry = HandlerRegistry::default();
        indexer::modules::sdlc::register_handlers(&registry, &indexer_config)
            .await
            .expect("failed to create SDLC handlers");
        registry
            .find_by_name("namespace_handler")
            .expect("namespace_handler not found")
    }

    async fn get_global_handler(&self) -> Arc<dyn Handler> {
        let indexer_config = create_test_indexer_config(&self.config);
        let registry = HandlerRegistry::default();
        indexer::modules::sdlc::register_handlers(&registry, &indexer_config)
            .await
            .expect("failed to create SDLC handlers");
        registry
            .find_by_name("global_handler")
            .expect("global_handler not found")
    }

    async fn assert_edge_count(
        &self,
        relationship_kind: &str,
        source_kind: &str,
        target_kind: &str,
        expected_count: usize,
    ) {
        let query = format!(
            "SELECT source_id, target_id FROM gl_edge FINAL WHERE relationship_kind = '{relationship_kind}' \
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

    async fn assert_edge_count_for_traversal_path(
        &self,
        relationship_kind: &str,
        source_kind: &str,
        target_kind: &str,
        traversal_path: &str,
        expected_count: usize,
    ) {
        let query = format!(
            "SELECT 1 FROM gl_edge FINAL WHERE relationship_kind = '{relationship_kind}' \
             AND source_kind = '{source_kind}' AND target_kind = '{target_kind}' \
             AND traversal_path = '{traversal_path}'"
        );
        let result = self.query(&query).await;
        let actual_count = result.first().map_or(0, |b| b.num_rows());
        assert_eq!(
            actual_count, expected_count,
            "expected {expected_count} {relationship_kind} edges ({source_kind} → {target_kind}) \
             with traversal_path '{traversal_path}', got {actual_count}"
        );
    }

    async fn assert_edges_have_traversal_path(
        &self,
        relationship_kind: &str,
        source_kind: &str,
        target_kind: &str,
        expected_traversal_path: &str,
        expected_count: usize,
    ) {
        let query = format!(
            "SELECT traversal_path FROM gl_edge FINAL WHERE relationship_kind = '{relationship_kind}' \
             AND source_kind = '{source_kind}' AND target_kind = '{target_kind}'"
        );
        let result = self.query(&query).await;
        assert!(
            !result.is_empty(),
            "{relationship_kind} edges from {source_kind} to {target_kind} should exist"
        );
        let batch = &result[0];
        assert_eq!(
            batch.num_rows(),
            expected_count,
            "expected {expected_count} {relationship_kind} edges from {source_kind} to {target_kind}"
        );
        let paths = get_string_column(batch, "traversal_path");
        for i in 0..batch.num_rows() {
            assert_eq!(
                paths.value(i),
                expected_traversal_path,
                "{relationship_kind} edge row {i} should have traversal_path '{expected_traversal_path}'"
            );
        }
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
