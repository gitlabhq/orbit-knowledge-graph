#![allow(dead_code, unused_imports)]

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use gkg_server::redaction::{QueryResult, ResourceAuthorization, ResourceCheck};
use indexer::clickhouse::ClickHouseDestination;
use indexer::handler::{Handler, HandlerContext, HandlerRegistry};
use indexer::metrics::EngineMetrics;
use indexer::testkit::{MockLockService, MockNatsServices, create_test_indexer_config};
use ontology::Ontology;
use query_engine::{CompiledQuery, SecurityContext, compile};

pub use integration_testkit::{
    TestContext, get_boolean_column, get_int64_column, get_string_column, get_uint64_column,
};

pub const SIPHON_SCHEMA_SQL: &str = include_str!("../fixtures/siphon.sql");
pub const GRAPH_SCHEMA_SQL: &str = include_str!("../../../../fixtures/schema/graph.sql");

pub trait GkgServerTestExt {
    fn create_destination(&self) -> ClickHouseDestination;
    fn create_handler_context(&self) -> HandlerContext;
    async fn get_namespace_handler(&self) -> Arc<dyn Handler>;
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

    async fn get_namespace_handler(&self) -> Arc<dyn Handler> {
        let indexer_config = create_test_indexer_config(&self.config);
        let registry = HandlerRegistry::default();
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        indexer::modules::sdlc::register_handlers(&registry, &indexer_config, &ontology)
            .await
            .expect("failed to create SDLC handlers");
        registry
            .find_by_name("namespace_handler")
            .expect("namespace_handler not found")
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

// ─────────────────────────────────────────────────────────────────────────────
// Shared test helpers for redaction / hydration integration tests
// ─────────────────────────────────────────────────────────────────────────────

pub fn load_ontology() -> Ontology {
    Ontology::load_embedded().expect("embedded ontology should load")
}

pub fn test_security_context() -> SecurityContext {
    SecurityContext::new(1, vec!["1/".into()]).expect("valid security context")
}

pub async fn compile_and_execute(ctx: &TestContext, json: &str) -> (CompiledQuery, QueryResult) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();
    let compiled = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&compiled.base).await;
    let result = QueryResult::from_batches(&batches, &compiled.base.result_context);
    (compiled, result)
}

pub struct MockRedactionService {
    pub authorizations: HashMap<String, HashMap<i64, bool>>,
}

impl MockRedactionService {
    pub fn new() -> Self {
        Self {
            authorizations: HashMap::new(),
        }
    }

    pub fn allow(&mut self, resource_type: &str, ids: &[i64]) {
        let map = self
            .authorizations
            .entry(resource_type.to_string())
            .or_default();
        for id in ids {
            map.insert(*id, true);
        }
    }

    pub fn deny(&mut self, resource_type: &str, ids: &[i64]) {
        let map = self
            .authorizations
            .entry(resource_type.to_string())
            .or_default();
        for id in ids {
            map.insert(*id, false);
        }
    }

    pub fn check(&self, checks: &[ResourceCheck]) -> Vec<ResourceAuthorization> {
        checks
            .iter()
            .map(|check| {
                let authorized = check
                    .ids
                    .iter()
                    .map(|id| {
                        let allowed = self
                            .authorizations
                            .get(&check.resource_type)
                            .and_then(|m| m.get(id))
                            .copied()
                            .unwrap_or(false);
                        (*id, allowed)
                    })
                    .collect();

                ResourceAuthorization {
                    resource_type: check.resource_type.clone(),
                    authorized,
                }
            })
            .collect()
    }
}

pub fn run_redaction(result: &mut QueryResult, mock_service: &MockRedactionService) -> usize {
    let checks = result.resource_checks();
    let authorizations = mock_service.check(&checks);
    result.apply_authorizations(&authorizations)
}
