#![allow(dead_code, unused_imports)]

use std::collections::HashMap;

use gkg_server::redaction::{QueryResult, ResourceAuthorization, ResourceCheck};
use ontology::Ontology;
use query_engine::{CompiledQueryContext, SecurityContext, compile};

pub use integration_testkit::{
    GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext, get_boolean_column, get_int64_column,
    get_string_column, get_uint64_column,
};

pub fn load_ontology() -> Ontology {
    Ontology::load_embedded().expect("embedded ontology should load")
}

pub fn test_security_context() -> SecurityContext {
    SecurityContext::new(1, vec!["1/".into()]).expect("valid security context")
}

pub async fn compile_and_execute(
    ctx: &TestContext,
    json: &str,
) -> (CompiledQueryContext, QueryResult) {
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

impl Default for MockRedactionService {
    fn default() -> Self {
        Self::new()
    }
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

pub trait DummyClaims {
    fn dummy() -> Self;
}

impl DummyClaims for gkg_server::auth::Claims {
    fn dummy() -> Self {
        Self {
            sub: "user:1".into(),
            iss: "gitlab".into(),
            aud: "gitlab-knowledge-graph".into(),
            iat: 0,
            exp: i64::MAX,
            user_id: 1,
            username: "test".into(),
            admin: true,
            organization_id: Some(1),
            min_access_level: Some(20),
            group_traversal_ids: vec!["1/".into()],
        }
    }
}

pub fn run_redaction(result: &mut QueryResult, mock_service: &MockRedactionService) -> usize {
    let checks = result.resource_checks();
    let authorizations = mock_service.check(&checks);
    result.apply_authorizations(&authorizations)
}
