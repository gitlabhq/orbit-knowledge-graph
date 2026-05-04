#![allow(dead_code, unused_imports)]

use gkg_server::redaction::QueryResult;
use ontology::Ontology;
pub use query_engine::compiler::compile;
use query_engine::compiler::{CompiledQueryContext, SecurityContext};

pub use integration_testkit::mock_redaction::MockRedactionService;
pub use integration_testkit::{GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext, load_ontology};

pub fn test_security_context() -> SecurityContext {
    SecurityContext::new(1, vec!["1/".into()]).expect("valid security context")
}

pub fn admin_security_context() -> SecurityContext {
    test_security_context().with_role(true, None)
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
            group_traversal_ids: vec![gkg_server::auth::TraversalPathClaim {
                path: "1/".into(),
                access_levels: vec![20],
            }],
            source_type: "rest".into(),
            ai_session_id: None,
            instance_id: None,
            unique_instance_id: None,
            instance_version: None,
            global_user_id: None,
            host_name: None,
            root_namespace_id: None,
            deployment_type: None,
            realm: None,
        }
    }
}

pub fn run_redaction(result: &mut QueryResult, mock_service: &MockRedactionService) -> usize {
    let checks = result.resource_checks();
    let authorizations = mock_service.check(&checks);
    result.apply_authorizations(&authorizations)
}
