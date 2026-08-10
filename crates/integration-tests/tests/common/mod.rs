#![allow(dead_code, unused_imports)]

use gkg_server::redaction::QueryResult;
use ontology::Ontology;
use query_engine::compiler::{AccessLevel, CompiledQueryContext, SecurityContext, TraversalPath};

pub use integration_testkit::mock_redaction::MockRedactionService;
pub use integration_testkit::{GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext, load_ontology};

pub fn test_security_context() -> SecurityContext {
    SecurityContext::new(1, vec!["1/".into()]).expect("valid security context")
}

pub fn admin_security_context() -> SecurityContext {
    SecurityContext::new_with_roles(1, vec![TraversalPath::new("1/", AccessLevel::Owner as u32)])
        .expect("valid admin security context")
        .with_role(true, Some(AccessLevel::Owner as u32))
}

pub fn compile(
    json: &str,
    ontology: &Ontology,
    security_ctx: &SecurityContext,
) -> query_engine::compiler::Result<CompiledQueryContext> {
    let mut input: serde_json::Value = serde_json::from_str(json)?;
    input["code_contexts"] = serde_json::json!([{
        "project_id": 1000,
        "ref": "main",
        "commit_sha": "abc123",
        "base_ref": "main",
        "indexed_sha": "abc123",
        "base_sha": "abc123",
        "generation": 1,
        "state": "ready"
    }]);
    query_engine::compiler::compile(&input.to_string(), ontology, security_ctx)
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
            min_access_level: Some(AccessLevel::Owner as u32),
            group_traversal_ids: vec![gkg_server::auth::TraversalPathClaim {
                path: "1/".into(),
                access_levels: vec![AccessLevel::Owner as u32],
            }],
            source_type: gkg_server::auth::SourceType::Rest,
            ai_session_id: None,
            instance_id: None,
            unique_instance_id: None,
            instance_version: None,
            global_user_id: None,
            host_name: None,
            root_namespace_id: None,
            deployment_type: None,
            realm: None,
            is_gitlab_team_member: None,
        }
    }
}

pub fn run_redaction(result: &mut QueryResult, mock_service: &MockRedactionService) -> usize {
    let checks = result.resource_checks();
    let authorizations = mock_service.check(&checks);
    result.apply_authorizations(&authorizations)
}
