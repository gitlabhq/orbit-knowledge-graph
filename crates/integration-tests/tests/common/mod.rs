#![allow(dead_code, unused_imports)]

use std::sync::Arc;

use orbit_server::pipeline::HydrationStage;
use orbit_server::redaction::QueryResult;
use query_engine::compiler::CompiledQueryContext;
pub use query_engine::compiler::compile;
use query_engine::compiler::orbit_authorization::Grants;
use query_engine::pipeline::{
    AccessLevel, AuthorizedPath, QueryPipelineContext, SecurityContext, TypeMap,
};

pub use integration_testkit::mock_redaction::MockRedactionService;
pub use integration_testkit::{
    GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext, edge_table_for, load_data_model,
};

/// The scope answers the default deployment's `gitlab.session` gives `session`.
pub fn grants(session: SecurityContext) -> Grants {
    let config = orbit_server_config::AppConfig::embedded_defaults().authorization;
    orbit_server::authorization::gitlab::session_grants(&config, load_data_model().model(), session)
        .expect("the default deployment answers the session")
}

pub fn test_session() -> SecurityContext {
    SecurityContext::new(1, vec!["1/".into()]).expect("valid security context")
}

pub fn test_security_context() -> Grants {
    grants(test_session())
}

pub fn admin_security_context() -> Grants {
    grants(
        SecurityContext::new_with_roles(
            1,
            vec![AuthorizedPath::new("1/", AccessLevel::Owner as u32)],
        )
        .expect("valid admin security context")
        .with_role(true, Some(AccessLevel::Owner as u32)),
    )
}

pub async fn compile_and_execute(
    ctx: &TestContext,
    query: &str,
) -> (CompiledQueryContext, QueryResult) {
    let data_model = load_data_model();
    let security_ctx = test_security_context();
    let compiled = compile(
        query,
        &data_model,
        &security_ctx,
        query_data_model::testkit::limits(),
    )
    .unwrap();
    let batches = ctx.query_parameterized(&compiled.base).await;
    let result = QueryResult::from_batches(&batches, &compiled.base.result_context).unwrap();
    (compiled, result)
}

pub trait DummyClaims {
    fn dummy() -> Self;
}

impl DummyClaims for orbit_server::auth::Claims {
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
            group_traversal_ids: vec![orbit_server::auth::TraversalPathClaim {
                path: orbit_utils::traversal_path::TraversalPath::new_unchecked("1/"),
                access_levels: vec![AccessLevel::Owner as u32],
            }],
            source_type: orbit_server::auth::SourceType::Rest,
            ai_session_id: None,
            request_id: None,
            instance_id: None,
            unique_instance_id: None,
            instance_version: None,
            global_user_id: None,
            host_name: None,
            root_namespace_id: None,
            deployment_type: None,
            realm: None,
            is_gitlab_team_member: None,
            user_type: None,
            token_type: None,
            license_checksum: None,
        }
    }
}

pub fn run_redaction(result: &mut QueryResult, mock_service: &MockRedactionService) -> usize {
    let checks = result.resource_checks();
    let authorizations = mock_service.check(&checks);
    result.apply_authorizations(&authorizations)
}

pub async fn resolve_owners(
    ctx: &TestContext,
    compiled: &CompiledQueryContext,
    result: &mut QueryResult,
) {
    let mut server_extensions = TypeMap::default();
    server_extensions.insert(Arc::new(ctx.create_client()));
    server_extensions.insert(load_data_model());
    server_extensions.insert(*query_data_model::testkit::limits());
    let mut pipeline_ctx = QueryPipelineContext {
        query_json: String::new(),
        compiled: Some(Arc::new(compiled.clone())),
        security_context: Some(test_session()),
        grants: Some(test_security_context()),
        server_extensions,
        phases: TypeMap::default(),
    };
    HydrationStage::resolve_keys(&mut pipeline_ctx, result)
        .await
        .expect("owner lookup should succeed");
}
