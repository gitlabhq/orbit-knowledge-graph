use std::sync::Arc;

use orbit_authorization::{AuthError, Request};

use crate::auth::Claims;
use crate::authorization::Deployment;
use crate::authorization::gitlab::{GitLabCaller, RailsExchange, VerifiedRequest};

use query_engine::pipeline::{
    PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext,
};

/// Identifies the caller through the deployment's identify provider and resolves the scope
/// answers its reads need.
#[derive(Clone)]
pub struct SecurityStage;

impl PipelineStage for SecurityStage {
    type Input = ();
    type Output = ();

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let missing =
            |what: &str| PipelineError::Security(format!("{what} not found in server_extensions"));
        let claims = ctx
            .server_extensions
            .get::<Claims>()
            .ok_or_else(|| missing("Claims"));
        let deployment = ctx
            .server_extensions
            .get::<Arc<Deployment>>()
            .ok_or_else(|| missing("authorization"));
        let (claims, deployment) = match (claims, deployment) {
            (Ok(claims), Ok(deployment)) => (claims.clone(), Arc::clone(deployment)),
            (Err(error), _) | (_, Err(error)) => {
                obs.record_error(&error);
                return Err(error);
            }
        };
        let exchange = ctx.server_extensions.get::<Arc<RailsExchange>>().cloned();
        let request = Request::new(VerifiedRequest { claims, exchange });
        let resolved = async {
            let caller = deployment.identify(request).await?;
            let grants = deployment.grants(&caller).await?;
            Ok::<_, AuthError>((caller, grants))
        };
        let (caller, grants) = resolved
            .await
            .map_err(security_error)
            .inspect_err(|e| obs.record_error(e))?;
        ctx.security_context = caller
            .session::<GitLabCaller>()
            .map(|session| session.context.clone());
        ctx.grants = Some(grants);
        ctx.server_extensions.insert(caller);
        Ok(())
    }
}

fn security_error(error: AuthError) -> PipelineError {
    match error {
        AuthError::NoScopes => PipelineError::NoEnabledNamespaces,
        AuthError::Denied(message)
        | AuthError::Invalid(message)
        | AuthError::Unavailable(message) => PipelineError::Security(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::claims::TraversalPathClaim;
    use query_engine::pipeline::{NoOpObserver, TypeMap};

    fn make_claims(
        admin: bool,
        group_traversal_ids: Vec<TraversalPathClaim>,
        organization_id: Option<u64>,
    ) -> Claims {
        Claims {
            sub: "user:1".to_string(),
            iss: "gitlab".to_string(),
            aud: "gitlab-knowledge-graph".to_string(),
            iat: 0,
            exp: i64::MAX,
            user_id: 1,
            username: "test_user".to_string(),
            admin,
            organization_id,
            min_access_level: Some(20),
            group_traversal_ids,
            source_type: crate::auth::SourceType::Rest,
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

    fn pipeline_context(claims: Claims) -> QueryPipelineContext {
        let mut extensions = TypeMap::default();
        extensions.insert(claims);
        extensions.insert(Arc::new(crate::authorization::tests::deployment()));
        QueryPipelineContext {
            query_json: String::new(),
            compiled: None,
            security_context: None,
            grants: None,
            server_extensions: extensions,
            phases: TypeMap::default(),
        }
    }

    #[tokio::test]
    async fn populates_security_context_for_admin() {
        let claims = make_claims(true, vec![], Some(42));
        let mut ctx = pipeline_context(claims);
        let mut obs = NoOpObserver;

        SecurityStage.execute(&mut ctx, &mut obs).await.unwrap();

        let sc = ctx.security_context.unwrap();
        assert_eq!(sc.org_id, 42);
        assert_eq!(sc.traversal_paths[0].path, "42/");
        let grants = ctx.grants.unwrap();
        assert!(grants.caller().admin());
        assert_eq!(
            grants.scope_set("namespace", "read_security").unwrap(),
            &orbit_authorization::ScopeSet::Subtrees(vec!["42/".into()])
        );
    }

    #[tokio::test]
    async fn populates_security_context_for_non_admin() {
        let claims = make_claims(
            false,
            vec![TraversalPathClaim {
                path: orbit_utils::traversal_path::TraversalPath::new_unchecked("1/22/"),
                access_levels: vec![20],
            }],
            Some(1),
        );
        let mut ctx = pipeline_context(claims);
        let mut obs = NoOpObserver;

        SecurityStage.execute(&mut ctx, &mut obs).await.unwrap();

        let sc = ctx.security_context.unwrap();
        assert_eq!(sc.traversal_paths[0].path, "1/22/");
    }

    #[tokio::test]
    async fn missing_claims_returns_security_error() {
        let mut ctx = QueryPipelineContext {
            query_json: String::new(),
            compiled: None,
            security_context: None,
            grants: None,
            server_extensions: TypeMap::default(),
            phases: TypeMap::default(),
        };
        let mut obs = NoOpObserver;

        let err = SecurityStage.execute(&mut ctx, &mut obs).await.unwrap_err();
        assert!(matches!(err, PipelineError::Security(_)));
    }

    #[tokio::test]
    async fn invalid_claims_returns_security_error() {
        let claims = make_claims(false, vec![], Some(1));
        let mut ctx = pipeline_context(claims);
        let mut obs = NoOpObserver;

        let err = SecurityStage.execute(&mut ctx, &mut obs).await.unwrap_err();
        assert!(matches!(err, PipelineError::NoEnabledNamespaces));
    }
}
