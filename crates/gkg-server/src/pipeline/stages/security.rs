use crate::auth::{Claims, build_security_context};

use query_engine::pipeline::{
    PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext,
};

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
        let claims = ctx.server_extensions.get::<Claims>().ok_or_else(|| {
            PipelineError::Security("Claims not found in server_extensions".into())
        })?;
        let result = build_security_context(claims)
            .map_err(PipelineError::Security)
            .inspect_err(|e| obs.record_error(e))?;
        ctx.security_context = Some(result);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::auth::claims::TraversalPathClaim;
    use ontology::Ontology;
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

    fn pipeline_context(claims: Claims) -> QueryPipelineContext {
        let mut extensions = TypeMap::default();
        extensions.insert(claims);
        QueryPipelineContext {
            query_json: String::new(),
            compiled: None,
            ontology: Arc::new(Ontology::load_embedded().unwrap()),
            security_context: None,
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
    }

    #[tokio::test]
    async fn populates_security_context_for_non_admin() {
        let claims = make_claims(
            false,
            vec![TraversalPathClaim {
                path: "1/22/".to_string(),
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
            ontology: Arc::new(Ontology::load_embedded().unwrap()),
            security_context: None,
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
        assert!(matches!(err, PipelineError::Security(_)));
    }
}
