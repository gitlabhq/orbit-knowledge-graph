use query_engine::SecurityContext;
use thiserror::Error;

use crate::auth::Claims;

use querying_pipeline::{PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext};

#[derive(Clone)]
pub struct SecurityStage;

impl SecurityStage {
    fn build_context(claims: &Claims) -> Result<SecurityContext, SecurityError> {
        let org_id = claims
            .organization_id
            .ok_or_else(|| SecurityError("missing organization_id in claims".to_string()))?
            as i64;
        let traversal_paths = if claims.admin {
            vec![format!("{}/", org_id)]
        } else {
            claims.group_traversal_ids.clone()
        };
        SecurityContext::new(org_id, traversal_paths).map_err(|e| SecurityError(e.to_string()))
    }
}

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
        let result = Self::build_context(claims)
            .map_err(|e| PipelineError::Security(e.to_string()))
            .inspect_err(|e| obs.record_error(e))?;
        ctx.security_context = Some(result);
        Ok(())
    }
}

#[derive(Debug, Error)]
#[error("{0}")]
pub struct SecurityError(pub String);

#[cfg(test)]
mod tests {
    use super::*;

    fn make_claims(
        admin: bool,
        group_traversal_ids: Vec<String>,
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
        }
    }

    #[test]
    fn admin_gets_org_wide_access() {
        let claims = make_claims(true, vec![], Some(42));
        let ctx = SecurityStage::build_context(&claims).unwrap();
        assert_eq!(ctx.org_id, 42);
        assert_eq!(ctx.traversal_paths, vec!["42/"]);
    }

    #[test]
    fn missing_org_id_returns_error() {
        let claims = make_claims(true, vec![], None);
        let err = SecurityStage::build_context(&claims).unwrap_err();
        assert!(err.to_string().contains("missing organization_id"));
    }

    #[test]
    fn non_admin_gets_their_group_paths() {
        let claims = make_claims(false, vec!["1/22/".into(), "1/33/".into()], Some(1));
        let ctx = SecurityStage::build_context(&claims).unwrap();
        assert_eq!(ctx.traversal_paths, vec!["1/22/", "1/33/"]);
    }
}
