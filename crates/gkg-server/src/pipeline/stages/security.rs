use query_engine::compiler::{SecurityContext, TraversalPath};
use thiserror::Error;

use crate::auth::Claims;

use query_engine::pipeline::{
    PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext,
};

/// Access level assigned to the admin org-root path. Matches `Gitlab::Access::OWNER`
/// so that every entity's `required_role` check passes for admins.
const ADMIN_ORG_ROOT_ACCESS_LEVEL: u32 = 50;

#[derive(Clone)]
pub struct SecurityStage;

impl SecurityStage {
    fn build_context(claims: &Claims) -> Result<SecurityContext, SecurityError> {
        let org_id = claims
            .organization_id
            .ok_or_else(|| SecurityError("missing organization_id in claims".to_string()))?
            as i64;
        let traversal_paths = if claims.admin {
            // Admins get the org-root path tagged Owner so every entity's
            // required_role check passes. This mirrors the pre-role-scoping
            // behavior where admins bypassed all filtering.
            vec![TraversalPath::new(
                format!("{org_id}/"),
                ADMIN_ORG_ROOT_ACCESS_LEVEL,
            )]
        } else {
            if claims.group_traversal_ids.is_empty() {
                return Err(SecurityError(
                    "no enabled namespaces for this user".to_string(),
                ));
            }
            claims
                .group_traversal_ids
                .iter()
                .map(|tp| TraversalPath::new(tp.path.clone(), tp.access_level))
                .collect()
        };
        SecurityContext::new_with_roles(org_id, traversal_paths)
            .map(|sc| sc.with_role(claims.admin, claims.min_access_level))
            .map_err(|e| SecurityError(e.to_string()))
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
    use crate::auth::claims::TraversalPathClaim;

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

    fn reporter(path: &str) -> TraversalPathClaim {
        TraversalPathClaim {
            path: path.to_string(),
            access_level: 20,
        }
    }

    fn developer(path: &str) -> TraversalPathClaim {
        TraversalPathClaim {
            path: path.to_string(),
            access_level: 30,
        }
    }

    fn paths(sc: &SecurityContext) -> Vec<String> {
        sc.traversal_paths
            .iter()
            .map(|tp| tp.path.clone())
            .collect()
    }

    #[test]
    fn admin_gets_org_wide_access() {
        let claims = make_claims(true, vec![], Some(42));
        let ctx = SecurityStage::build_context(&claims).unwrap();
        assert_eq!(ctx.org_id, 42);
        assert_eq!(paths(&ctx), vec!["42/".to_string()]);
        // Admin is tagged Owner so entity-level required_role gates always pass.
        assert_eq!(
            ctx.traversal_paths[0].access_level,
            ADMIN_ORG_ROOT_ACCESS_LEVEL
        );
    }

    #[test]
    fn missing_org_id_returns_error() {
        let claims = make_claims(true, vec![], None);
        let err = SecurityStage::build_context(&claims).unwrap_err();
        assert!(err.to_string().contains("missing organization_id"));
    }

    #[test]
    fn non_admin_gets_their_group_paths() {
        let claims = make_claims(false, vec![reporter("1/22/"), reporter("1/33/")], Some(1));
        let ctx = SecurityStage::build_context(&claims).unwrap();
        assert_eq!(paths(&ctx), vec!["1/22/".to_string(), "1/33/".to_string()]);
        assert!(ctx.traversal_paths.iter().all(|tp| tp.access_level == 20));
    }

    #[test]
    fn non_admin_with_empty_traversal_ids_returns_error() {
        let claims = make_claims(false, vec![], Some(1));
        let err = SecurityStage::build_context(&claims).unwrap_err();
        assert!(err.to_string().contains("no enabled namespaces"));
    }

    #[test]
    fn mixed_roles_propagate_into_context() {
        // Reporter on 1/22/, Developer on 1/33/. The compiler security pass
        // drops 1/22/ from any entity that requires Security Manager or higher
        // (e.g. Vulnerability).
        let claims = make_claims(false, vec![reporter("1/22/"), developer("1/33/")], Some(1));
        let ctx = SecurityStage::build_context(&claims).unwrap();

        assert_eq!(ctx.paths_at_least(20), vec!["1/22/", "1/33/"]);
        assert_eq!(ctx.paths_at_least(30), vec!["1/33/"]);
    }
}
