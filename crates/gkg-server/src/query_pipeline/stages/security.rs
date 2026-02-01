use query_engine::SecurityContext;

use crate::auth::Claims;

pub struct SecurityStage;

impl SecurityStage {
    // Important note: We handle most filtering via the user's traversal paths.
    // If the user is not an admin, we filter by the user's group traversal paths.
    // We must ensure that if the traversal paths are empty, we also return an empty result.
    pub fn execute(claims: &Claims) -> Result<SecurityContext, SecurityError> {
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

#[derive(Debug)]
pub struct SecurityError(pub String);

impl std::fmt::Display for SecurityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for SecurityError {}

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
        let ctx = SecurityStage::execute(&claims).unwrap();

        assert_eq!(ctx.org_id, 42);
        assert_eq!(ctx.traversal_paths, vec!["42/"]);
    }

    #[test]
    fn missing_org_id_returns_error() {
        let claims = make_claims(true, vec![], None);
        let err = SecurityStage::execute(&claims).unwrap_err();

        assert!(err.to_string().contains("missing organization_id"));
    }

    #[test]
    fn admin_ignores_group_traversal_ids() {
        let claims = make_claims(
            true,
            vec!["1/22/".to_string(), "1/33/".to_string()],
            Some(1),
        );
        let ctx = SecurityStage::execute(&claims).unwrap();

        assert_eq!(ctx.traversal_paths, vec!["1/"]);
    }

    #[test]
    fn non_admin_gets_their_group_paths() {
        let claims = make_claims(
            false,
            vec!["1/22/".to_string(), "1/33/".to_string()],
            Some(1),
        );
        let ctx = SecurityStage::execute(&claims).unwrap();

        assert_eq!(ctx.traversal_paths, vec!["1/22/", "1/33/"]);
    }

    #[test]
    fn non_admin_with_empty_groups_gets_no_access() {
        let claims = make_claims(false, vec![], Some(1));
        let ctx = SecurityStage::execute(&claims).unwrap();

        assert!(ctx.traversal_paths.is_empty());
    }

    #[test]
    fn non_admin_with_single_group_path() {
        let claims = make_claims(false, vec!["1/24/111/".to_string()], Some(1));
        let ctx = SecurityStage::execute(&claims).unwrap();

        assert_eq!(ctx.traversal_paths, vec!["1/24/111/"]);
    }
}
