use query_engine::compiler::{SecurityContext, TraversalPath};

use super::Claims;

const ADMIN_ORG_ROOT_ACCESS_LEVEL: u32 = 50;

pub fn build_security_context(claims: &Claims) -> Result<SecurityContext, String> {
    let org_id = claims
        .organization_id
        .ok_or("missing organization_id in claims")? as i64;

    let traversal_paths = if claims.admin {
        vec![TraversalPath::new(
            format!("{org_id}/"),
            ADMIN_ORG_ROOT_ACCESS_LEVEL,
        )]
    } else {
        if claims.group_traversal_ids.is_empty() {
            return Err("no enabled namespaces for this user".into());
        }
        claims
            .group_traversal_ids
            .iter()
            .map(|tp| TraversalPath::with_access_levels(tp.path.clone(), tp.access_levels.clone()))
            .collect()
    };

    SecurityContext::new_with_roles(org_id, traversal_paths)
        .map(|sc| sc.with_role(claims.admin, claims.min_access_level))
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::TraversalPathClaim;

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

    #[test]
    fn admin_gets_org_root_at_owner() {
        let claims = make_claims(true, vec![], Some(42));
        let ctx = build_security_context(&claims).unwrap();
        assert_eq!(ctx.org_id, 42);
        assert_eq!(ctx.traversal_paths.len(), 1);
        assert_eq!(ctx.traversal_paths[0].path, "42/");
        assert_eq!(
            ctx.traversal_paths[0].access_levels,
            vec![ADMIN_ORG_ROOT_ACCESS_LEVEL]
        );
    }

    #[test]
    fn missing_org_id_returns_error() {
        let claims = make_claims(true, vec![], None);
        let err = build_security_context(&claims).unwrap_err();
        assert!(err.contains("missing organization_id"));
    }

    #[test]
    fn non_admin_empty_paths_returns_error() {
        let claims = make_claims(false, vec![], Some(1));
        let err = build_security_context(&claims).unwrap_err();
        assert!(err.contains("no enabled namespaces"));
    }

    #[test]
    fn non_admin_maps_traversal_paths() {
        let claims = make_claims(
            false,
            vec![
                TraversalPathClaim {
                    path: "1/22/".to_string(),
                    access_levels: vec![20],
                },
                TraversalPathClaim {
                    path: "1/33/".to_string(),
                    access_levels: vec![30],
                },
            ],
            Some(1),
        );
        let ctx = build_security_context(&claims).unwrap();
        assert_eq!(ctx.org_id, 1);
        assert_eq!(ctx.paths_at_least(20), vec!["1/22/", "1/33/"]);
        assert_eq!(ctx.paths_at_least(30), vec!["1/33/"]);
        assert!(ctx.paths_at_least(50).is_empty());
    }

    #[test]
    fn non_admin_cross_org_paths() {
        let claims = make_claims(
            false,
            vec![
                TraversalPathClaim {
                    path: "1/22/".to_string(),
                    access_levels: vec![20],
                },
                TraversalPathClaim {
                    path: "2000271/122276018/".to_string(),
                    access_levels: vec![20],
                },
            ],
            Some(1),
        );
        let ctx = build_security_context(&claims).unwrap();
        assert_eq!(ctx.org_id, 1);
        assert_eq!(ctx.paths_at_least(20), vec!["1/22/", "2000271/122276018/"]);
    }

    #[test]
    fn non_admin_preserves_access_levels() {
        let claims = make_claims(
            false,
            vec![
                TraversalPathClaim {
                    path: "1/22/".to_string(),
                    access_levels: vec![20],
                },
                TraversalPathClaim {
                    path: "1/33/".to_string(),
                    access_levels: vec![20],
                },
            ],
            Some(1),
        );
        let ctx = build_security_context(&claims).unwrap();
        let paths: Vec<&str> = ctx
            .traversal_paths
            .iter()
            .map(|tp| tp.path.as_str())
            .collect();
        assert_eq!(paths, vec!["1/22/", "1/33/"]);
        assert!(
            ctx.traversal_paths
                .iter()
                .all(|tp| tp.access_levels == vec![20])
        );
    }

    #[test]
    fn exact_access_levels_propagate() {
        let claims = make_claims(
            false,
            vec![TraversalPathClaim {
                path: "1/22/".to_string(),
                access_levels: vec![20, 25],
            }],
            Some(1),
        );
        let ctx = build_security_context(&claims).unwrap();
        assert_eq!(ctx.traversal_paths[0].access_levels, vec![20, 25]);
        assert_eq!(ctx.paths_at_least(20), vec!["1/22/"]);
        assert_eq!(ctx.paths_at_least(25), vec!["1/22/"]);
        assert!(ctx.paths_at_least(30).is_empty());
    }
}
