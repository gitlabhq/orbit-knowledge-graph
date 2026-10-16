use labkit::verbose_logs::can_log_verbose;
use orbit_billing::constants::normalize_realm;
use orbit_log_fields::{expanded, safe};
use tracing::field::Empty;

use super::Claims;
use crate::expanded_logs::VERBOSE_LOGS;

#[derive(Debug)]
pub struct RequestContext {
    pub claims: Claims,
    pub user_agent: Option<String>,
}

/// The span of one RPC. It declares every field that
/// [`RequestContext::record_in_current_span`] records after authentication.
pub(crate) fn request_span() -> tracing::Span {
    tracing::info_span!(
        "orbit_request",
        { safe::GL_USER_ID } = Empty,
        { safe::GL_ROOT_NAMESPACE_ID } = Empty,
        { safe::GL_GOVERNING_NAMESPACE_ID } = Empty,
        { safe::GL_ORGANIZATION_ID } = Empty,
        { safe::SOURCE_TYPE } = Empty,
        { safe::REALM } = Empty,
        { safe::EXPANDED_LOGS_REQUESTED } = Empty,
        { expanded::CLIENT_SESSION_ID } = Empty,
        { expanded::CLIENT_REQUEST_ID } = Empty,
        { expanded::CODING_AGENT } = Empty,
    )
}

impl RequestContext {
    pub fn coding_agent(&self) -> Option<&str> {
        self.user_agent.as_deref().and_then(|ua| {
            ua.split_whitespace()
                .find_map(|token| token.strip_prefix("Coding-Agent/"))
        })
    }

    pub fn record_in_current_span(&self) {
        let span = tracing::Span::current();
        span.record(safe::GL_USER_ID, self.claims.user_id);
        span.record(
            safe::GL_ROOT_NAMESPACE_ID,
            request_root_namespace_id(&self.claims),
        );
        span.record(
            safe::GL_GOVERNING_NAMESPACE_ID,
            self.claims.root_namespace_id,
        );
        span.record(safe::GL_ORGANIZATION_ID, self.claims.organization_id);
        span.record(safe::SOURCE_TYPE, <&str>::from(self.claims.source_type));
        span.record(
            safe::REALM,
            self.claims.realm.as_deref().and_then(normalize_realm),
        );
        span.record(
            safe::EXPANDED_LOGS_REQUESTED,
            can_log_verbose(&VERBOSE_LOGS),
        );
        span.record(
            expanded::CLIENT_SESSION_ID,
            self.claims.ai_session_id.as_deref(),
        );
        span.record(
            expanded::CLIENT_REQUEST_ID,
            self.claims.request_id.as_deref(),
        );
        span.record(expanded::CODING_AGENT, self.coding_agent());
    }
}

/// The root namespace that every traversal path of the token is under. A
/// token without paths (admins) or with paths under several roots has none.
fn request_root_namespace_id(claims: &Claims) -> Option<u64> {
    let mut roots = claims
        .group_traversal_ids
        .iter()
        .map(|claim| claim.path.top_level_namespace_id());
    let first = roots.next()??;
    let is_single_root = roots.all(|root| root == Some(first));
    is_single_root.then(|| u64::try_from(first).ok()).flatten()
}

#[cfg(test)]
mod tests {
    use orbit_utils::traversal_path::TraversalPath;

    use super::*;
    use crate::auth::TraversalPathClaim;

    fn claims_with_paths(paths: &[&str]) -> Claims {
        let group_traversal_ids = paths
            .iter()
            .map(|path| TraversalPathClaim {
                path: TraversalPath::new_unchecked(path.to_string()),
                access_levels: vec![30],
            })
            .collect();
        Claims {
            sub: String::new(),
            iss: String::new(),
            aud: String::new(),
            iat: 0,
            exp: 0,
            user_id: 1,
            username: String::new(),
            admin: false,
            organization_id: None,
            min_access_level: None,
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
            license_checksum: None,
        }
    }

    #[test]
    fn paths_under_one_root_name_that_root() {
        let claims = claims_with_paths(&["1/9970/", "1/9970/123/"]);

        assert_eq!(request_root_namespace_id(&claims), Some(9970));
    }

    #[test]
    fn paths_under_several_roots_name_no_root() {
        let claims = claims_with_paths(&["1/9970/", "1/42/"]);

        assert_eq!(request_root_namespace_id(&claims), None);
    }

    #[test]
    fn token_without_paths_names_no_root() {
        let claims = claims_with_paths(&[]);

        assert_eq!(request_root_namespace_id(&claims), None);
    }
}
