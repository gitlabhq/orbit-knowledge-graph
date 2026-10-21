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

pub(crate) fn request_span() -> tracing::Span {
    orbit_log_fields::info_span!(
        "orbit_request",
        safe(GL_USER_ID) = Empty,
        safe(GL_ROOT_NAMESPACE_ID) = Empty,
        safe(GL_GOVERNING_NAMESPACE_ID) = Empty,
        safe(GL_ORGANIZATION_ID) = Empty,
        safe(SOURCE_TYPE) = Empty,
        safe(REALM) = Empty,
        safe(EXPANDED_LOGS_REQUESTED) = Empty,
        expanded(CLIENT_SESSION_ID) = Empty,
        expanded(CLIENT_REQUEST_ID) = Empty,
        expanded(CODING_AGENT) = Empty,
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

/// Admins read the whole organization, so they have no single root namespace.
fn request_root_namespace_id(claims: &Claims) -> Option<u64> {
    if claims.admin {
        return None;
    }
    let mut roots = claims
        .group_traversal_ids
        .iter()
        .map(|claim| claim.path.top_level_namespace_id());
    let first = roots.next().flatten()?;
    if !roots.all(|root| root == Some(first)) {
        return None;
    }
    u64::try_from(first).ok()
}
