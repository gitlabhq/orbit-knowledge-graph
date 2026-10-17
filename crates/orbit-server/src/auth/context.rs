use labkit::verbose_logs::can_log_verbose;
use orbit_billing::constants::normalize_realm;
use tracing::field::Empty;

use super::Claims;
use crate::expanded_logs::VERBOSE_LOGS;

#[derive(Debug)]
pub struct RequestContext {
    pub claims: Claims,
    pub user_agent: Option<String>,
}

pub(crate) fn request_span() -> tracing::Span {
    tracing::info_span!(
        "orbit_request",
        user_id = Empty,
        gl_root_namespace_id = Empty,
        gl_governing_namespace_id = Empty,
        gl_organization_id = Empty,
        source_type = Empty,
        realm = Empty,
        expanded_logs_requested = Empty,
        ai_session_id = Empty,
        client_request_id = Empty,
        coding_agent = Empty,
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
        span.record("user_id", self.claims.user_id);
        span.record(
            "gl_root_namespace_id",
            request_root_namespace_id(&self.claims),
        );
        span.record("gl_governing_namespace_id", self.claims.root_namespace_id);
        span.record("gl_organization_id", self.claims.organization_id);
        span.record("source_type", <&str>::from(self.claims.source_type));
        span.record(
            "realm",
            self.claims.realm.as_deref().and_then(normalize_realm),
        );
        span.record("expanded_logs_requested", can_log_verbose(&VERBOSE_LOGS));
        span.record("ai_session_id", self.claims.ai_session_id.as_deref());
        span.record("client_request_id", self.claims.request_id.as_deref());
        span.record("coding_agent", self.coding_agent());
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
