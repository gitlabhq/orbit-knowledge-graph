use super::Claims;

#[derive(Debug)]
pub struct RequestContext {
    pub claims: Claims,
    pub user_agent: Option<String>,
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
        span.record("source_type", <&str>::from(self.claims.source_type));
        if let Some(sid) = &self.claims.ai_session_id {
            span.record("ai_session_id", sid.as_str());
        }
        if let Some(rid) = &self.claims.request_id {
            span.record("client_request_id", rid.as_str());
        }
        if let Some(agent) = self.coding_agent() {
            span.record("coding_agent", agent);
        }
    }
}
