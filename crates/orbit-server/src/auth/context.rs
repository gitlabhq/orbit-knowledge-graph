use super::Claims;

#[derive(Debug)]
pub(crate) struct RequestContext {
    pub claims: Claims,
    pub user_agent: Option<String>,
}

impl RequestContext {
    pub fn coding_agent(&self) -> Option<&str> {
        self.user_agent.as_deref().and_then(|user_agent| {
            user_agent
                .split_whitespace()
                .find_map(|token| token.strip_prefix("Coding-Agent/"))
        })
    }

    pub fn record_in_current_span(&self) {
        let span = tracing::Span::current();
        span.record("user_id", self.claims.user_id);
        span.record("source_type", <&str>::from(self.claims.source_type));
        if let Some(session_id) = &self.claims.ai_session_id {
            span.record("ai_session_id", session_id.as_str());
        }
        if let Some(request_id) = &self.claims.request_id {
            span.record("client_request_id", request_id.as_str());
        }
        if let Some(agent) = self.coding_agent() {
            span.record("coding_agent", agent);
        }
    }
}
