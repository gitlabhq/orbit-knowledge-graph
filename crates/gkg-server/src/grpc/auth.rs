use tonic::{Request, Status};

use crate::auth::{Claims, JwtValidator};

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
}

pub fn extract_request_context<T>(
    request: &Request<T>,
    validator: &JwtValidator,
) -> Result<RequestContext, Status> {
    let token = request
        .metadata()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .ok_or_else(|| Status::unauthenticated("Missing or invalid authorization header"))?;

    let claims = validator.validate(token).map_err(|e| {
        tracing::warn!(error = %e, "JWT validation failed");
        Status::unauthenticated(format!("JWT validation failed: {e}"))
    })?;

    let user_agent = request
        .metadata()
        .get("x-client-user-agent")
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    Ok(RequestContext { claims, user_agent })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    fn mock_validator() -> JwtValidator {
        JwtValidator::new("test-secret-that-is-at-least-32-bytes-long", 0).unwrap()
    }

    #[test]
    fn test_missing_authorization_header() {
        let request: Request<()> = Request::new(());
        let validator = mock_validator();

        let result = extract_request_context(&request, &validator);
        assert!(result.is_err());

        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        assert!(status.message().contains("Missing"));
    }

    #[test]
    fn test_invalid_authorization_format() {
        let mut request: Request<()> = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from("Basic dXNlcjpwYXNz").unwrap(),
        );
        let validator = mock_validator();

        let result = extract_request_context(&request, &validator);
        assert!(result.is_err());

        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn test_invalid_token() {
        let mut request: Request<()> = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from("Bearer invalid.token.here").unwrap(),
        );
        let validator = mock_validator();

        let result = extract_request_context(&request, &validator);
        assert!(result.is_err());

        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        assert!(status.message().contains("JWT validation failed"));
    }

    fn request_context(user_agent: Option<&str>) -> RequestContext {
        RequestContext {
            claims: crate::auth::Claims {
                sub: String::new(),
                iss: String::new(),
                aud: String::new(),
                iat: 0,
                exp: 0,
                user_id: 0,
                username: String::new(),
                admin: false,
                organization_id: None,
                min_access_level: None,
                group_traversal_ids: vec![],
                source_type: String::new(),
                ai_session_id: None,
                instance_id: None,
                unique_instance_id: None,
                instance_version: None,
                global_user_id: None,
                host_name: None,
                root_namespace_id: None,
                deployment_type: None,
                realm: None,
            },
            user_agent: user_agent.map(Into::into),
        }
    }

    #[test]
    fn coding_agent_extracts_known_agents() {
        let cases = [
            (
                "glab/1.50.0 (linux, amd64) Coding-Agent/claude-code",
                Some("claude-code"),
            ),
            (
                "glab/1.50.0 (darwin, arm64) Coding-Agent/codex",
                Some("codex"),
            ),
            (
                "glab/1.50.0 (windows, amd64) Coding-Agent/cursor",
                Some("cursor"),
            ),
            (
                "glab/1.50.0 (linux, amd64) Coding-Agent/opencode",
                Some("opencode"),
            ),
            (
                "glab/DEV (linux, amd64) Coding-Agent/custom-agent_2.1",
                Some("custom-agent_2.1"),
            ),
        ];
        for (user_agent, expected) in cases {
            let ctx = request_context(Some(user_agent));
            assert_eq!(ctx.coding_agent(), expected, "for user_agent: {user_agent}");
        }
    }

    #[test]
    fn coding_agent_none_when_absent() {
        let ctx = request_context(Some("glab/1.50.0 (linux, amd64)"));
        assert_eq!(ctx.coding_agent(), None);
    }

    #[test]
    fn coding_agent_none_when_no_user_agent() {
        let ctx = request_context(None);
        assert_eq!(ctx.coding_agent(), None);
    }
}
