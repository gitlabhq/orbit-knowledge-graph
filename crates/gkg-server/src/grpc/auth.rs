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

    #[test]
    fn coding_agent_extracts_from_full_user_agent() {
        let ctx = RequestContext {
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
            user_agent: Some("glab/1.50.0 (linux, amd64) Coding-Agent/claude-code".into()),
        };
        assert_eq!(ctx.coding_agent(), Some("claude-code"));
    }

    #[test]
    fn coding_agent_none_when_absent() {
        let ctx = RequestContext {
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
            user_agent: Some("glab/1.50.0 (linux, amd64)".into()),
        };
        assert_eq!(ctx.coding_agent(), None);
    }

    #[test]
    fn coding_agent_none_when_no_user_agent() {
        let ctx = RequestContext {
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
            user_agent: None,
        };
        assert_eq!(ctx.coding_agent(), None);
    }
}
