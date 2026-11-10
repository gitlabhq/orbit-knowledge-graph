use tonic::{Request, Status};

use crate::auth::{JwtValidator, RequestContext};
use crate::user_agent::UserAgent;

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
        orbit_log_fields::warn!(
            safe(ERROR_TYPE) = <&str>::from(&e),
            expanded(ERROR_MESSAGE) = %e,
            "JWT validation failed"
        );
        Status::unauthenticated(format!("JWT validation failed: {e}"))
    })?;

    let user_agent = request
        .metadata()
        .get("x-client-user-agent")
        .and_then(|v| v.to_str().ok());

    let ctx = RequestContext {
        claims,
        user_agent: UserAgent::parse(user_agent),
    };
    ctx.record_in_current_span();

    let claims = &ctx.claims;
    orbit_log_fields::info!(
        safe(DEPLOYMENT_TYPE) = claims.deployment_type.as_deref(),
        safe(INSTANCE_ID) = claims.instance_id.as_deref(),
        safe(UNIQUE_INSTANCE_ID) = claims.unique_instance_id.as_deref(),
        safe(INSTANCE_VERSION) = claims.instance_version.as_deref(),
        safe(LICENSE_CHECKSUM_PRESENT) = claims.license_checksum.is_some(),
        expanded(GLOBAL_USER_ID) = claims.global_user_id.as_deref(),
        "JWT claims received"
    );

    Ok(ctx)
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
}
