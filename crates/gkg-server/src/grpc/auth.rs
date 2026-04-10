use tonic::{Request, Status};

use crate::auth::{Claims, JwtValidator};

pub fn extract_claims<T>(request: &Request<T>, validator: &JwtValidator) -> Result<Claims, Status> {
    let token = request
        .metadata()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .ok_or_else(|| Status::unauthenticated("Missing or invalid authorization header"))?;

    validator.validate(token).map_err(|e| {
        tracing::warn!(error = %e, "JWT validation failed");
        Status::unauthenticated(format!("JWT validation failed: {e}"))
    })
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

        let result = extract_claims(&request, &validator);
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

        let result = extract_claims(&request, &validator);
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

        let result = extract_claims(&request, &validator);
        assert!(result.is_err());

        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        assert!(status.message().contains("JWT validation failed"));
    }
}
