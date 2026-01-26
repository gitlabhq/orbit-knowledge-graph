use axum::{
    Json,
    body::Body,
    extract::{FromRequestParts, Request, State},
    http::{StatusCode, header::AUTHORIZATION, request::Parts},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde::Serialize;

use crate::auth::Claims;
use crate::auth::jwt::JwtValidator;
use crate::error::WebserverError;

const BEARER_PREFIX: &str = "Bearer ";

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    error: String,
}

impl ErrorResponse {
    fn new(message: impl Into<String>) -> Self {
        Self {
            error: message.into(),
        }
    }
}

fn extract_bearer_token(auth_header: &str) -> Result<&str, WebserverError> {
    if !auth_header.starts_with(BEARER_PREFIX) {
        return Err(WebserverError::InvalidAuthFormat(
            "Authorization header must start with 'Bearer '".into(),
        ));
    }

    Ok(auth_header.trim_start_matches(BEARER_PREFIX))
}

pub async fn auth_middleware(
    State(validator): State<JwtValidator>,
    mut request: Request<Body>,
    next: Next,
) -> Result<Response, Response> {
    let auth_header = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse::new("Missing Authorization header")),
            )
                .into_response()
        })?;

    let token = extract_bearer_token(auth_header).map_err(|e| {
        (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse::new(e.to_string())),
        )
            .into_response()
    })?;

    let claims = validator.validate(token).map_err(|e| {
        let status = match &e {
            WebserverError::TokenExpired => StatusCode::UNAUTHORIZED,
            WebserverError::InvalidToken(_) => StatusCode::UNAUTHORIZED,
            WebserverError::MissingAuth => StatusCode::UNAUTHORIZED,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(ErrorResponse::new(e.to_string()))).into_response()
    })?;

    request.extensions_mut().insert(claims);

    Ok(next.run(request).await)
}

#[derive(Debug, Clone)]
pub struct AuthenticatedUser(pub Claims);

impl AuthenticatedUser {
    pub fn claims(&self) -> &Claims {
        &self.0
    }

    pub fn into_claims(self) -> Claims {
        self.0
    }
}

impl<S> FromRequestParts<S> for AuthenticatedUser
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, Json<ErrorResponse>);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<Claims>()
            .cloned()
            .map(AuthenticatedUser)
            .ok_or_else(|| {
                (
                    StatusCode::UNAUTHORIZED,
                    Json(ErrorResponse::new("Not authenticated")),
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_bearer_token_valid() {
        let token = extract_bearer_token("Bearer my-token-here");
        assert!(token.is_ok());
        assert_eq!(token.unwrap(), "my-token-here");
    }

    #[test]
    fn test_extract_bearer_token_missing_prefix() {
        let token = extract_bearer_token("my-token-here");
        assert!(token.is_err());
    }

    #[test]
    fn test_extract_bearer_token_wrong_prefix() {
        let token = extract_bearer_token("Basic my-token-here");
        assert!(token.is_err());
    }
}
