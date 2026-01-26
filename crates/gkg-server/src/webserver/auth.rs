use axum::{
    Json,
    body::Body,
    extract::{FromRequestParts, Request, State},
    http::{StatusCode, header::AUTHORIZATION, request::Parts},
    middleware::Next,
    response::{IntoResponse, Response},
};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use serde::{Deserialize, Serialize};

use crate::config::ServerConfig;
use crate::error::ServerError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub iss: String,
    pub aud: String,
    pub iat: i64,
    pub exp: i64,
    pub user_id: u64,
    pub username: String,
    #[serde(default)]
    pub admin: bool,
    #[serde(default)]
    pub organization_id: Option<u64>,
    #[serde(default)]
    pub min_access_level: Option<u32>,
    #[serde(default)]
    pub group_traversal_ids: Vec<String>,
    #[serde(default)]
    pub project_ids: Vec<u64>,
}

impl Claims {
    pub fn has_project_access(&self, project_id: u64) -> bool {
        self.project_ids.contains(&project_id)
    }
}

#[derive(Clone)]
pub struct JwtValidator {
    decoding_key: DecodingKey,
    validation: Validation,
}

const MIN_JWT_SECRET_LENGTH: usize = 32;

impl JwtValidator {
    pub fn new(config: &ServerConfig) -> Result<Self, ServerError> {
        if config.jwt_secret.len() < MIN_JWT_SECRET_LENGTH {
            return Err(ServerError::Config(format!(
                "JWT secret must be at least {} bytes",
                MIN_JWT_SECRET_LENGTH
            )));
        }
        let decoding_key = DecodingKey::from_secret(config.jwt_secret.as_bytes());
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_issuer(&[&config.jwt_issuer]);
        validation.set_audience(&[&config.jwt_audience]);
        validation.leeway = config.jwt_clock_skew_secs;
        Ok(Self {
            decoding_key,
            validation,
        })
    }

    pub fn validate(&self, token: &str) -> Result<Claims, ServerError> {
        decode::<Claims>(token, &self.decoding_key, &self.validation)
            .map(|data| data.claims)
            .map_err(|e| match e.kind() {
                jsonwebtoken::errors::ErrorKind::ExpiredSignature => ServerError::TokenExpired,
                _ => ServerError::InvalidToken(e.to_string()),
            })
    }
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
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
                Json(ErrorResponse {
                    error: "Missing Authorization header".into(),
                }),
            )
                .into_response()
        })?;

    let token = auth_header.strip_prefix("Bearer ").ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "Invalid Authorization format".into(),
            }),
        )
            .into_response()
    })?;

    let claims = validator.validate(token).map_err(|_| {
        (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "Invalid or expired token".into(),
            }),
        )
            .into_response()
    })?;

    request.extensions_mut().insert(claims);
    Ok(next.run(request).await)
}

#[derive(Debug, Clone)]
pub struct AuthenticatedUser(pub Claims);

impl<S: Send + Sync> FromRequestParts<S> for AuthenticatedUser {
    type Rejection = (StatusCode, Json<ErrorResponse>);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<Claims>()
            .cloned()
            .map(AuthenticatedUser)
            .ok_or((
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse {
                    error: "Not authenticated".into(),
                }),
            ))
    }
}
