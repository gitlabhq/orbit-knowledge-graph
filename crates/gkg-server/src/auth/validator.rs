// NOTE: Only HS256 (HMAC-SHA256) is supported. Do NOT add RSA algorithms
// (RS256, RS384, RS512, PS256, PS384, PS512) until jsonwebtoken updates to
// rsa 0.10+ which fixes RUSTSEC-2023-0071 (Marvin Attack timing vulnerability).

use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};

use super::{AuthError, Claims};

const ISSUER: &str = "gitlab";
const AUDIENCE: &str = "gitlab-knowledge-graph";
const MIN_SECRET_LENGTH: usize = 32;

#[derive(Clone)]
pub struct JwtValidator {
    decoding_key: DecodingKey,
    validation: Validation,
}

impl JwtValidator {
    pub fn new(secret: &str, clock_skew_secs: u64) -> Result<Self, AuthError> {
        if secret.is_empty() || secret.len() < MIN_SECRET_LENGTH {
            return Err(AuthError::InvalidConfig(format!(
                "JWT secret must be at least {} bytes",
                MIN_SECRET_LENGTH
            )));
        }

        let decoding_key = DecodingKey::from_secret(secret.as_bytes());
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_issuer(&[ISSUER]);
        validation.set_audience(&[AUDIENCE]);
        validation.leeway = clock_skew_secs;

        Ok(Self {
            decoding_key,
            validation,
        })
    }

    pub fn validate(&self, token: &str) -> Result<Claims, AuthError> {
        decode::<Claims>(token, &self.decoding_key, &self.validation)
            .map(|data| data.claims)
            .map_err(|e| match e.kind() {
                jsonwebtoken::errors::ErrorKind::ExpiredSignature => AuthError::TokenExpired,
                _ => AuthError::InvalidToken,
            })
    }
}
