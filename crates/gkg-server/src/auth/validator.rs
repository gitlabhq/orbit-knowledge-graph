// SECURITY: Only HS256 (HMAC-SHA256) is supported. Do NOT add RSA algorithms
// (RS256, RS384, RS512, PS256, PS384, PS512) until jsonwebtoken updates to
// rsa 0.10+ which fixes RUSTSEC-2023-0071 (Marvin Attack timing vulnerability).

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};

use super::{AuthError, Claims};

// Both sides use the same issuer/audience: Rails signs and validates
// with iss="gitlab", aud="gitlab-knowledge-graph".
const EXPECTED_ISSUER: &str = "gitlab";
const EXPECTED_AUDIENCE: &str = "gitlab-knowledge-graph";
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

        // Rails base64-decodes the secret file before signing JWTs
        // (via Gitlab::JwtAuthenticatable). Decode here to match.
        let decoded = STANDARD
            .decode(secret.trim().as_bytes())
            .unwrap_or_else(|_| secret.as_bytes().to_vec());
        let decoding_key = DecodingKey::from_secret(&decoded);
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_issuer(&[EXPECTED_ISSUER]);
        validation.set_audience(&[EXPECTED_AUDIENCE]);
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
                _ => AuthError::InvalidToken(e.to_string()),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header, encode};

    #[test]
    fn validates_token_signed_with_decoded_base64_secret() {
        let raw_secret = b"test-secret-that-is-at-least-32-bytes-long";
        let base64_secret = STANDARD.encode(raw_secret);

        let validator = JwtValidator::new(&base64_secret, 0).unwrap();

        let now = chrono::Utc::now().timestamp();
        let claims = Claims {
            sub: "user".into(),
            iss: EXPECTED_ISSUER.into(),
            aud: EXPECTED_AUDIENCE.into(),
            iat: now,
            exp: now + 3600,
            user_id: 1,
            username: "testuser".into(),
            admin: false,
            organization_id: None,
            min_access_level: None,
            group_traversal_ids: vec![],
            source_type: "rest".into(),
            ai_session_id: None,
            instance_id: None,
            unique_instance_id: None,
            instance_version: None,
            global_user_id: None,
            host_name: None,
            root_namespace_id: None,
            deployment_type: None,
            realm: None,
            feature_qualified_name: None,
            feature_enablement_type: None,
        };

        let token = encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(raw_secret),
        )
        .unwrap();

        let result = validator.validate(&token);
        assert!(result.is_ok(), "expected valid token, got: {result:?}");
        let validated = result.unwrap();
        assert_eq!(validated.username, "testuser");
        assert_eq!(validated.source_type, "rest");
    }

    #[test]
    fn rejects_token_missing_source_type() {
        let raw_secret = b"test-secret-that-is-at-least-32-bytes-long";
        let base64_secret = STANDARD.encode(raw_secret);
        let validator = JwtValidator::new(&base64_secret, 0).unwrap();

        let now = chrono::Utc::now().timestamp();
        let payload = serde_json::json!({
            "sub": "user:1",
            "iss": EXPECTED_ISSUER,
            "aud": EXPECTED_AUDIENCE,
            "iat": now,
            "exp": now + 3600,
            "user_id": 1,
            "username": "testuser",
            "admin": false
        });

        let token = encode(
            &Header::new(Algorithm::HS256),
            &payload,
            &EncodingKey::from_secret(raw_secret),
        )
        .unwrap();

        let result = validator.validate(&token);
        assert!(
            result.is_err(),
            "expected rejection for missing source_type"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("source_type"),
            "error should mention source_type, got: {err}"
        );
    }
}
