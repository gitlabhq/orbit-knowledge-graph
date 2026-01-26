use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};

use crate::auth::Claims;
use crate::config::WebserverConfig;
use crate::error::WebserverError;

#[derive(Clone)]
pub struct JwtValidator {
    decoding_key: DecodingKey,
    validation: Validation,
}

impl JwtValidator {
    pub fn new(config: &WebserverConfig) -> Result<Self, WebserverError> {
        if config.jwt_secret.is_empty() {
            return Err(WebserverError::Config("JWT secret cannot be empty".into()));
        }

        let decoding_key = DecodingKey::from_secret(config.jwt_secret.as_bytes());

        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_issuer(&[&config.jwt_issuer]);
        validation.set_audience(&[&config.jwt_audience]);
        validation.leeway = config.jwt_clock_skew_secs;
        validation.validate_exp = true;

        Ok(Self {
            decoding_key,
            validation,
        })
    }

    pub fn validate(&self, token: &str) -> Result<Claims, WebserverError> {
        let token_data = decode::<Claims>(token, &self.decoding_key, &self.validation)
            .map_err(|e| match e.kind() {
                jsonwebtoken::errors::ErrorKind::ExpiredSignature => WebserverError::TokenExpired,
                jsonwebtoken::errors::ErrorKind::InvalidToken
                | jsonwebtoken::errors::ErrorKind::InvalidSignature => {
                    WebserverError::InvalidToken(e.to_string())
                }
                jsonwebtoken::errors::ErrorKind::InvalidIssuer => {
                    WebserverError::InvalidToken("invalid issuer".into())
                }
                jsonwebtoken::errors::ErrorKind::InvalidAudience => {
                    WebserverError::InvalidToken("invalid audience".into())
                }
                _ => WebserverError::Auth(e.to_string()),
            })?;

        Ok(token_data.claims)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header, encode};

    fn create_test_config() -> WebserverConfig {
        WebserverConfig {
            bind_address: "0.0.0.0:8080".to_string(),
            jwt_secret: "test-secret-key".to_string(),
            jwt_issuer: "gitlab".to_string(),
            jwt_audience: "gitlab-knowledge-graph".to_string(),
            jwt_clock_skew_secs: 60,
        }
    }

    fn create_test_token(claims: &Claims, secret: &str) -> String {
        let encoding_key = EncodingKey::from_secret(secret.as_bytes());
        encode(&Header::default(), claims, &encoding_key).unwrap()
    }

    fn create_valid_claims() -> Claims {
        let now = chrono::Utc::now().timestamp();
        Claims {
            sub: "user:123".to_string(),
            iss: "gitlab".to_string(),
            aud: "gitlab-knowledge-graph".to_string(),
            iat: now,
            exp: now + 3600,
            user_id: 123,
            username: "testuser".to_string(),
            admin: false,
            organization_id: Some(1),
            min_access_level: Some(10),
            group_traversal_ids: vec![],
            project_ids: vec![1, 2, 3],
        }
    }

    #[test]
    fn test_validator_creation() {
        let config = create_test_config();
        let validator = JwtValidator::new(&config);
        assert!(validator.is_ok());
    }

    #[test]
    fn test_validator_empty_secret() {
        let mut config = create_test_config();
        config.jwt_secret = String::new();
        let validator = JwtValidator::new(&config);
        assert!(validator.is_err());
    }

    #[test]
    fn test_validate_valid_token() {
        let config = create_test_config();
        let validator = JwtValidator::new(&config).unwrap();

        let claims = create_valid_claims();
        let token = create_test_token(&claims, &config.jwt_secret);

        let result = validator.validate(&token);
        assert!(result.is_ok());

        let validated_claims = result.unwrap();
        assert_eq!(validated_claims.user_id, 123);
        assert_eq!(validated_claims.username, "testuser");
    }

    #[test]
    fn test_validate_expired_token() {
        let config = create_test_config();
        let validator = JwtValidator::new(&config).unwrap();

        let mut claims = create_valid_claims();
        claims.exp = chrono::Utc::now().timestamp() - 3600;

        let token = create_test_token(&claims, &config.jwt_secret);

        let result = validator.validate(&token);
        assert!(matches!(result, Err(WebserverError::TokenExpired)));
    }

    #[test]
    fn test_validate_invalid_signature() {
        let config = create_test_config();
        let validator = JwtValidator::new(&config).unwrap();

        let claims = create_valid_claims();
        let token = create_test_token(&claims, "wrong-secret");

        let result = validator.validate(&token);
        assert!(matches!(result, Err(WebserverError::InvalidToken(_))));
    }

    #[test]
    fn test_validate_invalid_issuer() {
        let config = create_test_config();
        let validator = JwtValidator::new(&config).unwrap();

        let mut claims = create_valid_claims();
        claims.iss = "wrong-issuer".to_string();

        let token = create_test_token(&claims, &config.jwt_secret);

        let result = validator.validate(&token);
        assert!(matches!(result, Err(WebserverError::InvalidToken(_))));
    }
}
