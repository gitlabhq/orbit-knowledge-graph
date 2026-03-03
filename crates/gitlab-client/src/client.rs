use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use reqwest::StatusCode;
use serde::Serialize;
use tracing::debug;

use crate::config::GitlabClientConfiguration;
use crate::error::GitlabClientError;
use crate::types::RepositoryInfo;

/// JWT issuer — Rails expects this value when validating incoming tokens.
pub const JWT_ISSUER: &str = "gitlab";

/// JWT audience — Rails expects this value when validating incoming tokens.
pub const JWT_AUDIENCE: &str = "gitlab-knowledge-graph";

/// JWT subject — identifies this service. Rails validates that the subject
/// starts with an expected prefix (e.g. "gkg-").
pub const JWT_SUBJECT: &str = "gkg-indexer:code";

/// Custom authentication header used by the Knowledge Graph internal API.
/// The raw JWT token is sent directly as the header value (no `Bearer` prefix).
const AUTH_HEADER: &str = "Gitlab-Kg-Api-Request";

const JWT_EXPIRY_SECONDS: i64 = 300;

#[derive(Serialize)]
struct JwtClaims {
    iss: &'static str,
    sub: &'static str,
    aud: &'static str,
    iat: i64,
    exp: i64,
}

pub struct GitlabClient {
    http: reqwest::Client,
    base_url: String,
    signing_key: Vec<u8>,
}

impl GitlabClient {
    pub fn new(config: GitlabClientConfiguration) -> Result<Self, GitlabClientError> {
        let signing_key = BASE64.decode(&config.signing_key)?;
        Ok(Self {
            http: reqwest::Client::new(),
            base_url: config.base_url,
            signing_key,
        })
    }

    pub async fn repository_info(
        &self,
        project_id: i64,
    ) -> Result<RepositoryInfo, GitlabClientError> {
        let token = self.sign_jwt()?;
        let url = format!(
            "{}/api/v4/internal/knowledge_graph/{}/repository_info",
            self.base_url, project_id
        );

        debug!(project_id, url = %url, "fetching repository info from GitLab");

        let response = self
            .http
            .get(&url)
            .header(AUTH_HEADER, &token)
            .send()
            .await?;

        match response.status() {
            StatusCode::OK => {}
            StatusCode::UNAUTHORIZED => return Err(GitlabClientError::Unauthorized),
            StatusCode::NOT_FOUND => return Err(GitlabClientError::NotFound(project_id)),
            status => {
                let body = response.text().await.unwrap_or_default();
                return Err(GitlabClientError::Unexpected(format!(
                    "status {status}: {body}"
                )));
            }
        }

        let info: RepositoryInfo = response.json().await?;
        Ok(info)
    }

    fn sign_jwt(&self) -> Result<String, GitlabClientError> {
        let now = chrono::Utc::now().timestamp();
        let claims = JwtClaims {
            iss: JWT_ISSUER,
            sub: JWT_SUBJECT,
            aud: JWT_AUDIENCE,
            iat: now,
            exp: now + JWT_EXPIRY_SECONDS,
        };

        let key = EncodingKey::from_secret(&self.signing_key);
        encode(&Header::new(Algorithm::HS256), &claims, &key)
            .map_err(|e| GitlabClientError::JwtSigning(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_jwt_base64_decodes_secret_and_produces_valid_token() {
        let raw_secret = b"test-secret-that-is-long-enough!";
        let encoded_secret = BASE64.encode(raw_secret);

        let now = chrono::Utc::now().timestamp();
        let claims = JwtClaims {
            iss: JWT_ISSUER,
            sub: JWT_SUBJECT,
            aud: JWT_AUDIENCE,
            iat: now,
            exp: now + JWT_EXPIRY_SECONDS,
        };

        let decoded_key = BASE64.decode(&encoded_secret).unwrap();
        let key = EncodingKey::from_secret(&decoded_key);
        let token = encode(&Header::new(Algorithm::HS256), &claims, &key).unwrap();
        assert!(!token.is_empty());

        let decoding_key = jsonwebtoken::DecodingKey::from_secret(raw_secret);
        let mut validation = jsonwebtoken::Validation::new(Algorithm::HS256);
        validation.set_issuer(&[JWT_ISSUER]);
        validation.set_audience(&[JWT_AUDIENCE]);
        let decoded =
            jsonwebtoken::decode::<serde_json::Value>(&token, &decoding_key, &validation).unwrap();
        assert_eq!(decoded.claims["iss"], "gitlab");
        assert_eq!(decoded.claims["aud"], "gitlab-knowledge-graph");
    }
}
