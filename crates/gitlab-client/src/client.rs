use std::net::SocketAddr;
use std::pin::Pin;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures::Stream;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use reqwest::StatusCode;
use serde::Serialize;
use tracing::debug;

use crate::config::GitlabClientConfiguration;
use crate::error::GitlabClientError;
use crate::types::{ChangedPath, ProjectInfo};

pub type ByteStream = Pin<Box<dyn Stream<Item = Result<bytes::Bytes, GitlabClientError>> + Send>>;

/// JWT issuer — Rails expects this value when validating incoming tokens.
pub const JWT_ISSUER: &str = "gitlab";

/// JWT audience — Rails expects this value when validating incoming tokens.
pub const JWT_AUDIENCE: &str = "gitlab-knowledge-graph";

/// JWT subject — identifies this service. Rails validates that the subject
/// starts with an expected prefix (e.g. "gkg-").
pub const JWT_SUBJECT: &str = "gkg-indexer:code";

/// Custom authentication header used by the Knowledge Graph internal API.
/// The raw JWT token is sent directly as the header value (no `Bearer` prefix).
const AUTH_HEADER: &str = "Gitlab-Orbit-Api-Request";

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
        let http = Self::build_http_client(&config)?;
        Ok(Self {
            http,
            base_url: config.base_url,
            signing_key,
        })
    }

    fn build_http_client(
        config: &GitlabClientConfiguration,
    ) -> Result<reqwest::Client, GitlabClientError> {
        // reqwest is compiled with `rustls-no-provider`, so a CryptoProvider
        // must be installed before building any client. The `install_default`
        // call is idempotent — the Err case just means another caller already
        // installed a provider.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let mut builder = reqwest::Client::builder();

        if let Some(resolve_host) = &config.resolve_host {
            let parsed = reqwest::Url::parse(&config.base_url)
                .map_err(|e| GitlabClientError::Unexpected(format!("invalid base_url: {e}")))?;
            let domain = parsed
                .host_str()
                .ok_or_else(|| GitlabClientError::Unexpected("base_url has no host".into()))?;
            let port = parsed.port_or_known_default().ok_or_else(|| {
                GitlabClientError::Unexpected("base_url has no known default port".into())
            })?;

            let addr = std::net::ToSocketAddrs::to_socket_addrs(&(resolve_host.as_str(), port))
                .map_err(|e| {
                    GitlabClientError::Unexpected(format!("failed to resolve {resolve_host}: {e}"))
                })?
                .next()
                .ok_or_else(|| {
                    GitlabClientError::Unexpected(format!("no addresses found for {resolve_host}"))
                })?;

            debug!(
                domain,
                resolve_host,
                addr = %addr,
                "overriding DNS for base_url host"
            );
            // Port 0 tells reqwest to use the port from the request URL.
            builder = builder.resolve(domain, SocketAddr::new(addr.ip(), 0));
        }

        builder
            .build()
            .map_err(|e| GitlabClientError::Unexpected(format!("failed to build HTTP client: {e}")))
    }

    pub async fn project_info(&self, project_id: i64) -> Result<ProjectInfo, GitlabClientError> {
        let url = format!(
            "{}/api/v4/internal/orbit/project/{}/info",
            self.base_url, project_id
        );

        debug!(project_id, url = %url, "fetching project info from GitLab");

        let response = self.authenticated_get(&url).await?;
        Self::check_status(&response, project_id)?;

        let info: ProjectInfo = response.json().await?;
        Ok(info)
    }

    pub async fn download_archive(
        &self,
        project_id: i64,
        ref_name: &str,
    ) -> Result<Vec<u8>, GitlabClientError> {
        let base = format!(
            "{}/api/v4/internal/orbit/project/{}/repository/archive",
            self.base_url, project_id
        );
        let url = reqwest::Url::parse_with_params(&base, &[("ref", ref_name)])
            .map_err(|e| GitlabClientError::Unexpected(format!("invalid URL: {e}")))?;

        debug!(project_id, ref_name, url = %url, "downloading archive from GitLab");

        let response = self.authenticated_get(url).await?;
        Self::check_status(&response, project_id)?;

        let bytes = response.bytes().await?;
        Ok(bytes.to_vec())
    }

    pub async fn changed_paths(
        &self,
        project_id: i64,
        from_sha: &str,
        to_sha: &str,
    ) -> Result<Vec<ChangedPath>, GitlabClientError> {
        let base = format!(
            "{}/api/v4/internal/orbit/project/{}/repository/changed_paths",
            self.base_url, project_id
        );
        let url =
            reqwest::Url::parse_with_params(&base, &[("from_sha", from_sha), ("to_sha", to_sha)])
                .map_err(|e| GitlabClientError::Unexpected(format!("invalid URL: {e}")))?;

        debug!(
            project_id,
            from_sha, to_sha, "fetching changed paths from GitLab"
        );

        let response = self.authenticated_get(url).await?;
        Self::check_diff_status(&response, project_id)?;

        let body = response.text().await?;
        parse_ndjson(&body)
    }

    pub async fn download_blobs(
        &self,
        project_id: i64,
        from_sha: &str,
        to_sha: &str,
    ) -> Result<ByteStream, GitlabClientError> {
        let base = format!(
            "{}/api/v4/internal/orbit/project/{}/repository/blobs",
            self.base_url, project_id
        );
        let url =
            reqwest::Url::parse_with_params(&base, &[("from_sha", from_sha), ("to_sha", to_sha)])
                .map_err(|e| GitlabClientError::Unexpected(format!("invalid URL: {e}")))?;

        debug!(
            project_id,
            from_sha, to_sha, "downloading blobs from GitLab"
        );

        let response = self.authenticated_get(url).await?;
        Self::check_diff_status(&response, project_id)?;

        let stream = futures::stream::unfold(response, |mut resp| async {
            match resp.chunk().await {
                Ok(Some(bytes)) => Some((Ok(bytes), resp)),
                Ok(None) => None,
                Err(e) => Some((Err(e.into()), resp)),
            }
        });

        Ok(Box::pin(stream))
    }

    async fn authenticated_get(
        &self,
        url: impl reqwest::IntoUrl,
    ) -> Result<reqwest::Response, GitlabClientError> {
        let token = self.sign_jwt()?;
        Ok(self
            .http
            .get(url)
            .header(AUTH_HEADER, &token)
            .send()
            .await?)
    }

    fn check_status(
        response: &reqwest::Response,
        project_id: i64,
    ) -> Result<(), GitlabClientError> {
        match response.status() {
            StatusCode::OK => Ok(()),
            StatusCode::UNAUTHORIZED => Err(GitlabClientError::Unauthorized),
            StatusCode::NOT_FOUND => Err(GitlabClientError::NotFound(project_id)),
            status => Err(GitlabClientError::Unexpected(format!(
                "unexpected status {status}"
            ))),
        }
    }

    fn check_diff_status(
        response: &reqwest::Response,
        project_id: i64,
    ) -> Result<(), GitlabClientError> {
        match response.status() {
            StatusCode::OK => Ok(()),
            StatusCode::BAD_REQUEST => Err(GitlabClientError::ForcePush(project_id)),
            StatusCode::UNAUTHORIZED => Err(GitlabClientError::Unauthorized),
            StatusCode::NOT_FOUND => Err(GitlabClientError::NotFound(project_id)),
            status => Err(GitlabClientError::Unexpected(format!(
                "unexpected status {status}"
            ))),
        }
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

const MAX_CHANGED_PATHS: usize = 100_000;

fn parse_ndjson(body: &str) -> Result<Vec<ChangedPath>, GitlabClientError> {
    let mut paths = Vec::new();
    for line in body.lines() {
        if line.is_empty() {
            continue;
        }
        if paths.len() >= MAX_CHANGED_PATHS {
            return Err(GitlabClientError::Unexpected(format!(
                "changed paths exceeded limit of {MAX_CHANGED_PATHS}"
            )));
        }
        let path: ChangedPath = serde_json::from_str(line).map_err(|e| {
            GitlabClientError::Unexpected(format!("failed to parse changed path: {e}"))
        })?;
        paths.push(path);
    }
    Ok(paths)
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

    fn config_with_resolve(
        base_url: &str,
        resolve_host: Option<&str>,
    ) -> GitlabClientConfiguration {
        GitlabClientConfiguration {
            base_url: base_url.to_string(),
            signing_key: BASE64.encode(b"test-secret-that-is-long-enough!"),
            resolve_host: resolve_host.map(String::from),
        }
    }

    #[test]
    fn build_http_client_without_resolve_host() {
        let config = config_with_resolve("https://gitlab.example.com", None);
        assert!(GitlabClient::build_http_client(&config).is_ok());
    }

    #[test]
    fn build_http_client_with_resolve_host_localhost() {
        let config = config_with_resolve("https://gitlab.example.com:11443", Some("localhost"));
        assert!(GitlabClient::build_http_client(&config).is_ok());
    }

    #[test]
    fn build_http_client_with_resolve_host_and_path() {
        let config = config_with_resolve("https://gitlab.example.com/backend", Some("localhost"));
        assert!(GitlabClient::build_http_client(&config).is_ok());
    }

    #[test]
    fn build_http_client_rejects_invalid_base_url() {
        let config = config_with_resolve("not a url", Some("localhost"));
        let err = GitlabClient::build_http_client(&config).unwrap_err();
        assert!(err.to_string().contains("invalid base_url"));
    }

    #[test]
    fn build_http_client_rejects_unknown_scheme() {
        let config = config_with_resolve("custom://gitlab.example.com", Some("localhost"));
        let err = GitlabClient::build_http_client(&config).unwrap_err();
        assert!(err.to_string().contains("no known default port"));
    }

    #[test]
    fn build_http_client_rejects_unresolvable_host() {
        let config = config_with_resolve(
            "https://gitlab.example.com",
            Some("this-host-definitely-does-not-exist.invalid"),
        );
        let err = GitlabClient::build_http_client(&config).unwrap_err();
        assert!(err.to_string().contains("failed to resolve"));
    }

    mod ndjson_parsing {
        use super::*;
        use crate::types::ChangeStatus;

        #[test]
        fn parses_single_line() {
            let body = r#"{"path":"src/main.rs","status":"ADDED","old_path":"","new_mode":33188,"old_blob_id":"","new_blob_id":"abc123"}"#;
            let result = parse_ndjson(body).unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].path, "src/main.rs");
            assert_eq!(result[0].status, ChangeStatus::Added);
            assert_eq!(result[0].new_blob_id, "abc123");
        }

        #[test]
        fn parses_multiple_lines() {
            let body = concat!(
                r#"{"path":"a.rs","status":"ADDED","old_path":"","new_mode":33188,"old_blob_id":"","new_blob_id":"aaa"}"#,
                "\n",
                r#"{"path":"b.rs","status":"MODIFIED","old_path":"","new_mode":33188,"old_blob_id":"bbb","new_blob_id":"ccc"}"#,
            );
            let result = parse_ndjson(body).unwrap();
            assert_eq!(result.len(), 2);
            assert_eq!(result[0].status, ChangeStatus::Added);
            assert_eq!(result[1].status, ChangeStatus::Modified);
        }

        #[test]
        fn skips_empty_lines() {
            let body = "\n\n";
            let result = parse_ndjson(body).unwrap();
            assert!(result.is_empty());
        }

        #[test]
        fn parses_renamed_with_old_path() {
            let body = r#"{"path":"new.rs","status":"RENAMED","old_path":"old.rs","new_mode":33188,"old_blob_id":"aaa","new_blob_id":"aaa"}"#;
            let result = parse_ndjson(body).unwrap();
            assert_eq!(result[0].status, ChangeStatus::Renamed);
            assert_eq!(result[0].old_path, "old.rs");
        }

        #[test]
        fn unknown_status_deserializes() {
            let body = r#"{"path":"a.rs","status":"SOMETHING_NEW","old_path":"","new_mode":33188,"old_blob_id":"","new_blob_id":"aaa"}"#;
            let result = parse_ndjson(body).unwrap();
            assert_eq!(result[0].status, ChangeStatus::Unknown);
        }

        #[test]
        fn parses_all_known_statuses() {
            for (json_status, expected) in [
                ("DELETED", ChangeStatus::Deleted),
                ("RENAMED", ChangeStatus::Renamed),
                ("ADDED", ChangeStatus::Added),
                ("MODIFIED", ChangeStatus::Modified),
                ("COPIED", ChangeStatus::Copied),
                ("TYPE_CHANGE", ChangeStatus::TypeChange),
            ] {
                let body = format!(
                    r#"{{"path":"a","status":"{json_status}","old_path":"","new_mode":33188,"old_blob_id":"","new_blob_id":"x"}}"#
                );
                let result = parse_ndjson(&body).unwrap();
                assert_eq!(result[0].status, expected, "failed for {json_status}");
            }
        }
    }
}
