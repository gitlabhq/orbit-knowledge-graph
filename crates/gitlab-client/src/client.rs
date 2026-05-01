use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures::{Stream, StreamExt};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use reqwest::StatusCode;
use serde::Serialize;
use tracing::debug;

use crate::error::GitlabClientError;
use crate::types::{MergeRequestDiffBatch, ProjectInfo};
use gkg_server_config::GitlabClientConfiguration;

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

fn into_byte_stream(response: reqwest::Response) -> ByteStream {
    let stream = futures::stream::unfold(Some(response), |state| async {
        let mut resp = state?;
        match resp.chunk().await {
            Ok(Some(bytes)) => Some((Ok(bytes), Some(resp))),
            Ok(None) => None,
            Err(e) => Some((Err(e.into()), None)),
        }
    })
    .fuse();
    Box::pin(stream)
}

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
            .connect_timeout(Duration::from_secs(30))
            .read_timeout(Duration::from_secs(120))
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
        Self::check_response_status(&response, project_id)?;

        let info: ProjectInfo = response.json().await?;
        Ok(info)
    }

    pub async fn download_archive(
        &self,
        project_id: i64,
        ref_name: &str,
    ) -> Result<ByteStream, GitlabClientError> {
        let base = format!(
            "{}/api/v4/internal/orbit/project/{}/repository/archive",
            self.base_url, project_id
        );
        let url = reqwest::Url::parse_with_params(&base, &[("ref", ref_name)])
            .map_err(|e| GitlabClientError::Unexpected(format!("invalid URL: {e}")))?;

        debug!(project_id, ref_name, url = %url, "downloading archive from GitLab");

        let response = self.authenticated_get(url).await?;
        Self::check_response_status(&response, project_id)?;

        Ok(into_byte_stream(response))
    }

    pub async fn changed_paths(
        &self,
        project_id: i64,
        from_sha: &str,
        to_sha: &str,
    ) -> Result<ByteStream, GitlabClientError> {
        let base = format!(
            "{}/api/v4/internal/orbit/project/{}/repository/changed_paths",
            self.base_url, project_id
        );
        let url = reqwest::Url::parse_with_params(
            &base,
            &[
                ("left_tree_revision", from_sha),
                ("right_tree_revision", to_sha),
            ],
        )
        .map_err(|e| GitlabClientError::Unexpected(format!("invalid URL: {e}")))?;

        debug!(
            project_id,
            from_sha, to_sha, "fetching changed paths from GitLab"
        );

        self.streaming_get(url, project_id).await
    }

    pub async fn list_blobs(
        &self,
        project_id: i64,
        oids: &[String],
    ) -> Result<ByteStream, GitlabClientError> {
        let url = format!(
            "{}/api/v4/internal/orbit/project/{}/repository/list_blobs",
            self.base_url, project_id
        );

        debug!(
            project_id,
            blob_count = oids.len(),
            "listing blobs from GitLab"
        );

        #[derive(Serialize)]
        struct ListBlobsRequest<'a> {
            revisions: &'a [String],
        }

        let body = ListBlobsRequest { revisions: oids };
        self.streaming_post(&url, project_id, &body).await
    }

    pub async fn list_merge_request_diff_files(
        &self,
        project_id: i64,
        diff_id: i64,
        paths: &[String],
    ) -> Result<MergeRequestDiffBatch, GitlabClientError> {
        let base = format!(
            "{}/api/v4/internal/orbit/project/{}/merge_request_diffs/{}",
            self.base_url, project_id, diff_id,
        );
        let mut url = reqwest::Url::parse(&base)
            .map_err(|e| GitlabClientError::Unexpected(format!("invalid URL: {e}")))?;

        if !paths.is_empty() {
            let mut query = url.query_pairs_mut();
            for path in paths {
                query.append_pair("paths[]", path);
            }
        }

        debug!(
            project_id,
            diff_id,
            path_count = paths.len(),
            "listing MR diff files"
        );

        let response = self.authenticated_get(url).await?;
        Self::check_response_status(&response, project_id)?;
        Ok(response.json().await?)
    }

    pub async fn get_merge_request_raw_diff(
        &self,
        project_id: i64,
        diff_id: i64,
    ) -> Result<ByteStream, GitlabClientError> {
        let url = format!(
            "{}/api/v4/internal/orbit/project/{}/merge_request_diffs/{}/raw_diffs",
            self.base_url, project_id, diff_id,
        );

        debug!(project_id, diff_id, "fetching MR raw diff");

        let response = self.authenticated_get(&url).await?;
        Self::check_response_status(&response, project_id)?;
        Ok(into_byte_stream(response))
    }

    pub async fn get_merge_request_raw_diff_by_iid(
        &self,
        project_id: i64,
        merge_request_iid: i64,
    ) -> Result<ByteStream, GitlabClientError> {
        let url = format!(
            "{}/api/v4/internal/orbit/project/{}/merge_requests/{}/raw_diffs",
            self.base_url, project_id, merge_request_iid,
        );

        debug!(project_id, merge_request_iid, "fetching MR raw diff by IID");

        let response = self.authenticated_get(&url).await?;
        Self::check_response_status(&response, project_id)?;
        Ok(into_byte_stream(response))
    }

    async fn streaming_get(
        &self,
        url: reqwest::Url,
        project_id: i64,
    ) -> Result<ByteStream, GitlabClientError> {
        let response = self.authenticated_get(url).await?;
        Self::check_diff_status(&response, project_id)?;
        Ok(into_byte_stream(response))
    }

    async fn streaming_post(
        &self,
        url: &str,
        project_id: i64,
        body: &impl serde::Serialize,
    ) -> Result<ByteStream, GitlabClientError> {
        let response = self.authenticated_post(url, body).await?;
        Self::check_response_status(&response, project_id)?;
        Ok(into_byte_stream(response))
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

    async fn authenticated_post(
        &self,
        url: &str,
        body: &impl serde::Serialize,
    ) -> Result<reqwest::Response, GitlabClientError> {
        let token = self.sign_jwt()?;
        Ok(self
            .http
            .post(url)
            .header(AUTH_HEADER, &token)
            .json(body)
            .send()
            .await?)
    }

    fn check_response_status(
        response: &reqwest::Response,
        project_id: i64,
    ) -> Result<(), GitlabClientError> {
        let status = response.status();
        match status {
            StatusCode::OK => Ok(()),
            StatusCode::UNAUTHORIZED => Err(GitlabClientError::Unauthorized),
            StatusCode::NOT_FOUND => Err(GitlabClientError::NotFound(project_id)),
            _ if status.is_server_error() => Err(GitlabClientError::ServerError {
                project_id,
                status: status.as_u16(),
            }),
            _ => Err(GitlabClientError::Unexpected(format!(
                "unexpected status {status}"
            ))),
        }
    }

    fn check_diff_status(
        response: &reqwest::Response,
        project_id: i64,
    ) -> Result<(), GitlabClientError> {
        if response.status() == StatusCode::BAD_REQUEST {
            return Err(GitlabClientError::ForcePush(project_id));
        }
        let status = response.status();
        match status {
            StatusCode::OK => Ok(()),
            StatusCode::UNAUTHORIZED => Err(GitlabClientError::Unauthorized),
            StatusCode::NOT_FOUND => Err(GitlabClientError::NotFound(project_id)),
            _ if status.is_server_error() => Err(GitlabClientError::ServerError {
                project_id,
                status: status.as_u16(),
            }),
            _ => Err(GitlabClientError::Unexpected(format!(
                "unexpected status {status} for project {project_id}"
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
}
