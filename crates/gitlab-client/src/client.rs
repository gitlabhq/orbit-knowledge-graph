use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use circuit_breaker::{CircuitBreaker, CircuitConfig};
use futures::{Stream, StreamExt};
use jsonwebtoken::dangerous::insecure_decode;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use opentelemetry::KeyValue;
use opentelemetry::metrics::Counter;
use orbit_log_fields::{debug, info, warn};
use orbit_observability::gitlab_client::{INSECURE_CREDENTIAL_REQUESTS, labels};
use orbit_observability::resilience::MetricsObserver;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use url::Host;

use crate::error::GitlabClientError;
use crate::types::{CloudConnectorToken, GitlabInstance, MergeRequestDiffBatch, ProjectInfo};
use orbit_server_config::GitlabClientConfiguration;

pub type ByteStream = Pin<Box<dyn Stream<Item = Result<bytes::Bytes, GitlabClientError>> + Send>>;

/// Rails expects this value when validating incoming tokens.
pub const JWT_ISSUER: &str = "gitlab";

/// Rails expects this value when validating incoming tokens.
pub const JWT_AUDIENCE: &str = "gitlab-knowledge-graph";

/// Rails validates that the subject starts with an expected prefix (e.g. "gkg-").
pub const JWT_SUBJECT: &str = "gkg-indexer:code";

/// The raw JWT token is sent directly as this header value (no `Bearer` prefix).
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

#[derive(Deserialize)]
struct CloudConnectorTokenResponse {
    token: String,
}

#[derive(Deserialize)]
struct CloudConnectorTokenClaims {
    exp: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
enum Channel {
    Tls,
    Localhost,
    PrivateAddress,
    PublicAddress,
    Unresolved,
    InvalidUrl,
}

impl Channel {
    fn classify(base_url: &str, dialled: Option<IpAddr>) -> Self {
        let Ok(url) = reqwest::Url::parse(base_url) else {
            return Self::InvalidUrl;
        };
        if url.scheme() == "https" {
            return Self::Tls;
        }
        let addresses: Vec<IpAddr> = match url.host() {
            None => return Self::InvalidUrl,
            Some(Host::Domain("localhost")) if dialled.is_none() => return Self::Localhost,
            // reqwest ignores the resolve_host override for an IP literal host.
            Some(Host::Ipv4(ip)) => vec![ip.into()],
            Some(Host::Ipv6(ip)) => vec![ip.into()],
            Some(Host::Domain(domain)) => match dialled {
                Some(ip) => vec![ip],
                None => (domain, 0)
                    .to_socket_addrs()
                    .map(|addrs| addrs.map(|addr| addr.ip()).collect())
                    .unwrap_or_default(),
            },
        };
        if addresses.is_empty() {
            Self::Unresolved
        } else if addresses.into_iter().all(is_loopback_or_private) {
            Self::PrivateAddress
        } else {
            Self::PublicAddress
        }
    }

    fn is_tls_or_private(self) -> bool {
        matches!(self, Self::Tls | Self::Localhost | Self::PrivateAddress)
    }
}

fn is_loopback_or_private(ip: IpAddr) -> bool {
    match ip.to_canonical() {
        IpAddr::V4(ip) => ip.is_loopback() || ip.is_private(),
        IpAddr::V6(ip) => ip.is_loopback() || ip.is_unique_local(),
    }
}

struct DnsOverride {
    domain: String,
    ip: IpAddr,
}

pub struct GitlabClient {
    http: reqwest::Client,
    base_url: String,
    signing_key: Vec<u8>,
    circuit_breaker: CircuitBreaker,
    channel: Channel,
    insecure_credential_requests: Counter<u64>,
}

impl GitlabClient {
    pub fn new(config: GitlabClientConfiguration) -> Result<Self, GitlabClientError> {
        let signing_key = BASE64.decode(&config.signing_key)?;
        let dns_override = Self::dns_override(&config)?;
        let http = Self::build_http_client(dns_override.as_ref())?;
        let channel = Channel::classify(&config.base_url, dns_override.map(|o| o.ip));
        let reason: &'static str = channel.into();
        if channel.is_tls_or_private() {
            info!(
                safe(REASON) = reason,
                expanded(URL) = config.base_url.as_str(),
                "GitLab channel is TLS or private"
            );
        } else {
            warn!(
                safe(REASON) = reason,
                expanded(URL) = config.base_url.as_str(),
                "GitLab channel is neither TLS nor private; credentials are sent over it unprotected"
            );
        }
        Ok(Self {
            http,
            base_url: config.base_url,
            signing_key,
            circuit_breaker: CircuitBreaker::new(
                "gitlab",
                CircuitConfig::default(),
                Arc::new(MetricsObserver::new()),
            ),
            channel,
            insecure_credential_requests: INSECURE_CREDENTIAL_REQUESTS
                .build_counter_u64(&orbit_observability::meter()),
        })
    }

    #[cfg(test)]
    fn with_circuit_breaker(mut self, circuit_breaker: CircuitBreaker) -> Self {
        self.circuit_breaker = circuit_breaker;
        self
    }

    #[cfg(test)]
    fn with_channel(mut self, channel: Channel) -> Self {
        self.channel = channel;
        self
    }

    #[cfg(test)]
    fn with_meter(mut self, meter: &opentelemetry::metrics::Meter) -> Self {
        self.insecure_credential_requests = INSECURE_CREDENTIAL_REQUESTS.build_counter_u64(meter);
        self
    }

    fn dns_override(
        config: &GitlabClientConfiguration,
    ) -> Result<Option<DnsOverride>, GitlabClientError> {
        let Some(resolve_host) = &config.resolve_host else {
            return Ok(None);
        };
        let parsed = reqwest::Url::parse(&config.base_url)
            .map_err(|e| GitlabClientError::Unexpected(format!("invalid base_url: {e}")))?;
        let domain = parsed
            .host_str()
            .ok_or_else(|| GitlabClientError::Unexpected("base_url has no host".into()))?;
        let port = parsed.port_or_known_default().ok_or_else(|| {
            GitlabClientError::Unexpected("base_url has no known default port".into())
        })?;

        let addr = (resolve_host.as_str(), port)
            .to_socket_addrs()
            .map_err(|e| {
                GitlabClientError::Unexpected(format!("failed to resolve {resolve_host}: {e}"))
            })?
            .next()
            .ok_or_else(|| {
                GitlabClientError::Unexpected(format!("no addresses found for {resolve_host}"))
            })?;

        debug!(
            expanded(HOST) = domain,
            expanded(RESOLVE_HOST) = resolve_host.as_str(),
            expanded(REMOTE_ADDR) = %addr,
            "overriding DNS for base_url host"
        );
        Ok(Some(DnsOverride {
            domain: domain.to_string(),
            ip: addr.ip(),
        }))
    }

    fn build_http_client(
        dns_override: Option<&DnsOverride>,
    ) -> Result<reqwest::Client, GitlabClientError> {
        // reqwest is compiled with `rustls-no-provider`, so a CryptoProvider
        // must be installed before building any client. The `install_default`
        // call is idempotent — the Err case just means another caller already
        // installed a provider.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        // A redirect could carry the credential header to a host that was never classified.
        let mut builder = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none());

        if let Some(dns_override) = dns_override {
            // Port 0 tells reqwest to use the port from the request URL.
            builder = builder.resolve(&dns_override.domain, SocketAddr::new(dns_override.ip, 0));
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

        debug!(
            safe(GL_PROJECT_ID) = project_id,
            expanded(URL) = %url,
            "fetching project info from GitLab"
        );

        let response = self.authenticated_get(&url).await?;
        Self::check_response_status(&response, project_id)?;

        let info: ProjectInfo = response.json().await?;
        Ok(info)
    }

    pub async fn cloud_connector_token(&self) -> Result<CloudConnectorToken, GitlabClientError> {
        let url = format!(
            "{}/api/v4/internal/orbit/cloud_connector_token",
            self.base_url
        );

        info!(expanded(URL) = %url, "fetching cloud connector token from GitLab");

        let response = self.authenticated_get(&url).await?;
        Self::check_token_response_status(&response)?;

        let raw: CloudConnectorTokenResponse = response.json().await?;
        let exp = decode_token_exp(&raw.token)?;
        Ok(CloudConnectorToken {
            token: raw.token,
            exp,
        })
    }

    pub async fn instance(&self) -> Result<Option<GitlabInstance>, GitlabClientError> {
        let url = format!("{}/api/v4/internal/orbit/instance", self.base_url);

        let response = self.authenticated_get(&url).await?;
        match response.status() {
            StatusCode::OK => Ok(Some(response.json().await?)),
            StatusCode::NOT_FOUND => Ok(None),
            StatusCode::UNAUTHORIZED => Err(GitlabClientError::Unauthorized),
            status => Err(GitlabClientError::Unexpected(format!(
                "instance request returned status {status}"
            ))),
        }
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
        let url = reqwest::Url::parse_with_params(
            &base,
            &[("ref", ref_name), ("include_lfs_blobs", "false")],
        )
        .map_err(|e| GitlabClientError::Unexpected(format!("invalid URL: {e}")))?;

        debug!(
            safe(GL_PROJECT_ID) = project_id,
            expanded(BRANCH) = ref_name,
            expanded(URL) = %url,
            "downloading archive from GitLab"
        );

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
            safe(GL_PROJECT_ID) = project_id,
            safe(FROM_COMMIT_SHA) = from_sha,
            safe(TO_COMMIT_SHA) = to_sha,
            "fetching changed paths from GitLab"
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
            safe(GL_PROJECT_ID) = project_id,
            safe(COUNT) = oids.len(),
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
            safe(GL_PROJECT_ID) = project_id,
            safe(DIFF_ID) = diff_id,
            safe(FILE_COUNT) = paths.len(),
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

        debug!(
            safe(GL_PROJECT_ID) = project_id,
            safe(DIFF_ID) = diff_id,
            "fetching MR raw diff"
        );

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

        debug!(
            safe(GL_PROJECT_ID) = project_id,
            safe(IID) = merge_request_iid,
            "fetching MR raw diff by IID"
        );

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
        self.send("GET", self.http.get(url).header(AUTH_HEADER, &token))
            .await
    }

    async fn authenticated_post(
        &self,
        url: &str,
        body: &impl serde::Serialize,
    ) -> Result<reqwest::Response, GitlabClientError> {
        let token = self.sign_jwt()?;
        self.send(
            "POST",
            self.http.post(url).header(AUTH_HEADER, &token).json(body),
        )
        .await
    }

    async fn send(
        &self,
        method: &'static str,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, GitlabClientError> {
        self.circuit_breaker
            .call_transient(|| async {
                if !self.channel.is_tls_or_private() {
                    self.insecure_credential_requests
                        .add(1, &[KeyValue::new(labels::METHOD, method)]);
                }
                Ok(request.send().await?)
            })
            .await
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
            StatusCode::TOO_MANY_REQUESTS => Err(GitlabClientError::RateLimited { project_id }),
            _ if status.is_server_error() => Err(GitlabClientError::ServerError {
                project_id,
                status: status.as_u16(),
            }),
            _ => Err(GitlabClientError::Unexpected(format!(
                "unexpected status {status}"
            ))),
        }
    }

    fn check_token_response_status(response: &reqwest::Response) -> Result<(), GitlabClientError> {
        let status = response.status();
        match status {
            StatusCode::OK => Ok(()),
            StatusCode::UNAUTHORIZED => Err(GitlabClientError::Unauthorized),
            _ => Err(GitlabClientError::Unexpected(format!(
                "cloud connector token request returned status {status}"
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

pub(crate) fn decode_token_exp(token: &str) -> Result<i64, GitlabClientError> {
    let data = insecure_decode::<CloudConnectorTokenClaims>(token)
        .map_err(|e| GitlabClientError::JwtDecoding(e.to_string()))?;
    Ok(data.claims.exp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{UNREACHABLE_URL, circuit_breaker};
    use crate::types::{InstanceIdentity, InstanceSettings};
    use futures::future::BoxFuture;

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

    #[test]
    fn decode_token_exp_ignores_signature() {
        let key = EncodingKey::from_secret(b"some-other-key-entirely");
        let now = chrono::Utc::now().timestamp();
        let token = encode(
            &Header::new(Algorithm::HS256),
            &serde_json::json!({ "exp": now + 100 }),
            &key,
        )
        .unwrap();

        assert!(decode_token_exp(&token).is_ok());
    }

    #[test]
    fn decode_token_exp_rejects_malformed_token() {
        let err = decode_token_exp("not-a-jwt").unwrap_err();
        assert!(matches!(err, GitlabClientError::JwtDecoding(_)));
    }

    #[test]
    fn decode_token_exp_rejects_missing_exp_claim() {
        let key = EncodingKey::from_secret(b"any-secret");
        let token = encode(&Header::new(Algorithm::HS256), &serde_json::json!({}), &key).unwrap();

        let err = decode_token_exp(&token).unwrap_err();
        assert!(matches!(err, GitlabClientError::JwtDecoding(_)));
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
        assert!(GitlabClient::new(config).is_ok());
    }

    #[test]
    fn build_http_client_with_resolve_host_localhost() {
        let config = config_with_resolve("https://gitlab.example.com:11443", Some("localhost"));
        assert!(GitlabClient::new(config).is_ok());
    }

    #[test]
    fn build_http_client_with_resolve_host_and_path() {
        let config = config_with_resolve("https://gitlab.example.com/backend", Some("localhost"));
        assert!(GitlabClient::new(config).is_ok());
    }

    #[test]
    fn build_http_client_rejects_invalid_base_url() {
        let config = config_with_resolve("not a url", Some("localhost"));
        let err = GitlabClient::new(config).err().unwrap();
        assert!(err.to_string().contains("invalid base_url"));
    }

    #[test]
    fn build_http_client_rejects_unknown_scheme() {
        let config = config_with_resolve("custom://gitlab.example.com", Some("localhost"));
        let err = GitlabClient::new(config).err().unwrap();
        assert!(err.to_string().contains("no known default port"));
    }

    #[test]
    fn build_http_client_rejects_unresolvable_host() {
        let config = config_with_resolve(
            "https://gitlab.example.com",
            Some("this-host-definitely-does-not-exist.invalid"),
        );
        let err = GitlabClient::new(config).err().unwrap();
        assert!(err.to_string().contains("failed to resolve"));
    }

    #[tokio::test]
    async fn download_archive_asks_for_the_ref_without_lfs_object_contents() {
        use axum::Router;
        use axum::extract::RawQuery;
        use axum::routing::get;
        use std::sync::{Arc, Mutex};

        let seen = Arc::new(Mutex::new(String::new()));
        let recorder = Arc::clone(&seen);
        let app = Router::new().route(
            "/api/v4/internal/orbit/project/{id}/repository/archive",
            get(move |RawQuery(query): RawQuery| {
                let recorder = Arc::clone(&recorder);
                async move {
                    *recorder.lock().unwrap() = query.unwrap_or_default();
                    Vec::<u8>::new()
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let client =
            GitlabClient::new(config_with_resolve(&format!("http://{addr}"), None)).unwrap();
        let _stream = client.download_archive(7, "abc123").await.unwrap();

        assert_eq!(*seen.lock().unwrap(), "ref=abc123&include_lfs_blobs=false");
    }

    #[tokio::test]
    async fn project_info_advertises_compression_and_decodes_gzip_response() {
        use axum::Router;
        use axum::http::{HeaderMap, HeaderValue, header};
        use axum::routing::get;
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Write;
        use std::sync::{Arc, Mutex};

        let seen = Arc::new(Mutex::new(None::<String>));
        let recorder = Arc::clone(&seen);
        let app = Router::new().route(
            "/api/v4/internal/orbit/project/{id}/info",
            get(move |headers: HeaderMap| {
                let recorder = Arc::clone(&recorder);
                async move {
                    *recorder.lock().unwrap() = headers
                        .get(header::ACCEPT_ENCODING)
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_owned);
                    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                    encoder
                        .write_all(br#"{"project_id":7,"default_branch":"main"}"#)
                        .unwrap();
                    (
                        [(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"))],
                        encoder.finish().unwrap(),
                    )
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let client =
            GitlabClient::new(config_with_resolve(&format!("http://{addr}"), None)).unwrap();
        let info = client.project_info(7).await.unwrap();

        assert_eq!(info.project_id, 7);
        assert_eq!(info.default_branch, "main");
        let accept_encoding = seen
            .lock()
            .unwrap()
            .clone()
            .expect("Accept-Encoding header");
        assert!(accept_encoding.contains("gzip"), "{accept_encoding}");
        assert!(accept_encoding.contains("br"), "{accept_encoding}");
        assert!(accept_encoding.contains("zstd"), "{accept_encoding}");
    }

    #[tokio::test]
    async fn cloud_connector_token_returns_the_served_token_with_its_exp_even_if_expired() {
        use axum::Router;
        use axum::routing::get;

        let now = chrono::Utc::now().timestamp();
        for exp in [now + 3600, now - 3600] {
            let key = EncodingKey::from_secret(b"any-secret");
            let mut header = Header::new(Algorithm::HS256);
            header.kid = Some("cloud-connector-2026".to_string());
            let cc_token = encode(
                &header,
                &serde_json::json!({
                    "iss": "gitlab-cloud-connector",
                    "aud": "gitlab-orbit",
                    "sub": "cloud_connector_token",
                    "exp": exp,
                }),
                &key,
            )
            .unwrap();
            let expected_token = cc_token.clone();

            let app = Router::new().route(
                "/api/v4/internal/orbit/cloud_connector_token",
                get(move || {
                    let cc_token = cc_token.clone();
                    async move { axum::Json(serde_json::json!({ "token": cc_token })) }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

            let client =
                GitlabClient::new(config_with_resolve(&format!("http://{addr}"), None)).unwrap();
            let result = client.cloud_connector_token().await.unwrap();

            assert_eq!(result.token, expected_token);
            assert_eq!(result.exp, exp);
        }
    }

    #[tokio::test]
    async fn instance_returns_the_served_identity_and_settings_and_ignores_unknown_fields() {
        use axum::Router;
        use axum::routing::get;

        let app = Router::new().route(
            "/api/v4/internal/orbit/instance",
            get(|| async {
                axum::Json(serde_json::json!({
                    "identity": {
                        "instance_id": "a1b2",
                        "unique_instance_id": "c3d4",
                        "host_name": "gitlab.example.com",
                        "instance_version": "18.6.0-ee",
                    },
                    "settings": { "product_usage_data_enabled": true },
                }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let client =
            GitlabClient::new(config_with_resolve(&format!("http://{addr}"), None)).unwrap();

        assert_eq!(
            client.instance().await.unwrap(),
            Some(GitlabInstance {
                identity: InstanceIdentity {
                    instance_id: Some("a1b2".to_string()),
                    unique_instance_id: Some("c3d4".to_string()),
                    host_name: Some("gitlab.example.com".to_string()),
                },
                settings: InstanceSettings {
                    product_usage_data_enabled: true,
                },
            })
        );
    }

    #[tokio::test]
    async fn instance_is_none_when_gitlab_has_no_endpoint() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, axum::Router::new()).await.unwrap() });

        let client =
            GitlabClient::new(config_with_resolve(&format!("http://{addr}"), None)).unwrap();

        assert_eq!(client.instance().await.unwrap(), None);
    }

    fn client_with_breaker(base_url: &str, breaker: &CircuitBreaker) -> GitlabClient {
        GitlabClient::new(config_with_resolve(base_url, None))
            .unwrap()
            .with_circuit_breaker(breaker.clone())
    }

    type RequestCall = for<'a> fn(&'a GitlabClient) -> BoxFuture<'a, Result<(), GitlabClientError>>;

    const REQUEST_CALLS: [(&str, RequestCall); 9] = [
        ("project_info", |client| {
            Box::pin(async { client.project_info(1).await.map(drop) })
        }),
        ("cloud_connector_token", |client| {
            Box::pin(async { client.cloud_connector_token().await.map(drop) })
        }),
        ("instance", |client| {
            Box::pin(async { client.instance().await.map(drop) })
        }),
        ("download_archive", |client| {
            Box::pin(async { client.download_archive(1, "main").await.map(drop) })
        }),
        ("changed_paths", |client| {
            Box::pin(async { client.changed_paths(1, "a", "b").await.map(drop) })
        }),
        ("list_blobs", |client| {
            Box::pin(async { client.list_blobs(1, &["oid".to_string()]).await.map(drop) })
        }),
        ("list_merge_request_diff_files", |client| {
            Box::pin(async {
                client
                    .list_merge_request_diff_files(1, 1, &[])
                    .await
                    .map(drop)
            })
        }),
        ("get_merge_request_raw_diff", |client| {
            Box::pin(async { client.get_merge_request_raw_diff(1, 1).await.map(drop) })
        }),
        ("get_merge_request_raw_diff_by_iid", |client| {
            Box::pin(async {
                client
                    .get_merge_request_raw_diff_by_iid(1, 1)
                    .await
                    .map(drop)
            })
        }),
    ];

    #[tokio::test]
    async fn every_request_counts_toward_the_circuit() {
        for (name, call) in REQUEST_CALLS {
            let breaker = circuit_breaker(1);
            let client = client_with_breaker(UNREACHABLE_URL, &breaker);

            let _ = call(&client).await;

            assert!(
                !breaker.is_available(),
                "{name} bypassed the circuit breaker"
            );
        }
    }

    #[tokio::test]
    async fn an_open_circuit_rejects_every_request() {
        let client = client_with_breaker(UNREACHABLE_URL, &circuit_breaker(1));
        let err = client.project_info(1).await.unwrap_err();
        assert!(matches!(err, GitlabClientError::Request(_)), "got {err:?}");

        for (name, call) in REQUEST_CALLS {
            let err = call(&client).await.unwrap_err();
            assert!(
                matches!(err, GitlabClientError::CircuitOpen { .. }),
                "{name} returned {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn error_responses_never_open_the_circuit() {
        use axum::Router;
        use axum::http::StatusCode as AxumStatus;
        use axum::routing::get;

        for status in [
            AxumStatus::INTERNAL_SERVER_ERROR,
            AxumStatus::TOO_MANY_REQUESTS,
            AxumStatus::NOT_FOUND,
            AxumStatus::UNAUTHORIZED,
        ] {
            let app = Router::new().route(
                "/api/v4/internal/orbit/project/{id}/info",
                get(move || async move { status }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let client = client_with_breaker(&format!("http://{addr}"), &circuit_breaker(2));

            for _ in 0..5 {
                let err = client.project_info(1).await.unwrap_err();
                assert!(
                    !matches!(err, GitlabClientError::CircuitOpen { .. }),
                    "{status} opened the circuit"
                );
            }
        }
    }

    #[tokio::test]
    async fn archive_overload_statuses_are_distinct_errors() {
        use axum::Router;
        use axum::http::StatusCode as AxumStatus;
        use axum::routing::get;

        let app = Router::new().route(
            "/api/v4/internal/orbit/project/{id}/repository/archive",
            get(
                |axum::extract::Path(id): axum::extract::Path<i64>| async move {
                    match id {
                        1 => AxumStatus::INTERNAL_SERVER_ERROR,
                        _ => AxumStatus::TOO_MANY_REQUESTS,
                    }
                },
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = client_with_breaker(&format!("http://{addr}"), &circuit_breaker(2));

        let err = |result: Result<ByteStream, GitlabClientError>| result.err().unwrap();
        assert!(matches!(
            err(client.download_archive(1, "main").await),
            GitlabClientError::ServerError { status: 500, .. }
        ));
        assert!(matches!(
            err(client.download_archive(2, "main").await),
            GitlabClientError::RateLimited { .. }
        ));
    }

    fn channel_of(base_url: &str, resolve_host: Option<&str>) -> Channel {
        GitlabClient::new(config_with_resolve(base_url, resolve_host))
            .ok()
            .unwrap()
            .channel
    }

    #[test]
    fn https_is_tls() {
        assert_eq!(channel_of("https://gitlab.invalid", None), Channel::Tls);
    }

    #[test]
    fn loopback_and_private_hosts_are_private() {
        assert_eq!(
            channel_of("http://localhost:8080", None),
            Channel::Localhost
        );
        for base_url in [
            "http://127.0.0.1",
            "http://10.0.0.5",
            "http://[::1]",
            "http://[fd00::1]",
            "http://[::ffff:10.0.0.5]",
        ] {
            assert_eq!(
                channel_of(base_url, None),
                Channel::PrivateAddress,
                "{base_url}"
            );
        }
    }

    #[test]
    fn public_and_link_local_addresses_are_insecure() {
        for base_url in [
            "http://203.0.113.10",
            "http://169.254.0.1",
            "http://[fe80::1]",
        ] {
            let channel = channel_of(base_url, None);
            assert_eq!(channel, Channel::PublicAddress, "{base_url}");
            assert!(!channel.is_tls_or_private(), "{base_url}");
        }
    }

    #[test]
    fn an_unresolvable_host_is_insecure() {
        let channel = channel_of("http://gitlab.invalid", None);
        assert_eq!(channel, Channel::Unresolved);
        assert!(!channel.is_tls_or_private());
    }

    #[test]
    fn resolve_host_decides_the_channel_of_a_domain() {
        assert_eq!(
            channel_of("http://gitlab.example.com", Some("127.0.0.1")),
            Channel::PrivateAddress
        );
        assert_eq!(
            channel_of("http://gitlab.example.com", Some("203.0.113.10")),
            Channel::PublicAddress
        );
    }

    #[test]
    fn resolve_host_decides_the_channel_of_localhost() {
        assert_eq!(
            channel_of("http://localhost:8080", Some("203.0.113.10")),
            Channel::PublicAddress
        );
        assert_eq!(
            channel_of("http://localhost:8080", Some("127.0.0.1")),
            Channel::PrivateAddress
        );
    }

    #[test]
    fn resolve_host_does_not_apply_to_an_ip_literal() {
        assert_eq!(
            channel_of("http://203.0.113.10", Some("127.0.0.1")),
            Channel::PublicAddress
        );
    }

    #[tokio::test]
    async fn redirects_are_not_followed() {
        use axum::Router;
        use axum::http::StatusCode as AxumStatus;
        use axum::http::header::LOCATION;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let target_hits = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&target_hits);
        let target = Router::new().fallback(move || {
            counter.fetch_add(1, Ordering::SeqCst);
            async { axum::Json(serde_json::json!({"project_id": 1, "default_branch": "main"})) }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, target).await.unwrap() });

        let location = format!("http://{target_addr}/api/v4/internal/orbit/project/1/info");
        let redirector = Router::new().fallback(move || {
            let location = location.clone();
            async move { (AxumStatus::TEMPORARY_REDIRECT, [(LOCATION, location)]) }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, redirector).await.unwrap() });

        let client =
            GitlabClient::new(config_with_resolve(&format!("http://{addr}"), None)).unwrap();

        assert!(client.project_info(1).await.is_err());
        assert_eq!(target_hits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn credentialed_requests_on_an_insecure_channel_succeed_and_are_counted() {
        use axum::Router;
        use axum::routing::get;
        use opentelemetry::metrics::MeterProvider;
        use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
        use opentelemetry_sdk::metrics::{
            InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
        };

        let app = Router::new()
            .route(
                "/api/v4/internal/orbit/project/{id}/info",
                get(|| async {
                    axum::Json(serde_json::json!({"project_id": 1, "default_branch": "main"}))
                }),
            )
            .route(
                "/api/v4/internal/orbit/project/{id}/repository/list_blobs",
                axum::routing::post(|| async { Vec::<u8>::new() }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        let meter = provider.meter("test");
        let base_url = format!("http://{addr}");
        let private = GitlabClient::new(config_with_resolve(&base_url, None))
            .unwrap()
            .with_meter(&meter);
        let insecure = GitlabClient::new(config_with_resolve(&base_url, None))
            .unwrap()
            .with_meter(&meter)
            .with_channel(Channel::PublicAddress);

        private.project_info(1).await.unwrap();
        insecure.project_info(1).await.unwrap();
        insecure.project_info(1).await.unwrap();
        let _stream = insecure.list_blobs(1, &["oid".to_string()]).await.unwrap();
        provider.force_flush().unwrap();

        let mut counted: Vec<(u64, String)> = exporter
            .get_finished_metrics()
            .unwrap()
            .iter()
            .flat_map(|resource| resource.scope_metrics())
            .flat_map(|scope| scope.metrics())
            .filter(|metric| metric.name() == INSECURE_CREDENTIAL_REQUESTS.otel_name)
            .flat_map(|metric| match metric.data() {
                AggregatedMetrics::U64(MetricData::Sum(sum)) => sum
                    .data_points()
                    .map(|point| {
                        let method = point
                            .attributes()
                            .find(|kv| kv.key.as_str() == labels::METHOD)
                            .map(|kv| kv.value.as_str().into_owned())
                            .unwrap_or_default();
                        (point.value(), method)
                    })
                    .collect::<Vec<_>>(),
                _ => panic!("insecure credential requests is not a u64 sum"),
            })
            .collect();
        counted.sort();
        assert_eq!(
            counted,
            vec![(1, "POST".to_string()), (2, "GET".to_string())]
        );
    }
}
