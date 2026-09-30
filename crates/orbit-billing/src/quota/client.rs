use std::time::Duration;

use reqwest::StatusCode;
use reqwest::header::{CACHE_CONTROL, HeaderMap, HeaderName, HeaderValue};
use secrecy::ExposeSecret;
use tracing::warn;

use super::key::CdotRequest;
use crate::constants::{APP_ID, CDOT_QUOTA_PATH};

const X_ADMIN_EMAIL: HeaderName = HeaderName::from_static("x-admin-email");
const X_ADMIN_TOKEN: HeaderName = HeaderName::from_static("x-admin-token");
const X_LICENSE_TOKEN: HeaderName = HeaderName::from_static("x-license-token");

pub(crate) enum QuotaAuth {
    AdminToken { user: String, token: String },
    LicenseChecksum,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum QuotaDecision {
    Allow,
    Deny(DenyReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DenyReason {
    QuotaExhausted,
}

impl DenyReason {
    pub(crate) fn message(self) -> &'static str {
        match self {
            DenyReason::QuotaExhausted => "GitLab credits exhausted",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum QuotaOutcome {
    Decided {
        decision: QuotaDecision,
        ttl: Duration,
    },
    FailOpen(FailOpenReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FailOpenReason {
    Unreachable,
    Unauthorized,
    UnexpectedResponse,
}

pub(crate) struct QuotaClient {
    http: reqwest::Client,
    base_url: String,
    default_ttl: Duration,
    license_auth: bool,
}

impl QuotaClient {
    pub(crate) fn new(
        base_url: String,
        auth: QuotaAuth,
        request_timeout: Duration,
        default_ttl: Duration,
    ) -> Result<Self, reqwest::Error> {
        let mut headers = HeaderMap::new();
        let license_auth = match auth {
            QuotaAuth::AdminToken { user, token } => {
                if let Ok(v) = HeaderValue::from_str(&user) {
                    headers.insert(X_ADMIN_EMAIL, v);
                }
                if let Ok(mut v) = HeaderValue::from_str(&token) {
                    v.set_sensitive(true);
                    headers.insert(X_ADMIN_TOKEN, v);
                }
                false
            }
            QuotaAuth::LicenseChecksum => true,
        };

        let http = reqwest::Client::builder()
            .user_agent(format!("{APP_ID}/{}", orbit_utils::version::get()))
            .timeout(request_timeout)
            .default_headers(headers)
            .build()?;
        Ok(Self {
            http,
            base_url,
            default_ttl,
            license_auth,
        })
    }

    pub(crate) async fn check(&self, request: &CdotRequest) -> QuotaOutcome {
        let url = format!("{}{}", self.base_url.trim_end_matches('/'), CDOT_QUOTA_PATH);
        let params = request.as_query_params();

        let mut builder = self.http.head(&url).query(&params);
        if self.license_auth {
            // Without the header CDot can only answer 401, so an unusable claim fails open here
            // instead of sending an unauthenticated request.
            let Some(mut token) = request
                .license_checksum
                .as_ref()
                .and_then(|c| HeaderValue::from_str(c.expose_secret()).ok())
            else {
                warn!(
                    user_id = %request.key.user_id,
                    instance_id = %request.key.instance_id,
                    unique_instance_id = %request.key.unique_instance_id,
                    feature_qualified_name = %request.key.feature_qualified_name,
                    "license_checksum claim is missing or not a valid header value; failing open"
                );
                return QuotaOutcome::FailOpen(FailOpenReason::Unauthorized);
            };
            token.set_sensitive(true);
            builder = builder.header(X_LICENSE_TOKEN, token);
        }

        let response = match builder.send().await {
            Ok(r) => r,
            Err(e) => {
                warn!(
                    error = %e,
                    user_id = %request.key.user_id,
                    realm = %request.key.realm,
                    root_namespace_id = %request.key.root_namespace_id,
                    global_user_id = %request.global_user_id,
                    instance_id = %request.key.instance_id,
                    unique_instance_id = %request.key.unique_instance_id,
                    feature_qualified_name = %request.key.feature_qualified_name,
                    "quota check request failed; failing open"
                );
                return QuotaOutcome::FailOpen(FailOpenReason::Unreachable);
            }
        };

        let status = response.status();
        let ttl = parse_max_age(response.headers().get(CACHE_CONTROL)).unwrap_or(self.default_ttl);

        match status {
            StatusCode::OK => QuotaOutcome::Decided {
                decision: QuotaDecision::Allow,
                ttl,
            },
            StatusCode::PAYMENT_REQUIRED => QuotaOutcome::Decided {
                decision: QuotaDecision::Deny(DenyReason::QuotaExhausted),
                ttl,
            },
            other => {
                warn!(
                    status = %other,
                    user_id = %request.key.user_id,
                    realm = %request.key.realm,
                    root_namespace_id = %request.key.root_namespace_id,
                    global_user_id = %request.global_user_id,
                    instance_id = %request.key.instance_id,
                    unique_instance_id = %request.key.unique_instance_id,
                    feature_qualified_name = %request.key.feature_qualified_name,
                    "unexpected quota check response; failing open"
                );
                let reason = if other == StatusCode::UNAUTHORIZED {
                    FailOpenReason::Unauthorized
                } else {
                    FailOpenReason::UnexpectedResponse
                };
                QuotaOutcome::FailOpen(reason)
            }
        }
    }
}

fn parse_max_age(header: Option<&HeaderValue>) -> Option<Duration> {
    let raw = header?.to_str().ok()?;
    for directive in raw.split(',') {
        let directive = directive.trim();
        if let Some(v) = directive
            .strip_prefix("max-age=")
            .or_else(|| directive.strip_prefix("max-age= "))
            && let Ok(secs) = v.trim().parse::<u64>()
        {
            return Some(Duration::from_secs(secs));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::http::{HeaderMap as AxumHeaderMap, StatusCode as AxumStatus, Uri};
    use axum::routing::head;
    use reqwest::header::HeaderMap;
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;

    fn hv(s: &str) -> HeaderValue {
        HeaderValue::from_str(s).unwrap()
    }

    fn sample_request() -> CdotRequest {
        CdotRequest {
            key: super::super::key::CacheKey {
                realm: "SaaS".into(),
                user_id: "1".into(),
                root_namespace_id: "9970".into(),
                instance_id: String::new(),
                unique_instance_id: "u".into(),
                event_type: "orbit_workflow_completion".into(),
                feature_qualified_name: "orbit_mcp".into(),
            },
            global_user_id: "g".into(),
            instance_version: "19.5.0".into(),
            license_checksum: None,
            correlation_id: String::new(),
        }
    }

    fn admin_auth() -> QuotaAuth {
        QuotaAuth::AdminToken {
            user: "test@example.com".into(),
            token: "test-token".into(),
        }
    }

    fn expected_user_agent() -> String {
        format!("gkg-server/{}", orbit_utils::version::get())
    }

    const CHECKSUM: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    async fn recording_server(
        status: AxumStatus,
    ) -> (String, Arc<Mutex<Vec<(AxumHeaderMap, String)>>>) {
        install_crypto();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let recorder = seen.clone();
        let app = Router::new().route(
            crate::constants::CDOT_QUOTA_PATH,
            head(move |headers: AxumHeaderMap, uri: Uri| {
                let recorder = recorder.clone();
                async move {
                    let query = uri.query().unwrap_or_default().to_string();
                    recorder.lock().unwrap().push((headers, query));
                    status
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), seen)
    }

    fn license_request() -> CdotRequest {
        let mut request = sample_request();
        request.key.realm = "self-managed".into();
        request.key.root_namespace_id = String::new();
        request.key.instance_id = "inst-1".into();
        request.license_checksum = Some(CHECKSUM.into());
        request
    }

    fn license_client(url: String) -> QuotaClient {
        QuotaClient::new(
            url,
            QuotaAuth::LicenseChecksum,
            Duration::from_secs(5),
            Duration::from_secs(3600),
        )
        .unwrap()
    }

    fn install_crypto() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    }

    async fn stub_server(status: AxumStatus, cache_control: Option<&'static str>) -> String {
        install_crypto();
        let app = Router::new().route(
            crate::constants::CDOT_QUOTA_PATH,
            head(move || async move {
                let mut headers = AxumHeaderMap::new();
                if let Some(cc) = cache_control {
                    headers.insert("cache-control", cc.parse().unwrap());
                }
                (status, headers)
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    #[test]
    fn parses_max_age() {
        let mut h = HeaderMap::new();
        h.insert(CACHE_CONTROL, hv("public, max-age=120"));
        assert_eq!(
            parse_max_age(h.get(CACHE_CONTROL)),
            Some(Duration::from_secs(120))
        );
    }

    #[test]
    fn ignores_non_max_age_directives() {
        let mut h = HeaderMap::new();
        h.insert(CACHE_CONTROL, hv("no-cache, no-store"));
        assert_eq!(parse_max_age(h.get(CACHE_CONTROL)), None);
    }

    #[test]
    fn invalid_max_age_returns_none() {
        let mut h = HeaderMap::new();
        h.insert(CACHE_CONTROL, hv("max-age=abc"));
        assert_eq!(parse_max_age(h.get(CACHE_CONTROL)), None);
    }

    #[tokio::test]
    async fn status_200_maps_to_allow() {
        let url = stub_server(AxumStatus::OK, Some("max-age=60")).await;
        let client = QuotaClient::new(
            url,
            admin_auth(),
            Duration::from_secs(5),
            Duration::from_secs(3600),
        )
        .unwrap();
        let outcome = client.check(&sample_request()).await;
        match outcome {
            QuotaOutcome::Decided { decision, ttl } => {
                assert_eq!(decision, QuotaDecision::Allow);
                assert_eq!(ttl, Duration::from_secs(60));
            }
            other => panic!("expected Decided, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn status_402_maps_to_quota_exhausted() {
        let url = stub_server(AxumStatus::PAYMENT_REQUIRED, None).await;
        let client = QuotaClient::new(
            url,
            admin_auth(),
            Duration::from_secs(5),
            Duration::from_secs(42),
        )
        .unwrap();
        let outcome = client.check(&sample_request()).await;
        match outcome {
            QuotaOutcome::Decided { decision, ttl } => {
                assert_eq!(decision, QuotaDecision::Deny(DenyReason::QuotaExhausted));
                assert_eq!(ttl, Duration::from_secs(42));
            }
            other => panic!("expected Decided, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn status_403_fails_open_as_unexpected_response() {
        let url = stub_server(AxumStatus::FORBIDDEN, None).await;
        let client = QuotaClient::new(
            url,
            admin_auth(),
            Duration::from_secs(5),
            Duration::from_secs(3600),
        )
        .unwrap();
        assert_eq!(
            client.check(&sample_request()).await,
            QuotaOutcome::FailOpen(FailOpenReason::UnexpectedResponse)
        );
    }

    #[tokio::test]
    async fn connection_error_fails_open_as_unreachable() {
        // Port 1 is reserved and unroutable; the TCP connect fails before any HTTP exchange.
        install_crypto();
        let client = QuotaClient::new(
            "http://127.0.0.1:1".into(),
            admin_auth(),
            Duration::from_millis(500),
            Duration::from_secs(3600),
        )
        .unwrap();
        assert_eq!(
            client.check(&sample_request()).await,
            QuotaOutcome::FailOpen(FailOpenReason::Unreachable)
        );
    }

    #[tokio::test]
    async fn license_mode_sends_license_token_and_no_admin_headers() {
        let (url, seen) = recording_server(AxumStatus::OK).await;
        let outcome = license_client(url).check(&license_request()).await;
        assert!(matches!(outcome, QuotaOutcome::Decided { .. }));

        let seen = seen.lock().unwrap();
        let (headers, query) = &seen[0];
        assert_eq!(headers.get("user-agent").unwrap(), &expected_user_agent());
        assert_eq!(headers.get("x-license-token").unwrap(), CHECKSUM);
        assert!(headers.get("x-admin-email").is_none());
        assert!(headers.get("x-admin-token").is_none());
        for param in [
            "realm=self-managed",
            "instance_id=inst-1",
            "unique_instance_id=u",
            "instance_version=19.5.0",
        ] {
            assert!(
                query.split('&').any(|p| p == param),
                "{param} missing from {query}"
            );
        }
    }

    #[tokio::test]
    async fn admin_mode_sends_admin_headers_and_ignores_license_checksum() {
        let (url, seen) = recording_server(AxumStatus::OK).await;
        let client = QuotaClient::new(
            url,
            admin_auth(),
            Duration::from_secs(5),
            Duration::from_secs(3600),
        )
        .unwrap();
        client.check(&license_request()).await;

        let seen = seen.lock().unwrap();
        let (headers, _) = &seen[0];
        assert_eq!(headers.get("user-agent").unwrap(), &expected_user_agent());
        assert_eq!(headers.get("x-admin-email").unwrap(), "test@example.com");
        assert_eq!(headers.get("x-admin-token").unwrap(), "test-token");
        assert!(headers.get("x-license-token").is_none());
    }

    #[tokio::test]
    async fn license_mode_with_unencodable_checksum_fails_open_without_calling_cdot() {
        let (url, seen) = recording_server(AxumStatus::OK).await;
        let mut request = license_request();
        request.license_checksum = Some("bad\nvalue".into());

        assert_eq!(
            license_client(url).check(&request).await,
            QuotaOutcome::FailOpen(FailOpenReason::Unauthorized)
        );
        assert!(seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn license_mode_401_fails_open_as_unauthorized() {
        let (url, _) = recording_server(AxumStatus::UNAUTHORIZED).await;
        assert_eq!(
            license_client(url).check(&license_request()).await,
            QuotaOutcome::FailOpen(FailOpenReason::Unauthorized)
        );
    }

    #[tokio::test]
    async fn admin_mode_401_fails_open_as_unauthorized() {
        let (url, _) = recording_server(AxumStatus::UNAUTHORIZED).await;
        let client = QuotaClient::new(
            url,
            admin_auth(),
            Duration::from_secs(5),
            Duration::from_secs(3600),
        )
        .unwrap();
        assert_eq!(
            client.check(&sample_request()).await,
            QuotaOutcome::FailOpen(FailOpenReason::Unauthorized)
        );
    }

    #[tokio::test]
    async fn sends_correlation_id_query_param() {
        let (url, seen) = recording_server(AxumStatus::OK).await;
        let mut request = license_request();
        request.correlation_id = "req-123".into();
        license_client(url).check(&request).await;

        let seen = seen.lock().unwrap();
        let (_, query) = &seen[0];
        assert!(
            query.split('&').any(|p| p == "correlation_id=req-123"),
            "correlation_id missing from {query}"
        );
    }
}
