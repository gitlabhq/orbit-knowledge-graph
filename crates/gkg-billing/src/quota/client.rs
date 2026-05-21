use std::time::Duration;

use reqwest::StatusCode;
use reqwest::header::{CACHE_CONTROL, HeaderMap, HeaderName, HeaderValue};
use tracing::warn;

use super::key::CdotRequest;
use crate::constants::CDOT_QUOTA_PATH;

const X_ADMIN_EMAIL: HeaderName = HeaderName::from_static("x-admin-email");
const X_ADMIN_TOKEN: HeaderName = HeaderName::from_static("x-admin-token");

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
    FailOpen,
}

pub(crate) struct QuotaClient {
    http: reqwest::Client,
    base_url: String,
    default_ttl: Duration,
}

impl QuotaClient {
    pub(crate) fn new(
        base_url: String,
        api_user: &str,
        api_token: &str,
        request_timeout: Duration,
        default_ttl: Duration,
    ) -> Result<Self, reqwest::Error> {
        let mut headers = HeaderMap::new();
        if let Ok(v) = HeaderValue::from_str(api_user) {
            headers.insert(X_ADMIN_EMAIL, v);
        }
        if let Ok(mut v) = HeaderValue::from_str(api_token) {
            v.set_sensitive(true);
            headers.insert(X_ADMIN_TOKEN, v);
        }

        let http = reqwest::Client::builder()
            .timeout(request_timeout)
            .default_headers(headers)
            .build()?;
        Ok(Self {
            http,
            base_url,
            default_ttl,
        })
    }

    pub(crate) async fn check(&self, request: &CdotRequest) -> QuotaOutcome {
        let url = format!("{}{}", self.base_url.trim_end_matches('/'), CDOT_QUOTA_PATH);
        let params = request.as_query_params();

        let response = match self.http.head(&url).query(&params).send().await {
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
                return QuotaOutcome::FailOpen;
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
                QuotaOutcome::FailOpen
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
    use axum::http::{HeaderMap as AxumHeaderMap, StatusCode as AxumStatus};
    use axum::routing::head;
    use reqwest::header::HeaderMap;
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
                feature_qualified_name: "orbit-mcp".into(),
            },
            global_user_id: "g".into(),
        }
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
            "test@example.com",
            "test-token",
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
            "test@example.com",
            "test-token",
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
    async fn status_403_fails_open() {
        let url = stub_server(AxumStatus::FORBIDDEN, None).await;
        let client = QuotaClient::new(
            url,
            "test@example.com",
            "test-token",
            Duration::from_secs(5),
            Duration::from_secs(3600),
        )
        .unwrap();
        assert_eq!(
            client.check(&sample_request()).await,
            QuotaOutcome::FailOpen
        );
    }

    #[tokio::test]
    async fn connection_error_fails_open() {
        // Port 1 is reserved and unroutable; the TCP connect fails before any HTTP exchange.
        install_crypto();
        let client = QuotaClient::new(
            "http://127.0.0.1:1".into(),
            "test@example.com",
            "test-token",
            Duration::from_millis(500),
            Duration::from_secs(3600),
        )
        .unwrap();
        assert_eq!(
            client.check(&sample_request()).await,
            QuotaOutcome::FailOpen
        );
    }
}
