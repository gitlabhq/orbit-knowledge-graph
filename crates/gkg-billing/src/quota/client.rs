use std::time::Duration;

use reqwest::StatusCode;
use reqwest::header::{CACHE_CONTROL, HeaderMap, HeaderName, HeaderValue};
use tracing::warn;

use super::key::CacheKey;

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
    EntitlementFailed,
}

impl DenyReason {
    pub(crate) fn message(self) -> &'static str {
        match self {
            DenyReason::QuotaExhausted => "Orbit query quota exhausted",
            DenyReason::EntitlementFailed => "Orbit entitlement check failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum QuotaOutcome {
    // Decision came back cleanly. `ttl` is the `Cache-Control: max-age` value if
    // present, otherwise the configured fallback. Callers cache by this TTL.
    Decided {
        decision: QuotaDecision,
        ttl: Duration,
    },
    // Transient failure (timeout, connection error, 5xx, unparseable). Callers
    // must fail-open and must NOT cache this outcome — re-checking next request
    // is cheap compared to pinning a stale error for an hour.
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
        // Bake the admin credentials into the client's default headers so
        // every call carries `X-Admin-Email` / `X-Admin-Token`. CDot's
        // `Api::V1::Consumers::ResolveController` uses these for the Admin
        // token authentication handler — without them the resolve endpoint
        // returns 401, which would silently fall through to FailOpen.
        let mut headers = HeaderMap::new();
        // Invalid header values (control chars, etc.) shouldn't happen with
        // well-formed credentials from config, but treat parse failures the
        // same as empty creds by skipping the header — caller gates the
        // service to disabled when creds are blank, so we shouldn't end up
        // here with garbage in normal operation.
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

    pub(crate) async fn check(&self, key: &CacheKey) -> QuotaOutcome {
        let url = format!(
            "{}/api/v1/consumers/resolve",
            self.base_url.trim_end_matches('/')
        );
        let params = key.as_query_params();

        let response = match self.http.head(&url).query(&params).send().await {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "quota check request failed; failing open");
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
            StatusCode::FORBIDDEN => QuotaOutcome::Decided {
                decision: QuotaDecision::Deny(DenyReason::EntitlementFailed),
                ttl,
            },
            other => {
                warn!(status = %other, "unexpected quota check response; failing open");
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

    fn sample_key() -> CacheKey {
        CacheKey {
            environment: "production".into(),
            realm: "SaaS".into(),
            user_id: "1".into(),
            global_user_id: "g".into(),
            root_namespace_id: "9970".into(),
            unique_instance_id: "u".into(),
            event_type: "orbit_workflow_completion".into(),
            feature_qualified_name: "orbit-mcp".into(),
        }
    }

    fn install_crypto() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    }

    async fn stub_server(status: AxumStatus, cache_control: Option<&'static str>) -> String {
        install_crypto();
        let app = Router::new().route(
            "/api/v1/consumers/resolve",
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
    fn parses_max_age_directive() {
        let mut h = HeaderMap::new();
        h.insert(CACHE_CONTROL, hv("public, max-age=120"));
        assert_eq!(
            parse_max_age(h.get(CACHE_CONTROL)),
            Some(Duration::from_secs(120))
        );
    }

    #[test]
    fn parses_max_age_solo() {
        let mut h = HeaderMap::new();
        h.insert(CACHE_CONTROL, hv("max-age=45"));
        assert_eq!(
            parse_max_age(h.get(CACHE_CONTROL)),
            Some(Duration::from_secs(45))
        );
    }

    #[test]
    fn ignores_non_max_age_directives() {
        let mut h = HeaderMap::new();
        h.insert(CACHE_CONTROL, hv("no-cache, no-store"));
        assert_eq!(parse_max_age(h.get(CACHE_CONTROL)), None);
    }

    #[test]
    fn none_when_header_absent() {
        assert_eq!(parse_max_age(None), None);
    }

    #[test]
    fn invalid_max_age_returns_none() {
        let mut h = HeaderMap::new();
        h.insert(CACHE_CONTROL, hv("max-age=abc"));
        assert_eq!(parse_max_age(h.get(CACHE_CONTROL)), None);
    }

    #[tokio::test]
    async fn two_hundred_maps_to_allow() {
        let url = stub_server(AxumStatus::OK, Some("max-age=60")).await;
        let client = QuotaClient::new(
            url,
            "test@example.com",
            "test-token",
            Duration::from_secs(5),
            Duration::from_secs(3600),
        )
        .unwrap();
        let outcome = client.check(&sample_key()).await;
        match outcome {
            QuotaOutcome::Decided { decision, ttl } => {
                assert_eq!(decision, QuotaDecision::Allow);
                assert_eq!(ttl, Duration::from_secs(60));
            }
            other => panic!("expected Decided, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn four_oh_two_maps_to_quota_exhausted() {
        let url = stub_server(AxumStatus::PAYMENT_REQUIRED, None).await;
        let client = QuotaClient::new(
            url,
            "test@example.com",
            "test-token",
            Duration::from_secs(5),
            Duration::from_secs(42),
        )
        .unwrap();
        let outcome = client.check(&sample_key()).await;
        match outcome {
            QuotaOutcome::Decided { decision, ttl } => {
                assert_eq!(decision, QuotaDecision::Deny(DenyReason::QuotaExhausted));
                assert_eq!(ttl, Duration::from_secs(42));
            }
            other => panic!("expected Decided, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn four_oh_three_maps_to_entitlement_failed() {
        let url = stub_server(AxumStatus::FORBIDDEN, None).await;
        let client = QuotaClient::new(
            url,
            "test@example.com",
            "test-token",
            Duration::from_secs(5),
            Duration::from_secs(3600),
        )
        .unwrap();
        let outcome = client.check(&sample_key()).await;
        assert!(matches!(
            outcome,
            QuotaOutcome::Decided {
                decision: QuotaDecision::Deny(DenyReason::EntitlementFailed),
                ..
            }
        ));
    }

    #[tokio::test]
    async fn five_xx_fails_open() {
        let url = stub_server(AxumStatus::INTERNAL_SERVER_ERROR, None).await;
        let client = QuotaClient::new(
            url,
            "test@example.com",
            "test-token",
            Duration::from_secs(5),
            Duration::from_secs(3600),
        )
        .unwrap();
        assert_eq!(client.check(&sample_key()).await, QuotaOutcome::FailOpen);
    }

    #[tokio::test]
    async fn teapot_fails_open() {
        // Covers non-5xx unexpected statuses: anything outside the
        // {200, 402, 403} mapping must fall through to FailOpen so that
        // surprise codes (418, 503, 504, gateway proxies, ...) don't block
        // traffic.
        let url = stub_server(AxumStatus::IM_A_TEAPOT, None).await;
        let client = QuotaClient::new(
            url,
            "test@example.com",
            "test-token",
            Duration::from_secs(5),
            Duration::from_secs(3600),
        )
        .unwrap();
        assert_eq!(client.check(&sample_key()).await, QuotaOutcome::FailOpen);
    }

    #[tokio::test]
    async fn connection_error_fails_open() {
        install_crypto();
        // Unroutable port — the connect fails before any response can be read.
        let client = QuotaClient::new(
            "http://127.0.0.1:1".into(),
            "test@example.com",
            "test-token",
            Duration::from_millis(500),
            Duration::from_secs(3600),
        )
        .unwrap();
        assert_eq!(client.check(&sample_key()).await, QuotaOutcome::FailOpen);
    }
}
