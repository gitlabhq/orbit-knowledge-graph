use std::sync::Arc;
use std::time::Duration;

use circuit_breaker::{CircuitBreaker, CircuitBreakerError, CircuitConfig};
use orbit_log_fields::warn;
use orbit_observability::resilience::MetricsObserver;
use reqwest::header::{CACHE_CONTROL, HeaderMap, HeaderName, HeaderValue};
use reqwest::{RequestBuilder, Response, StatusCode};
use secrecy::ExposeSecret;
use serde::Deserialize;

use super::key::CdotRequest;
use crate::constants::{APP_ID, CDOT_QUOTA_PATH, normalize_realm};

const X_ADMIN_EMAIL: HeaderName = HeaderName::from_static("x-admin-email");
const X_ADMIN_TOKEN: HeaderName = HeaderName::from_static("x-admin-token");
const X_LICENSE_TOKEN: HeaderName = HeaderName::from_static("x-license-token");
const RETRY_BACKOFF: Duration = Duration::from_millis(250);
const CIRCUIT_CONFIG: CircuitConfig = CircuitConfig {
    failure_threshold: 2,
    window: Duration::from_secs(30),
    cooldown: Duration::from_secs(15),
};

pub(crate) enum QuotaAuth {
    AdminToken { user: String, token: String },
    LicenseChecksum,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum QuotaDecision {
    Allow,
    Deny(Option<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum QuotaOutcome {
    Decided {
        decision: QuotaDecision,
        ttl: Duration,
    },
    Failed {
        reason: FailureReason,
        block_reason: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum FailureReason {
    Unreachable,
    ServerError,
    CircuitOpen,
    Unauthorized,
    UnexpectedResponse,
}

impl FailureReason {
    pub(crate) fn is_outage(self) -> bool {
        matches!(
            self,
            Self::Unreachable | Self::ServerError | Self::CircuitOpen
        )
    }
}

enum Outage {
    Unreachable(reqwest::Error),
    ServerError(Box<Response>),
}

pub(crate) struct QuotaClient {
    http: reqwest::Client,
    base_url: String,
    default_ttl: Duration,
    license_auth: bool,
    circuit_breaker: CircuitBreaker,
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
            .redirect(reqwest::redirect::Policy::none())
            .default_headers(headers)
            .build()?;
        Ok(Self {
            http,
            base_url,
            default_ttl,
            license_auth,
            circuit_breaker: CircuitBreaker::new(
                "customers_dot",
                CIRCUIT_CONFIG,
                Arc::new(MetricsObserver::new()),
            ),
        })
    }

    #[cfg(test)]
    fn with_circuit_breaker(mut self, circuit_breaker: CircuitBreaker) -> Self {
        self.circuit_breaker = circuit_breaker;
        self
    }

    pub(crate) async fn check(&self, request: &CdotRequest) -> QuotaOutcome {
        let url = format!("{}{}", self.base_url.trim_end_matches('/'), CDOT_QUOTA_PATH);
        let params = request.as_query_params();

        let mut builder = self.http.get(&url).query(&params);
        if self.license_auth {
            // Without the header CDot can only answer 401, so an unusable claim fails closed
            // here instead of sending an unauthenticated request.
            let Some(mut token) = request
                .license_checksum
                .as_ref()
                .and_then(|c| HeaderValue::from_str(c.expose_secret()).ok())
            else {
                warn!(
                    safe(INSTANCE_ID) = request.key.instance_id.as_str(),
                    safe(UNIQUE_INSTANCE_ID) = request.key.unique_instance_id.as_str(),
                    safe(FEATURE_QUALIFIED_NAME) = request.key.feature_qualified_name.as_str(),
                    safe(CORRELATION_ID) = request.correlation_id.as_str(),
                    "license_checksum claim is missing or not a valid header value; failing closed"
                );
                return QuotaOutcome::Failed {
                    reason: FailureReason::Unauthorized,
                    block_reason: None,
                };
            };
            token.set_sensitive(true);
            builder = builder.header(X_LICENSE_TOKEN, token);
        }

        let response = match self
            .circuit_breaker
            .call(|| send_with_one_retry(builder, request))
            .await
        {
            Ok(response) => response,
            Err(CircuitBreakerError::Inner(Outage::ServerError(response))) => *response,
            Err(CircuitBreakerError::Open { .. }) => {
                return QuotaOutcome::Failed {
                    reason: FailureReason::CircuitOpen,
                    block_reason: None,
                };
            }
            Err(CircuitBreakerError::Inner(Outage::Unreachable(e))) => {
                warn!(
                    safe(ERROR_TYPE) = "quota_request",
                    expanded(ERROR_MESSAGE) = %e,
                    safe(REALM) = normalize_realm(&request.key.realm),
                    safe(INSTANCE_ID) = request.key.instance_id.as_str(),
                    safe(UNIQUE_INSTANCE_ID) = request.key.unique_instance_id.as_str(),
                    safe(FEATURE_QUALIFIED_NAME) = request.key.feature_qualified_name.as_str(),
                    safe(CORRELATION_ID) = request.correlation_id.as_str(),
                    "quota check request failed"
                );
                return QuotaOutcome::Failed {
                    reason: FailureReason::Unreachable,
                    block_reason: None,
                };
            }
        };

        let status = response.status();
        let ttl = parse_max_age(response.headers().get(CACHE_CONTROL)).unwrap_or(self.default_ttl);

        if status == StatusCode::OK {
            return QuotaOutcome::Decided {
                decision: QuotaDecision::Allow,
                ttl,
            };
        }

        let block_reason = read_block_reason(response).await;
        if status == StatusCode::PAYMENT_REQUIRED {
            return QuotaOutcome::Decided {
                decision: QuotaDecision::Deny(block_reason),
                ttl,
            };
        }

        warn!(
            safe(HTTP_STATUS_CODE) = status.as_u16(),
            expanded(BLOCK_REASON) = block_reason.as_deref().unwrap_or("none"),
            safe(REALM) = normalize_realm(&request.key.realm),
            safe(INSTANCE_ID) = request.key.instance_id.as_str(),
            safe(UNIQUE_INSTANCE_ID) = request.key.unique_instance_id.as_str(),
            safe(FEATURE_QUALIFIED_NAME) = request.key.feature_qualified_name.as_str(),
            safe(CORRELATION_ID) = request.correlation_id.as_str(),
            "unexpected quota check response"
        );
        let reason = match status {
            StatusCode::UNAUTHORIZED => FailureReason::Unauthorized,
            status if status.is_server_error() => FailureReason::ServerError,
            _ => FailureReason::UnexpectedResponse,
        };
        QuotaOutcome::Failed {
            reason,
            block_reason,
        }
    }
}

async fn send_with_one_retry(
    builder: RequestBuilder,
    request: &CdotRequest,
) -> Result<Response, Outage> {
    let retry = builder.try_clone();
    let first = classify_outage(builder.send().await);
    let (Some(retry), Err(outage)) = (retry, &first) else {
        return first;
    };

    let (status, error) = match outage {
        Outage::ServerError(response) => (Some(response.status().as_u16()), None),
        Outage::Unreachable(e) => (None, Some(e.to_string())),
    };
    warn!(
        safe(ERROR_TYPE) = "quota_request",
        safe(HTTP_STATUS_CODE) = status,
        expanded(ERROR_MESSAGE) = error,
        safe(REALM) = normalize_realm(&request.key.realm),
        safe(INSTANCE_ID) = request.key.instance_id.as_str(),
        safe(UNIQUE_INSTANCE_ID) = request.key.unique_instance_id.as_str(),
        safe(FEATURE_QUALIFIED_NAME) = request.key.feature_qualified_name.as_str(),
        safe(CORRELATION_ID) = request.correlation_id.as_str(),
        "quota check failed transiently; retrying once"
    );
    tokio::time::sleep(RETRY_BACKOFF).await;
    classify_outage(retry.send().await)
}

fn classify_outage(result: reqwest::Result<Response>) -> Result<Response, Outage> {
    match result {
        Ok(response) if response.status().is_server_error() => {
            Err(Outage::ServerError(Box::new(response)))
        }
        Ok(response) => Ok(response),
        Err(e) => Err(Outage::Unreachable(e)),
    }
}

#[derive(Deserialize)]
struct BlockBody {
    block_reason: String,
}

async fn read_block_reason(response: reqwest::Response) -> Option<String> {
    let body = response.bytes().await.ok()?;
    serde_json::from_slice::<BlockBody>(&body)
        .ok()
        .map(|b| b.block_reason)
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
    use axum::routing::get;
    use reqwest::header::HeaderMap;
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;

    fn failed(reason: FailureReason) -> QuotaOutcome {
        QuotaOutcome::Failed {
            reason,
            block_reason: None,
        }
    }

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
            get(move |headers: AxumHeaderMap, uri: Uri| {
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

    async fn sequenced_server(statuses: Vec<AxumStatus>) -> (String, Arc<Mutex<usize>>) {
        install_crypto();
        let calls = Arc::new(Mutex::new(0));
        let counter = calls.clone();
        let app = Router::new().route(
            crate::constants::CDOT_QUOTA_PATH,
            get(move || {
                let counter = counter.clone();
                let statuses = statuses.clone();
                async move {
                    let mut calls = counter.lock().unwrap();
                    let status = statuses[(*calls).min(statuses.len() - 1)];
                    *calls += 1;
                    status
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), calls)
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
        stub_server_with_body(status, cache_control, "").await
    }

    async fn stub_server_with_body(
        status: AxumStatus,
        cache_control: Option<&'static str>,
        body: &'static str,
    ) -> String {
        install_crypto();
        let app = Router::new().route(
            crate::constants::CDOT_QUOTA_PATH,
            get(move || async move {
                let mut headers = AxumHeaderMap::new();
                if let Some(cc) = cache_control {
                    headers.insert("cache-control", cc.parse().unwrap());
                }
                (status, headers, body)
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
                assert_eq!(decision, QuotaDecision::Deny(None));
                assert_eq!(ttl, Duration::from_secs(42));
            }
            other => panic!("expected Decided, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn status_403_fails_closed_as_unexpected_response() {
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
            failed(FailureReason::UnexpectedResponse)
        );
    }

    #[tokio::test]
    async fn server_error_is_retried_once_then_allows() {
        let (url, calls) = sequenced_server(vec![AxumStatus::BAD_GATEWAY, AxumStatus::OK]).await;
        let outcome = license_client(url).check(&license_request()).await;
        assert!(matches!(
            outcome,
            QuotaOutcome::Decided {
                decision: QuotaDecision::Allow,
                ..
            }
        ));
        assert_eq!(*calls.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn repeated_server_error_fails_after_one_retry() {
        let (url, calls) = sequenced_server(vec![AxumStatus::BAD_GATEWAY]).await;
        assert_eq!(
            license_client(url).check(&license_request()).await,
            failed(FailureReason::ServerError)
        );
        assert_eq!(*calls.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn open_circuit_skips_cdot() {
        let (url, calls) = sequenced_server(vec![AxumStatus::BAD_GATEWAY]).await;
        let breaker = CircuitBreaker::new(
            "customers_dot-test",
            CircuitConfig {
                failure_threshold: 1,
                window: Duration::from_secs(60),
                cooldown: Duration::from_secs(60),
            },
            Arc::new(circuit_breaker::NoopObserver),
        );
        let client = license_client(url).with_circuit_breaker(breaker);

        assert_eq!(
            client.check(&license_request()).await,
            failed(FailureReason::ServerError)
        );
        assert_eq!(
            client.check(&license_request()).await,
            failed(FailureReason::CircuitOpen)
        );
        assert_eq!(*calls.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn client_errors_are_not_retried() {
        for status in [
            AxumStatus::PAYMENT_REQUIRED,
            AxumStatus::UNAUTHORIZED,
            AxumStatus::FORBIDDEN,
        ] {
            let (url, calls) = sequenced_server(vec![status, AxumStatus::OK]).await;
            license_client(url).check(&license_request()).await;
            assert_eq!(*calls.lock().unwrap(), 1, "{status} was retried");
        }
    }

    #[tokio::test]
    async fn status_402_carries_block_reason() {
        let url = stub_server_with_body(
            AxumStatus::PAYMENT_REQUIRED,
            None,
            r#"{"block_reason":"license_revoked"}"#,
        )
        .await;
        let outcome = license_client(url).check(&license_request()).await;
        assert!(matches!(
            outcome,
            QuotaOutcome::Decided {
                decision: QuotaDecision::Deny(Some(ref reason)),
                ..
            } if reason == "license_revoked"
        ));
    }

    #[tokio::test]
    async fn status_403_fails_closed_with_block_reason() {
        let url = stub_server_with_body(
            AxumStatus::FORBIDDEN,
            None,
            r#"{"block_reason":"resolution_error"}"#,
        )
        .await;
        assert_eq!(
            license_client(url).check(&license_request()).await,
            QuotaOutcome::Failed {
                reason: FailureReason::UnexpectedResponse,
                block_reason: Some("resolution_error".into()),
            }
        );
    }

    #[tokio::test]
    async fn non_json_402_body_has_no_block_reason() {
        let url = stub_server_with_body(
            AxumStatus::PAYMENT_REQUIRED,
            None,
            "<html>Payment Required</html>",
        )
        .await;
        let outcome = license_client(url).check(&license_request()).await;
        assert!(matches!(
            outcome,
            QuotaOutcome::Decided {
                decision: QuotaDecision::Deny(None),
                ..
            }
        ));
    }

    #[tokio::test]
    async fn non_200_success_status_fails_closed() {
        let url = stub_server(AxumStatus::NO_CONTENT, None).await;
        assert_eq!(
            license_client(url).check(&license_request()).await,
            failed(FailureReason::UnexpectedResponse)
        );
    }

    #[tokio::test]
    async fn redirect_is_not_followed() {
        let (target, seen) = recording_server(AxumStatus::OK).await;
        let location: &'static str =
            Box::leak(format!("{target}{}", crate::constants::CDOT_QUOTA_PATH).into_boxed_str());
        let app = Router::new().route(
            crate::constants::CDOT_QUOTA_PATH,
            get(move || async move {
                (
                    AxumStatus::TEMPORARY_REDIRECT,
                    [(axum::http::header::LOCATION, location)],
                )
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        assert_eq!(
            license_client(format!("http://{addr}"))
                .check(&license_request())
                .await,
            failed(FailureReason::UnexpectedResponse)
        );
        assert!(seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn timeout_fails_closed_as_unreachable() {
        install_crypto();
        let app = Router::new().route(
            crate::constants::CDOT_QUOTA_PATH,
            get(|| async {
                tokio::time::sleep(Duration::from_secs(5)).await;
                AxumStatus::OK
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let client = QuotaClient::new(
            format!("http://{addr}"),
            admin_auth(),
            Duration::from_millis(200),
            Duration::from_secs(3600),
        )
        .unwrap();
        assert_eq!(
            client.check(&sample_request()).await,
            failed(FailureReason::Unreachable)
        );
    }

    #[tokio::test]
    async fn stalled_402_body_still_denies() {
        use tokio::io::AsyncWriteExt;

        install_crypto();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 4096];
            let _ = tokio::io::AsyncReadExt::read(&mut socket, &mut request).await;
            socket
                .write_all(b"HTTP/1.1 402 Payment Required\r\ncontent-length: 100\r\n\r\n")
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let client = QuotaClient::new(
            format!("http://{addr}"),
            admin_auth(),
            Duration::from_millis(300),
            Duration::from_secs(42),
        )
        .unwrap();
        assert_eq!(
            client.check(&sample_request()).await,
            QuotaOutcome::Decided {
                decision: QuotaDecision::Deny(None),
                ttl: Duration::from_secs(42),
            }
        );
    }

    #[tokio::test]
    async fn connection_error_fails_closed_as_unreachable() {
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
            failed(FailureReason::Unreachable)
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
    async fn license_mode_with_unencodable_checksum_fails_closed_without_calling_cdot() {
        let (url, seen) = recording_server(AxumStatus::OK).await;
        let mut request = license_request();
        request.license_checksum = Some("bad\nvalue".into());

        assert_eq!(
            license_client(url).check(&request).await,
            failed(FailureReason::Unauthorized)
        );
        assert!(seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn license_mode_401_fails_closed_as_unauthorized() {
        let (url, _) = recording_server(AxumStatus::UNAUTHORIZED).await;
        assert_eq!(
            license_client(url).check(&license_request()).await,
            failed(FailureReason::Unauthorized)
        );
    }

    #[tokio::test]
    async fn admin_mode_401_fails_closed_as_unauthorized() {
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
            failed(FailureReason::Unauthorized)
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
