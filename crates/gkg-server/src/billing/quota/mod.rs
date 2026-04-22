mod cache;
mod client;
mod key;

use std::sync::Arc;
use std::time::Duration;

use gkg_server_config::QuotaConfig;
use tonic::Status;
use tracing::{debug, warn};

use crate::auth::Claims;

use cache::QuotaCache;
pub(crate) use client::{QuotaClient, QuotaDecision};
pub(crate) use key::CacheKey;

pub struct QuotaService {
    inner: Option<QuotaServiceInner>,
}

struct QuotaServiceInner {
    cache: QuotaCache,
    environment: String,
}

impl QuotaService {
    pub fn from_config(cfg: &QuotaConfig, environment: &str) -> Result<Self, reqwest::Error> {
        if !cfg.enabled {
            return Ok(Self { inner: None });
        }

        let client = QuotaClient::new(
            cfg.customers_dot_url.clone(),
            Duration::from_millis(cfg.request_timeout_ms),
            Duration::from_secs(cfg.default_ttl_secs),
        )?;
        let cache = QuotaCache::new(Arc::new(client), cfg.max_cache_entries);

        Ok(Self {
            inner: Some(QuotaServiceInner {
                cache,
                environment: environment.to_string(),
            }),
        })
    }

    #[cfg(test)]
    pub(crate) fn from_parts(cache: QuotaCache, environment: String) -> Self {
        Self {
            inner: Some(QuotaServiceInner { cache, environment }),
        }
    }

    pub fn disabled() -> Self {
        Self { inner: None }
    }

    pub async fn check(&self, claims: &Claims) -> Result<(), Status> {
        let Some(inner) = &self.inner else {
            return Ok(());
        };

        // Only `mcp` and `rest` are metered channels per ADR §1.5. `frontend`,
        // `core`, and `dws` are included or zero-rated and must not hit CDot.
        if !matches!(claims.source_type.as_str(), "mcp" | "rest") {
            return Ok(());
        }

        // Missing claim fields → fail-open with a warning. Rails MR !232123
        // populates these; rolling GKG ahead of Rails should not break traffic.
        let Some(key) = CacheKey::from_claims(claims, &inner.environment) else {
            warn!(
                user_id = claims.user_id,
                source_type = %claims.source_type,
                "quota check skipped: required claim fields missing; failing open"
            );
            return Ok(());
        };

        match inner.cache.check(key).await {
            QuotaDecision::Allow => Ok(()),
            QuotaDecision::Deny(reason) => {
                debug!(
                    user_id = claims.user_id,
                    source_type = %claims.source_type,
                    reason = ?reason,
                    "quota check denied request"
                );
                Err(Status::resource_exhausted(reason.message()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::http::StatusCode as AxumStatus;
    use axum::routing::head;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::net::TcpListener;

    fn install_crypto() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    }

    fn claims_with_source(source_type: &str) -> Claims {
        Claims {
            sub: "u".into(),
            iss: "gitlab".into(),
            aud: "gitlab-knowledge-graph".into(),
            iat: 0,
            exp: i64::MAX,
            user_id: 1,
            username: "t".into(),
            admin: false,
            organization_id: None,
            min_access_level: None,
            group_traversal_ids: vec![],
            source_type: source_type.into(),
            ai_session_id: None,
            instance_id: None,
            unique_instance_id: Some("u".into()),
            instance_version: None,
            global_user_id: Some("g".into()),
            host_name: None,
            root_namespace_id: Some(1),
            deployment_type: None,
            realm: Some("SaaS".into()),
            feature_qualified_name: Some("orbit_query".into()),
            feature_enablement_type: Some("duo_enterprise".into()),
        }
    }

    async fn counting_server(status: AxumStatus) -> (String, Arc<AtomicUsize>) {
        install_crypto();
        let counter = Arc::new(AtomicUsize::new(0));
        let c = counter.clone();
        let app = Router::new().route(
            "/api/v1/consumers/resolve",
            head(move || {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    status
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), counter)
    }

    fn service_for(url: String) -> QuotaService {
        let client =
            QuotaClient::new(url, Duration::from_secs(5), Duration::from_secs(3600)).unwrap();
        let cache = cache::QuotaCache::new(Arc::new(client), 1024);
        QuotaService::from_parts(cache, "production".into())
    }

    #[tokio::test]
    async fn disabled_service_allows_any_claims() {
        let svc = QuotaService::disabled();
        assert!(svc.check(&claims_with_source("mcp")).await.is_ok());
    }

    #[tokio::test]
    async fn skips_quota_check_for_frontend() {
        let (url, counter) = counting_server(AxumStatus::PAYMENT_REQUIRED).await;
        let svc = service_for(url);

        assert!(svc.check(&claims_with_source("frontend")).await.is_ok());
        assert!(svc.check(&claims_with_source("core")).await.is_ok());
        assert!(svc.check(&claims_with_source("dws")).await.is_ok());

        // Despite the stub returning 402, non-metered channels never hit it.
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn mcp_denies_on_payment_required() {
        let (url, _counter) = counting_server(AxumStatus::PAYMENT_REQUIRED).await;
        let svc = service_for(url);
        let err = svc.check(&claims_with_source("mcp")).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    }

    #[tokio::test]
    async fn rest_denies_on_payment_required() {
        let (url, _counter) = counting_server(AxumStatus::PAYMENT_REQUIRED).await;
        let svc = service_for(url);
        let err = svc.check(&claims_with_source("rest")).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    }

    #[tokio::test]
    async fn mcp_allowed_on_ok() {
        let (url, _counter) = counting_server(AxumStatus::OK).await;
        let svc = service_for(url);
        assert!(svc.check(&claims_with_source("mcp")).await.is_ok());
    }

    #[tokio::test]
    async fn fails_open_when_required_claims_missing() {
        let (url, counter) = counting_server(AxumStatus::PAYMENT_REQUIRED).await;
        let svc = service_for(url);
        let mut claims = claims_with_source("mcp");
        claims.feature_qualified_name = None;

        assert!(svc.check(&claims).await.is_ok());
        // Should never have hit CustomersDot since we bailed before cache lookup.
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn fails_open_on_upstream_5xx() {
        let (url, _counter) = counting_server(AxumStatus::INTERNAL_SERVER_ERROR).await;
        let svc = service_for(url);
        // 5xx from upstream must not block the request.
        assert!(svc.check(&claims_with_source("mcp")).await.is_ok());
    }
}
