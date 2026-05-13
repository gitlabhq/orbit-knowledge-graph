//! Pre-execution quota gate for metered Orbit traffic.
//!
//! Implements ADR 007 §1.5 Layer D. Issues `HEAD /api/v1/consumers/resolve` on
//! CustomersDot and caches the decision in memory (moka, ±10% jitter, per-key
//! single-flight). Only `mcp` and `rest` source types are metered; all other
//! source types and all upstream failures bypass the gate (fail-open).
//!
//! The auth → quota seam is `QuotaInputs` (see `inputs.rs`). `gkg-billing`
//! never sees `auth::Claims`; the single conversion lives in
//! `crates/gkg-server/src/billing_adapter.rs`.

mod cache;
mod client;
pub mod inputs;
mod key;

use std::sync::Arc;
use std::time::Duration;

use gkg_server_config::BillingConfig;
use tonic::Status;
use tracing::{debug, warn};

use cache::QuotaCache;
use client::{QuotaClient, QuotaDecision};
pub use inputs::QuotaInputs;
use key::CacheKey;

pub struct QuotaService {
    inner: Option<QuotaServiceInner>,
}

struct QuotaServiceInner {
    cache: QuotaCache,
    environment: String,
}

impl QuotaService {
    /// Build a quota service from `BillingConfig`. The check fires when
    /// `billing.quota.enabled` is true, independently of `billing.enabled` —
    /// billing event emission and quota enforcement are separate operational
    /// concerns (you may want one without the other during rollout).
    pub fn from_config(billing: &BillingConfig, environment: &str) -> Result<Self, reqwest::Error> {
        if !billing.quota.enabled {
            return Ok(Self { inner: None });
        }

        let cfg = &billing.quota;

        // Mirrors AIGW's `self.enabled = api_user is not None and api_token is not None`
        // guard (lib/usage_quota/client.py). Without admin credentials every
        // call to CDot returns 401, which falls through our match arm into
        // FailOpen and silently bypasses the gate. Return disabled so prod
        // can't accidentally ship a no-op quota check.
        if cfg.api_user.is_empty() || cfg.api_token.is_empty() {
            warn!(
                "quota.enabled=true but api_user or api_token is empty; \
                 disabling quota gate to avoid silent fail-open on 401"
            );
            return Ok(Self { inner: None });
        }

        let client = QuotaClient::new(
            cfg.customers_dot_url.clone(),
            &cfg.api_user,
            &cfg.api_token,
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

    pub fn disabled() -> Self {
        Self { inner: None }
    }

    pub async fn check(&self, inputs: &QuotaInputs) -> Result<(), Status> {
        let Some(inner) = &self.inner else {
            return Ok(());
        };

        // Only `mcp` and `rest` are metered channels per ADR §1.5. `frontend`,
        // `core`, and `dws` are included or zero-rated and must not hit CDot.
        if !matches!(inputs.source_type.as_str(), "mcp" | "rest") {
            return Ok(());
        }

        // Missing required cache-key fields → fail-open with a warning. Rails
        // MR !232123 populates these; rolling GKG ahead of Rails should not
        // break traffic.
        let Some(key) = CacheKey::from_inputs(inputs, &inner.environment) else {
            warn!(
                user_id = inputs.user_id,
                source_type = %inputs.source_type,
                "quota check skipped: required claim fields missing; failing open"
            );
            return Ok(());
        };

        match inner.cache.check(key).await {
            QuotaDecision::Allow => Ok(()),
            QuotaDecision::Deny(reason) => {
                debug!(
                    user_id = inputs.user_id,
                    source_type = %inputs.source_type,
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
    use gkg_server_config::QuotaConfig;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::net::TcpListener;

    fn install_crypto() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    }

    fn inputs_with_source(source_type: &str) -> QuotaInputs {
        QuotaInputs {
            source_type: source_type.into(),
            user_id: 1,
            realm: Some("SaaS".into()),
            global_user_id: Some("g".into()),
            root_namespace_id: Some(1),
            unique_instance_id: Some("u".into()),
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

    fn enabled_billing(customers_dot_url: String) -> BillingConfig {
        BillingConfig {
            enabled: true,
            collector_url: String::new(),
            quota: QuotaConfig {
                enabled: true,
                customers_dot_url,
                api_user: "test@example.com".into(),
                api_token: "test-token".into(),
                request_timeout_ms: 5_000,
                default_ttl_secs: 3600,
                max_cache_entries: 1024,
            },
        }
    }

    fn service_for(url: String) -> QuotaService {
        QuotaService::from_config(&enabled_billing(url), "production").unwrap()
    }

    #[tokio::test]
    async fn disabled_service_allows_any_inputs() {
        let svc = QuotaService::disabled();
        assert!(svc.check(&inputs_with_source("mcp")).await.is_ok());
    }

    #[tokio::test]
    async fn from_config_independent_of_billing_flag() {
        // Quota gate fires regardless of billing.enabled — emission and
        // enforcement are separate concerns. With billing.enabled=false and
        // billing.quota.enabled=true the gate must still fire (a 402 stub
        // here proves we actually reached the wire).
        let (url, counter) = counting_server(AxumStatus::PAYMENT_REQUIRED).await;
        let cfg = BillingConfig {
            enabled: false,
            collector_url: String::new(),
            quota: QuotaConfig {
                enabled: true,
                customers_dot_url: url,
                api_user: "test@example.com".into(),
                api_token: "test-token".into(),
                request_timeout_ms: 5_000,
                default_ttl_secs: 3600,
                max_cache_entries: 1024,
            },
        };
        let svc = QuotaService::from_config(&cfg, "production").unwrap();

        let err = svc.check(&inputs_with_source("mcp")).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn from_config_disabled_when_quota_off() {
        let cfg = BillingConfig {
            enabled: true,
            collector_url: String::new(),
            quota: QuotaConfig {
                enabled: false,
                ..Default::default()
            },
        };
        let svc = QuotaService::from_config(&cfg, "production").unwrap();
        assert!(svc.check(&inputs_with_source("mcp")).await.is_ok());
    }

    #[tokio::test]
    async fn from_config_disabled_when_api_credentials_missing() {
        // Mirrors AIGW: without admin creds we'd hit 401 in prod and silently
        // fail open. Force the service disabled so operators see "off" rather
        // than "always allow."
        let (url, counter) = counting_server(AxumStatus::PAYMENT_REQUIRED).await;
        let cfg = BillingConfig {
            enabled: true,
            collector_url: String::new(),
            quota: QuotaConfig {
                enabled: true,
                customers_dot_url: url,
                api_user: String::new(),
                api_token: String::new(),
                request_timeout_ms: 5_000,
                default_ttl_secs: 3600,
                max_cache_entries: 1024,
            },
        };
        let svc = QuotaService::from_config(&cfg, "production").unwrap();

        // Even though the stub would return 402, the service is disabled and
        // never touches the wire.
        assert!(svc.check(&inputs_with_source("mcp")).await.is_ok());
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn skips_quota_check_for_non_metered_sources() {
        let (url, counter) = counting_server(AxumStatus::PAYMENT_REQUIRED).await;
        let svc = service_for(url);

        assert!(svc.check(&inputs_with_source("frontend")).await.is_ok());
        assert!(svc.check(&inputs_with_source("core")).await.is_ok());
        assert!(svc.check(&inputs_with_source("dws")).await.is_ok());

        // Despite the stub returning 402, non-metered channels never hit it.
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn mcp_denies_on_payment_required() {
        let (url, _counter) = counting_server(AxumStatus::PAYMENT_REQUIRED).await;
        let svc = service_for(url);
        let err = svc.check(&inputs_with_source("mcp")).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    }

    #[tokio::test]
    async fn rest_denies_on_payment_required() {
        let (url, _counter) = counting_server(AxumStatus::PAYMENT_REQUIRED).await;
        let svc = service_for(url);
        let err = svc.check(&inputs_with_source("rest")).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    }

    #[tokio::test]
    async fn mcp_allowed_on_ok() {
        let (url, _counter) = counting_server(AxumStatus::OK).await;
        let svc = service_for(url);
        assert!(svc.check(&inputs_with_source("mcp")).await.is_ok());
    }

    #[tokio::test]
    async fn fails_open_when_required_claims_missing() {
        let (url, counter) = counting_server(AxumStatus::PAYMENT_REQUIRED).await;
        let svc = service_for(url);
        let mut inputs = inputs_with_source("mcp");
        // feature_enablement_type is the only remaining JWT-derived required
        // cache-key field that can legitimately be absent (realm too, but it
        // gates other behavior). Missing → uncached fail-open.
        inputs.feature_enablement_type = None;

        assert!(svc.check(&inputs).await.is_ok());
        // Should never have hit CustomersDot since we bailed before cache lookup.
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn fails_open_on_upstream_5xx() {
        let (url, _counter) = counting_server(AxumStatus::INTERNAL_SERVER_ERROR).await;
        let svc = service_for(url);
        // 5xx from upstream must not block the request.
        assert!(svc.check(&inputs_with_source("mcp")).await.is_ok());
    }
}
