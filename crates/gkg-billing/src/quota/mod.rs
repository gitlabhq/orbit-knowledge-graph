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
    /// Build a quota service from `BillingConfig`. The check fires only when
    /// **both** `billing.enabled` and `billing.quota.enabled` are true — the
    /// parent flag is the SOX-audited surface, the child is the operational
    /// kill-switch. Either flag off → returns a disabled service that always
    /// allows.
    pub fn from_config(billing: &BillingConfig, environment: &str) -> Result<Self, reqwest::Error> {
        if !(billing.enabled && billing.quota.enabled) {
            return Ok(Self { inner: None });
        }

        let cfg = &billing.quota;
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

    fn enabled_billing(customers_dot_url: String) -> BillingConfig {
        BillingConfig {
            enabled: true,
            collector_url: String::new(),
            quota: QuotaConfig {
                enabled: true,
                customers_dot_url,
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
    async fn from_config_disabled_when_billing_off() {
        // billing.quota.enabled = true but billing.enabled = false → still disabled.
        // This is the SOX gate: quota cannot fire without the parent billing flag.
        let cfg = BillingConfig {
            enabled: false,
            collector_url: String::new(),
            quota: QuotaConfig {
                enabled: true,
                customers_dot_url: "http://unused".into(),
                ..Default::default()
            },
        };
        let svc = QuotaService::from_config(&cfg, "production").unwrap();
        assert!(svc.check(&inputs_with_source("mcp")).await.is_ok());
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
        inputs.feature_qualified_name = None;

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
