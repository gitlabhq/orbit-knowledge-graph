mod cache;
mod client;
pub mod inputs;
mod key;

use std::sync::Arc;
use std::time::Duration;

use gkg_server_config::BillingConfig;
use tonic::Status;
use tracing::{info, warn};

use crate::constants::{METERED_SOURCE_TYPES, QUOTA_MAX_CACHE_ENTRIES};
use cache::QuotaCache;
use client::{QuotaClient, QuotaDecision};
pub use inputs::QuotaCheckInputs;
use key::CdotRequest;

pub struct QuotaService {
    inner: Option<QuotaServiceInner>,
}

struct QuotaServiceInner {
    cache: QuotaCache,
}

impl QuotaService {
    pub fn from_config(billing: &BillingConfig) -> Result<Self, reqwest::Error> {
        if !billing.quota.enabled {
            return Ok(Self { inner: None });
        }

        let cfg = &billing.quota;

        if cfg.api_user.is_none() || cfg.api_token.is_none() {
            warn!(
                "quota.enabled=true but api_user or api_token is not set; \
                 disabling quota gate to avoid silent fail-open on 401"
            );
            return Ok(Self { inner: None });
        }

        let client = QuotaClient::new(
            cfg.customers_dot_url.clone(),
            cfg.api_user.as_deref().unwrap_or(""),
            cfg.api_token.as_deref().unwrap_or(""),
            Duration::from_millis(cfg.request_timeout_ms),
            Duration::from_secs(cfg.fallback_cache_ttl_secs),
        )?;
        let cache = QuotaCache::new(Arc::new(client), QUOTA_MAX_CACHE_ENTRIES);

        Ok(Self {
            inner: Some(QuotaServiceInner { cache }),
        })
    }

    pub fn disabled() -> Self {
        Self { inner: None }
    }

    pub async fn check(&self, inputs: &QuotaCheckInputs) -> Result<(), Status> {
        let Some(inner) = &self.inner else {
            return Ok(());
        };

        if !METERED_SOURCE_TYPES.contains(&inputs.source_type.as_str()) {
            return Ok(());
        }

        let Some(request) = CdotRequest::from_inputs(inputs) else {
            warn!(
                user_id = inputs.user_id,
                realm = inputs.realm.as_deref().unwrap_or(""),
                root_namespace_id = inputs.root_namespace_id.unwrap_or_default(),
                global_user_id = inputs.global_user_id.as_deref().unwrap_or(""),
                instance_id = inputs.instance_id.as_deref().unwrap_or(""),
                unique_instance_id = inputs.unique_instance_id.as_deref().unwrap_or(""),
                source_type = %inputs.source_type,
                "quota check failed: required claim fields missing"
            );
            return Err(Status::internal(
                "quota check failed: required fields missing from token claims",
            ));
        };

        match inner.cache.check(request).await {
            QuotaDecision::Allow => Ok(()),
            QuotaDecision::Deny(reason) => {
                info!(
                    user_id = inputs.user_id,
                    realm = inputs.realm.as_deref().unwrap_or(""),
                    root_namespace_id = inputs.root_namespace_id.unwrap_or_default(),
                    global_user_id = inputs.global_user_id.as_deref().unwrap_or(""),
                    instance_id = inputs.instance_id.as_deref().unwrap_or(""),
                    unique_instance_id = inputs.unique_instance_id.as_deref().unwrap_or(""),
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

    fn inputs_with_source(source_type: &str) -> QuotaCheckInputs {
        QuotaCheckInputs {
            source_type: source_type.into(),
            user_id: 1,
            realm: Some("SaaS".into()),
            global_user_id: Some("g".into()),
            root_namespace_id: Some(1),
            instance_id: None,
            unique_instance_id: Some("u".into()),
        }
    }

    async fn counting_server(status: AxumStatus) -> (String, Arc<AtomicUsize>) {
        install_crypto();
        let counter = Arc::new(AtomicUsize::new(0));
        let c = counter.clone();
        let app = Router::new().route(
            crate::constants::CDOT_QUOTA_PATH,
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
                api_user: Some("test@example.com".into()),
                api_token: Some("test-token".into()),
                request_timeout_ms: 5_000,
                fallback_cache_ttl_secs: 3_600,
            },
        }
    }

    fn service_for(url: String) -> QuotaService {
        QuotaService::from_config(&enabled_billing(url)).unwrap()
    }

    #[tokio::test]
    async fn disabled_service_allows_any_inputs() {
        let svc = QuotaService::disabled();
        assert!(svc.check(&inputs_with_source("mcp")).await.is_ok());
    }

    #[tokio::test]
    async fn from_config_independent_of_billing_flag() {
        let (url, counter) = counting_server(AxumStatus::PAYMENT_REQUIRED).await;
        let cfg = BillingConfig {
            enabled: false,
            collector_url: String::new(),
            quota: QuotaConfig {
                enabled: true,
                customers_dot_url: url,
                api_user: Some("test@example.com".into()),
                api_token: Some("test-token".into()),
                request_timeout_ms: 5_000,
                fallback_cache_ttl_secs: 3_600,
            },
        };
        let svc = QuotaService::from_config(&cfg).unwrap();

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
        let svc = QuotaService::from_config(&cfg).unwrap();
        assert!(svc.check(&inputs_with_source("mcp")).await.is_ok());
    }

    #[tokio::test]
    async fn from_config_disabled_when_api_credentials_missing() {
        let (url, counter) = counting_server(AxumStatus::PAYMENT_REQUIRED).await;
        let cfg = BillingConfig {
            enabled: true,
            collector_url: String::new(),
            quota: QuotaConfig {
                enabled: true,
                customers_dot_url: url,
                api_user: None,
                api_token: None,
                request_timeout_ms: 5_000,
                fallback_cache_ttl_secs: 3_600,
            },
        };
        let svc = QuotaService::from_config(&cfg).unwrap();

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
    async fn errors_when_realm_missing() {
        let (url, counter) = counting_server(AxumStatus::PAYMENT_REQUIRED).await;
        let svc = service_for(url);
        let mut inputs = inputs_with_source("mcp");
        inputs.realm = None;

        let err = svc.check(&inputs).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn errors_when_saas_root_namespace_id_missing() {
        let (url, counter) = counting_server(AxumStatus::PAYMENT_REQUIRED).await;
        let svc = service_for(url);
        let mut inputs = inputs_with_source("mcp");
        inputs.root_namespace_id = None;

        let err = svc.check(&inputs).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn fails_open_on_upstream_5xx() {
        let (url, _counter) = counting_server(AxumStatus::INTERNAL_SERVER_ERROR).await;
        let svc = service_for(url);
        assert!(svc.check(&inputs_with_source("mcp")).await.is_ok());
    }
}
