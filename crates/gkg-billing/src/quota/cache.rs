use std::sync::Arc;
use std::time::{Duration, Instant};

use moka::future::Cache;
use opentelemetry::metrics::ObservableGauge;

use super::client::{DenyReason, QuotaClient, QuotaDecision, QuotaOutcome};
use super::key::{CacheKey, CdotRequest};

// Cached decision plus the instant at which it should be treated as expired.
// moka's `expire_after` policy queries `CachedDecision::expires_at` so each
// entry carries its own TTL (jittered at insert time).
#[derive(Clone)]
struct CachedDecision {
    decision: QuotaDecision,
    expires_at: Instant,
}

// ±10% jitter staggers expiry across instances that all populate their caches
// from the same CustomersDot response. Without it, a fleet-wide cache miss happens
// in lockstep whenever CustomersDot hands out identical max-age values.
fn jittered(ttl: Duration) -> Duration {
    let nanos = ttl.as_nanos() as f64;
    let factor = 0.9_f64 + rand::random::<f64>() * 0.2_f64;
    Duration::from_nanos((nanos * factor).max(0.0) as u64)
}

/// Decision surfaced to `QuotaService`. Distinct from the internal
/// `QuotaDecision` so that fail-open is visible as its own label value in
/// the metrics rather than being collapsed into `Allow`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum QuotaGateDecision {
    Allow,
    Deny(DenyReason),
    FailOpen,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CacheOutcome {
    Hit,
    Miss,
}

pub(crate) struct QuotaCache {
    inner: Cache<CacheKey, CachedDecision>,
    client: Arc<QuotaClient>,
    // Kept alive for the lifetime of the cache so the OTel SDK keeps invoking
    // the registered callback. Dropping the handle deregisters the gauge.
    _entries_gauge: ObservableGauge<i64>,
}

impl QuotaCache {
    pub(crate) fn new(client: Arc<QuotaClient>, max_entries: u64) -> Self {
        // Per-entry TTL: moka calls the expire_after closure on each access, and
        // we use the stored `expires_at` rather than a uniform cache-wide TTL so
        // each key can carry the TTL CDot returned for it.
        let inner: Cache<CacheKey, CachedDecision> = Cache::builder()
            .max_capacity(max_entries)
            .expire_after(ExpireByInstant)
            .build();

        // The OTel SDK owns the callback and invokes it independently of `QuotaCache`.
        // Cloning is cheap (moka Cache is Arc-backed) and gives the closure its own
        // owned handle so the SDK can hold it as 'static without borrowing from self.
        let cache_for_gauge = inner.clone();
        let meter = gkg_observability::meter();
        let entries_gauge = gkg_observability::billing::quota::QUOTA_CACHE_ENTRIES
            .build_observable_gauge_i64(&meter, move |observer| {
                observer.observe(cache_for_gauge.entry_count() as i64, &[]);
            });

        Self {
            inner,
            client,
            _entries_gauge: entries_gauge,
        }
    }

    /// Returns the gate decision and whether it was served from cache.
    ///
    /// `FailOpen` means CDot was unreachable or returned an unexpected status.
    /// The caller should still allow the request through but record the
    /// outcome separately from a genuine `Allow`.
    pub(crate) async fn check(&self, request: CdotRequest) -> (QuotaGateDecision, CacheOutcome) {
        let key = request.key.clone();

        // Fast path: cache hit. Done as a separate `get` so we can label the
        // metric on the service side; `try_get_with` below masks hit-vs-miss.
        if let Some(cached) = self.inner.get(&key).await {
            return (gate_from_decision(cached.decision), CacheOutcome::Hit);
        }

        // Cache miss: fetch from CDot with moka coalescing for concurrent callers.
        let client = self.client.clone();
        let entry = self
            .inner
            .try_get_with(key, async move {
                let start = Instant::now();
                match client.check(&request).await {
                    QuotaOutcome::Decided { decision, ttl } => {
                        record_cdot_duration(
                            start.elapsed().as_secs_f64(),
                            outcome_label(&decision),
                        );
                        Ok(CachedDecision {
                            decision,
                            expires_at: Instant::now() + jittered(ttl),
                        })
                    }
                    QuotaOutcome::FailOpen => {
                        record_cdot_duration(
                            start.elapsed().as_secs_f64(),
                            gkg_observability::billing::quota::values::FAIL_OPEN,
                        );
                        Err(FailOpen)
                    }
                }
            })
            .await;

        let gate = match entry {
            Ok(cached) => gate_from_decision(cached.decision),
            Err(_) => QuotaGateDecision::FailOpen,
        };
        (gate, CacheOutcome::Miss)
    }
}

fn gate_from_decision(decision: QuotaDecision) -> QuotaGateDecision {
    match decision {
        QuotaDecision::Allow => QuotaGateDecision::Allow,
        QuotaDecision::Deny(reason) => QuotaGateDecision::Deny(reason),
    }
}

fn outcome_label(decision: &QuotaDecision) -> &'static str {
    use gkg_observability::billing::quota::values::{ALLOW, DENY};
    match decision {
        QuotaDecision::Allow => ALLOW,
        QuotaDecision::Deny(_) => DENY,
    }
}

fn record_cdot_duration(secs: f64, outcome: &'static str) {
    use gkg_observability::billing::quota::labels::OUTCOME;
    super::metrics::QUOTA_METRICS
        .cdot_duration
        .record(secs, &[opentelemetry::KeyValue::new(OUTCOME, outcome)]);
}

#[derive(Debug)]
struct FailOpen;

impl std::fmt::Display for FailOpen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("fail-open")
    }
}

impl std::error::Error for FailOpen {}

struct ExpireByInstant;

impl moka::Expiry<CacheKey, CachedDecision> for ExpireByInstant {
    fn expire_after_create(
        &self,
        _key: &CacheKey,
        value: &CachedDecision,
        current_time: Instant,
    ) -> Option<Duration> {
        Some(value.expires_at.saturating_duration_since(current_time))
    }

    fn expire_after_read(
        &self,
        _key: &CacheKey,
        value: &CachedDecision,
        current_time: Instant,
        _current_duration: Option<Duration>,
        _last_modified_at: Instant,
    ) -> Option<Duration> {
        Some(value.expires_at.saturating_duration_since(current_time))
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

    fn request_with(user: &str) -> CdotRequest {
        CdotRequest {
            key: CacheKey {
                realm: "SaaS".into(),
                user_id: user.into(),
                root_namespace_id: String::new(),
                instance_id: String::new(),
                unique_instance_id: String::new(),
                event_type: "orbit_workflow_completion".into(),
                feature_qualified_name: "orbit-mcp".into(),
            },
            global_user_id: String::new(),
        }
    }

    async fn counting_status_server(status: AxumStatus) -> (String, Arc<AtomicUsize>) {
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

    #[test]
    fn jitter_stays_within_plus_minus_10_percent() {
        let base = Duration::from_secs(600);
        let low = base.mul_f64(0.9);
        let high = base.mul_f64(1.1);
        for _ in 0..200 {
            let j = jittered(base);
            assert!(
                j >= low && j <= high,
                "jittered TTL {:?} outside ±10% of {:?}",
                j,
                base
            );
        }
    }

    #[test]
    fn jitter_never_zero_for_positive_input() {
        for _ in 0..100 {
            assert!(jittered(Duration::from_secs(1)) > Duration::from_millis(0));
        }
    }

    #[tokio::test]
    async fn caches_allow_decisions() {
        let (url, counter) = counting_status_server(AxumStatus::OK).await;
        let client = Arc::new(
            QuotaClient::new(
                url,
                "test@example.com",
                "test-token",
                Duration::from_secs(5),
                Duration::from_secs(3600),
            )
            .unwrap(),
        );
        let cache = QuotaCache::new(client, 1024);

        for _ in 0..3 {
            assert_eq!(
                cache.check(request_with("1")).await.0,
                QuotaGateDecision::Allow
            );
        }

        // First call hits the server; the next two are cache hits.
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn caches_deny_decisions() {
        // A Deny outcome carries a TTL just like Allow and must be cached so
        // we don't re-hit CDot for every denied request. Three sequential
        // denies for the same key must produce exactly one upstream call.
        let (url, counter) = counting_status_server(AxumStatus::PAYMENT_REQUIRED).await;
        let client = Arc::new(
            QuotaClient::new(
                url,
                "test@example.com",
                "test-token",
                Duration::from_secs(5),
                Duration::from_secs(3600),
            )
            .unwrap(),
        );
        let cache = QuotaCache::new(client, 1024);

        for _ in 0..3 {
            assert_eq!(
                cache.check(request_with("denied")).await.0,
                QuotaGateDecision::Deny(DenyReason::QuotaExhausted)
            );
        }

        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn coalesces_concurrent_misses_for_same_key() {
        let (url, counter) = counting_status_server(AxumStatus::OK).await;
        let client = Arc::new(
            QuotaClient::new(
                url,
                "test@example.com",
                "test-token",
                Duration::from_secs(5),
                Duration::from_secs(3600),
            )
            .unwrap(),
        );
        let cache = Arc::new(QuotaCache::new(client, 1024));

        let mut handles = Vec::new();
        for _ in 0..20 {
            let c = cache.clone();
            handles.push(tokio::spawn(async move {
                c.check(request_with("concurrent")).await
            }));
        }
        for h in handles {
            assert_eq!(h.await.unwrap().0, QuotaGateDecision::Allow);
        }

        // 20 concurrent callers, single upstream request due to get_with coalescing.
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }
}
