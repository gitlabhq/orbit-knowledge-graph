use std::sync::Arc;
use std::time::{Duration, Instant};

use moka::future::Cache;

use super::client::{QuotaClient, QuotaDecision, QuotaOutcome};
use super::key::CacheKey;

// Cached decision plus the instant at which it should be treated as expired.
// moka's `expire_after` policy queries `CachedDecision::expires_at` so each
// entry carries its own TTL (jittered at insert time).
#[derive(Clone)]
struct CachedDecision {
    decision: QuotaDecision,
    expires_at: Instant,
}

// ±10% jitter staggers expiry across instances that all populate their caches
// from the same upstream response. Without it, a fleet-wide cache miss happens
// in lockstep whenever CustomersDot hands out identical max-age values.
fn jittered(ttl: Duration) -> Duration {
    let nanos = ttl.as_nanos() as f64;
    let factor = 0.9_f64 + rand::random::<f64>() * 0.2_f64;
    Duration::from_nanos((nanos * factor).max(0.0) as u64)
}

pub(crate) struct QuotaCache {
    inner: Cache<CacheKey, CachedDecision>,
    client: Arc<QuotaClient>,
}

impl QuotaCache {
    pub(crate) fn new(client: Arc<QuotaClient>, max_entries: u64) -> Self {
        // Per-entry TTL: moka calls the expire_after closure on each access, and
        // we use the stored `expires_at` rather than a uniform cache-wide TTL so
        // each key can carry the TTL CDot returned for it.
        let inner = Cache::builder()
            .max_capacity(max_entries)
            .expire_after(ExpireByInstant)
            .build();
        Self { inner, client }
    }

    // Returns the cached decision or fetches one on miss. On fetch failure
    // (FailOpen), does not cache and propagates a plain Allow — callers treat
    // fail-open as identical to allow at the call site.
    pub(crate) async fn check(&self, key: CacheKey) -> QuotaDecision {
        // moka's `get_with` coalesces concurrent callers that miss on the same
        // key: exactly one closure runs, the rest await the result. This is the
        // single-flight requirement from the issue.
        //
        // We wrap the outcome in `Option` so FailOpen doesn't populate the cache
        // at all — we return None, then short-circuit to Allow below.
        let client = self.client.clone();
        let key_for_fetch = key.clone();
        let entry = self
            .inner
            .try_get_with(key, async move {
                match client.check(&key_for_fetch).await {
                    QuotaOutcome::Decided { decision, ttl } => Ok(CachedDecision {
                        decision,
                        expires_at: Instant::now() + jittered(ttl),
                    }),
                    QuotaOutcome::FailOpen => Err(FailOpen),
                }
            })
            .await;

        match entry {
            Ok(cached) => cached.decision,
            Err(_) => QuotaDecision::Allow,
        }
    }
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

    fn key_with(user: &str) -> CacheKey {
        CacheKey {
            environment: "production".into(),
            realm: "SaaS".into(),
            user_id: user.into(),
            global_user_id: String::new(),
            root_namespace_id: String::new(),
            unique_instance_id: String::new(),
            feature_enablement_type: "duo_enterprise".into(),
            feature_qualified_name: "orbit_query".into(),
        }
    }

    // Counting stub returning 200. Each request increments the shared counter
    // so we can assert how many times the client actually hit the network.
    async fn counting_allow_server() -> (String, Arc<AtomicUsize>) {
        install_crypto();
        let counter = Arc::new(AtomicUsize::new(0));
        let c = counter.clone();
        let app = Router::new().route(
            "/api/v1/consumers/resolve",
            head(move || {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    AxumStatus::OK
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
        let (url, counter) = counting_allow_server().await;
        let client = Arc::new(
            QuotaClient::new(url, Duration::from_secs(5), Duration::from_secs(3600)).unwrap(),
        );
        let cache = QuotaCache::new(client, 1024);

        let key = key_with("1");
        for _ in 0..3 {
            assert_eq!(cache.check(key.clone()).await, QuotaDecision::Allow);
        }

        // First call hits the server; the next two are cache hits.
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn coalesces_concurrent_misses_for_same_key() {
        let (url, counter) = counting_allow_server().await;
        let client = Arc::new(
            QuotaClient::new(url, Duration::from_secs(5), Duration::from_secs(3600)).unwrap(),
        );
        let cache = Arc::new(QuotaCache::new(client, 1024));

        let mut handles = Vec::new();
        for _ in 0..20 {
            let c = cache.clone();
            handles.push(tokio::spawn(async move {
                c.check(key_with("concurrent")).await
            }));
        }
        for h in handles {
            assert_eq!(h.await.unwrap(), QuotaDecision::Allow);
        }

        // 20 concurrent callers, single upstream request due to get_with coalescing.
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }
}
