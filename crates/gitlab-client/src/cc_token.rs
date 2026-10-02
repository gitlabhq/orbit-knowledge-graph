use std::future::Future;
use std::pin::Pin;
use std::sync::RwLock;
use std::time::Duration;

use chrono::Utc;
use rand::RngExt;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::client::GitlabClient;
use crate::error::GitlabClientError;
use crate::types::CloudConnectorToken;

const REFRESH_JITTER_MAX_SECS: i64 = 30;

/// How long before `exp` to refresh, so a brief Rails outage near expiry
/// doesn't drop events.
const CC_TOKEN_REFRESH_LEAD_SECS: i64 = 30 * 60;

/// Minimum gap between Rails calls after a failure, so an outage draws at
/// most one call per pod per interval.
const CC_TOKEN_RETRY_INTERVAL_SECS: i64 = 60;

/// Caps the fetch while the cached token is still valid, so it can cover for
/// a timeout (matches labkit's own OIDC token source). With no token, or only
/// an expired one the collector would reject, a slow Rails is worth waiting
/// out instead of giving up early.
const CC_TOKEN_FETCH_TIMEOUT: Duration = Duration::from_secs(15);

pub trait CloudConnectorTokenFetcher: Send + Sync {
    fn fetch(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<CloudConnectorToken, GitlabClientError>> + Send + '_>>;
}

impl CloudConnectorTokenFetcher for GitlabClient {
    fn fetch(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<CloudConnectorToken, GitlabClientError>> + Send + '_>>
    {
        Box::pin(self.cloud_connector_token())
    }
}

struct State {
    token: Option<String>,
    token_exp: i64,
    refresh_at: i64,
}

pub struct CloudConnectorTokenCache {
    fetcher: std::sync::Arc<dyn CloudConnectorTokenFetcher>,
    state: RwLock<State>,
    refresh_lock: Mutex<()>,
}

impl CloudConnectorTokenCache {
    pub fn new(fetcher: std::sync::Arc<dyn CloudConnectorTokenFetcher>) -> Self {
        Self {
            fetcher,
            state: RwLock::new(State {
                token: None,
                token_exp: 0,
                refresh_at: 0,
            }),
            refresh_lock: Mutex::new(()),
        }
    }

    /// Serves the cached token even past its `exp` on a failed refresh, so
    /// the collector's 401 (not a silent local drop) is what surfaces it.
    pub async fn token(&self) -> Result<String, GitlabClientError> {
        if let Some(token) = self.cached_token_before_refresh_at(Utc::now().timestamp()) {
            return Ok(token);
        }

        let _guard = self.refresh_lock.lock().await;
        if let Some(token) = self.cached_token_before_refresh_at(Utc::now().timestamp()) {
            return Ok(token);
        }

        let has_valid_cached_token = {
            let state = self.state.read().unwrap();
            state.token.is_some() && Utc::now().timestamp() < state.token_exp
        };
        let fetched = if has_valid_cached_token {
            tokio::time::timeout(CC_TOKEN_FETCH_TIMEOUT, self.fetcher.fetch())
                .await
                .unwrap_or_else(|_| {
                    Err(GitlabClientError::Unexpected(format!(
                        "cloud connector token fetch timed out after {CC_TOKEN_FETCH_TIMEOUT:?}"
                    )))
                })
        } else {
            self.fetcher.fetch().await
        };

        let now = Utc::now().timestamp();
        let mut state = self.state.write().unwrap();
        match fetched {
            Ok(fetched) => {
                state.refresh_at = compute_refresh_at(fetched.exp, now, jitter_secs());
                info!(
                    exp = fetched.exp,
                    refresh_at = state.refresh_at,
                    "cloud connector token refreshed"
                );
                state.token = Some(fetched.token.clone());
                state.token_exp = fetched.exp;
                Ok(fetched.token)
            }
            Err(error) => {
                state.refresh_at = now + CC_TOKEN_RETRY_INTERVAL_SECS;
                warn!(
                    %error,
                    retry_at = state.refresh_at,
                    serving_cached_token = state.token.is_some(),
                    cached_token_expired = state.token.is_some() && now >= state.token_exp,
                    "cloud connector token refresh failed"
                );
                state.token.clone().ok_or(error)
            }
        }
    }

    /// `None` means "attempt a fetch": either `refresh_at` has passed, or
    /// there is no token yet to rate-limit around.
    fn cached_token_before_refresh_at(&self, now: i64) -> Option<String> {
        let state = self.state.read().unwrap();
        if now < state.refresh_at {
            state.token.clone()
        } else {
            None
        }
    }
}

fn compute_refresh_at(exp: i64, now: i64, jitter_secs: i64) -> i64 {
    let planned = exp - CC_TOKEN_REFRESH_LEAD_SECS - jitter_secs;
    if planned > now {
        planned
    } else {
        now + CC_TOKEN_RETRY_INTERVAL_SECS
    }
}

fn jitter_secs() -> i64 {
    rand::rng().random_range(0..=REFRESH_JITTER_MAX_SECS)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};

    use super::*;

    struct StubFetcher {
        calls: AtomicUsize,
        exp: AtomicI64,
        fail: AtomicBool,
    }

    impl StubFetcher {
        fn new(exp: i64) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                exp: AtomicI64::new(exp),
                fail: AtomicBool::new(false),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl CloudConnectorTokenFetcher for StubFetcher {
        fn fetch(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<CloudConnectorToken, GitlabClientError>> + Send + '_>>
        {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            let exp = self.exp.load(Ordering::SeqCst);
            let fail = self.fail.load(Ordering::SeqCst);
            Box::pin(async move {
                // Without a real suspension point, a concurrent burst resolves the
                // first future to completion before the executor polls the rest,
                // so single-flight dedup would never actually be exercised.
                tokio::task::yield_now().await;
                if fail {
                    return Err(GitlabClientError::Unexpected(
                        "cloud connector token request returned status 503".into(),
                    ));
                }
                Ok(CloudConnectorToken {
                    token: format!("token-{n}"),
                    exp,
                })
            })
        }
    }

    fn now() -> i64 {
        Utc::now().timestamp()
    }

    fn let_refresh_at_pass(cache: &CloudConnectorTokenCache) {
        cache.state.write().unwrap().refresh_at = 0;
    }

    fn refresh_at(cache: &CloudConnectorTokenCache) -> i64 {
        cache.state.read().unwrap().refresh_at
    }

    #[test]
    fn refresh_at_leads_exp_by_the_refresh_lead_plus_jitter() {
        let now = 1_000_000;
        let exp = now + 3 * 24 * 3_600;
        for jitter in [0, REFRESH_JITTER_MAX_SECS] {
            assert_eq!(
                compute_refresh_at(exp, now, jitter),
                exp - CC_TOKEN_REFRESH_LEAD_SECS - jitter
            );
        }
    }

    #[test]
    fn refresh_at_falls_back_to_the_retry_interval_for_a_near_expiry_token() {
        let now = 1_000_000;
        for exp in [now + CC_TOKEN_REFRESH_LEAD_SECS, now + 10, now - 3_600] {
            assert_eq!(
                compute_refresh_at(exp, now, 0),
                now + CC_TOKEN_RETRY_INTERVAL_SECS
            );
        }
    }

    #[test]
    fn jitter_stays_within_bounds() {
        for _ in 0..1_000 {
            let j = jitter_secs();
            assert!((0..=REFRESH_JITTER_MAX_SECS).contains(&j));
        }
    }

    #[tokio::test]
    async fn caches_token_until_refresh_at() {
        let fetcher = Arc::new(StubFetcher::new(now() + 3_600 * 24));
        let cache = CloudConnectorTokenCache::new(fetcher.clone());

        assert_eq!(cache.token().await.unwrap(), "token-0");
        assert_eq!(cache.token().await.unwrap(), "token-0");
        assert_eq!(fetcher.calls(), 1);
    }

    #[tokio::test]
    async fn refetches_once_refresh_at_passes() {
        let fetcher = Arc::new(StubFetcher::new(now() + 3_600 * 24));
        let cache = CloudConnectorTokenCache::new(fetcher.clone());

        assert_eq!(cache.token().await.unwrap(), "token-0");
        let_refresh_at_pass(&cache);

        assert_eq!(cache.token().await.unwrap(), "token-1");
        assert_eq!(fetcher.calls(), 2);
    }

    #[tokio::test]
    async fn serves_the_cached_token_while_refreshes_fail() {
        let fetcher = Arc::new(StubFetcher::new(now() + 3_600 * 24));
        let cache = CloudConnectorTokenCache::new(fetcher.clone());
        assert_eq!(cache.token().await.unwrap(), "token-0");
        fetcher.fail.store(true, Ordering::SeqCst);

        for _ in 0..3 {
            let_refresh_at_pass(&cache);
            assert_eq!(cache.token().await.unwrap(), "token-0");
        }
        assert_eq!(fetcher.calls(), 4);

        fetcher.fail.store(false, Ordering::SeqCst);
        let_refresh_at_pass(&cache);
        assert_eq!(cache.token().await.unwrap(), "token-4");
    }

    #[tokio::test]
    async fn serves_an_expired_cached_token_when_nothing_else_is_available() {
        let fetcher = Arc::new(StubFetcher::new(now() - 3_600));
        let cache = CloudConnectorTokenCache::new(fetcher.clone());
        assert_eq!(cache.token().await.unwrap(), "token-0");
        fetcher.fail.store(true, Ordering::SeqCst);
        let_refresh_at_pass(&cache);

        assert_eq!(cache.token().await.unwrap(), "token-0");
        assert_eq!(fetcher.calls(), 2);
    }

    #[tokio::test]
    async fn a_burst_after_a_failed_refresh_fetches_at_most_once_per_retry_interval() {
        let fetcher = Arc::new(StubFetcher::new(now() + 3_600 * 24));
        let cache = CloudConnectorTokenCache::new(fetcher.clone());
        assert_eq!(cache.token().await.unwrap(), "token-0");
        fetcher.fail.store(true, Ordering::SeqCst);
        let_refresh_at_pass(&cache);

        let before = now();
        let results = futures::future::join_all((0..50).map(|_| cache.token())).await;

        assert!(results.iter().all(|r| r.as_deref().ok() == Some("token-0")));
        assert_eq!(fetcher.calls(), 2);
        assert!(refresh_at(&cache) >= before + CC_TOKEN_RETRY_INTERVAL_SECS);
    }

    #[tokio::test]
    async fn every_call_attempts_a_fetch_until_a_token_is_obtained() {
        let fetcher = Arc::new(StubFetcher::new(now() + 3_600 * 24));
        fetcher.fail.store(true, Ordering::SeqCst);
        let cache = CloudConnectorTokenCache::new(fetcher.clone());

        let results = futures::future::join_all((0..50).map(|_| cache.token())).await;

        assert!(
            results
                .iter()
                .all(|r| matches!(r, Err(GitlabClientError::Unexpected(_))))
        );
        assert_eq!(fetcher.calls(), 50);
        assert!(cache.state.read().unwrap().token.is_none());
    }

    struct DelayedFetcher {
        delay: Duration,
        exp: i64,
    }

    impl CloudConnectorTokenFetcher for DelayedFetcher {
        fn fetch(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<CloudConnectorToken, GitlabClientError>> + Send + '_>>
        {
            let delay = self.delay;
            let exp = self.exp;
            Box::pin(async move {
                tokio::time::sleep(delay).await;
                Ok(CloudConnectorToken {
                    token: "delayed-token".into(),
                    exp,
                })
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_fetch_is_abandoned_at_the_timeout_when_a_cached_token_exists() {
        let fetcher = Arc::new(DelayedFetcher {
            delay: Duration::from_secs(3_600),
            exp: now() + 3_600,
        });
        let cache = CloudConnectorTokenCache::new(fetcher);
        {
            let mut state = cache.state.write().unwrap();
            state.token = Some("seed-token".to_string());
            state.token_exp = now() + 3_600;
            state.refresh_at = 0;
        }

        let started = tokio::time::Instant::now();
        let before = now();
        let result = cache.token().await;

        assert_eq!(result.unwrap(), "seed-token");
        assert_eq!(started.elapsed(), CC_TOKEN_FETCH_TIMEOUT);
        assert!(refresh_at(&cache) >= before + CC_TOKEN_RETRY_INTERVAL_SECS);
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_fetch_is_waited_out_when_the_cached_token_has_expired() {
        let delay = CC_TOKEN_FETCH_TIMEOUT + Duration::from_secs(5);
        let fetcher = Arc::new(DelayedFetcher {
            delay,
            exp: now() + 3_600,
        });
        let cache = CloudConnectorTokenCache::new(fetcher);
        {
            let mut state = cache.state.write().unwrap();
            state.token = Some("expired-token".to_string());
            state.token_exp = now() - 60;
            state.refresh_at = 0;
        }

        let started = tokio::time::Instant::now();
        let result = cache.token().await;

        assert_eq!(result.unwrap(), "delayed-token");
        assert_eq!(started.elapsed(), delay);
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_fetch_is_not_abandoned_when_no_cached_token_exists() {
        let delay = CC_TOKEN_FETCH_TIMEOUT + Duration::from_secs(5);
        let fetcher = Arc::new(DelayedFetcher {
            delay,
            exp: now() + 3_600,
        });
        let cache = CloudConnectorTokenCache::new(fetcher);

        let started = tokio::time::Instant::now();
        let result = cache.token().await;

        assert_eq!(result.unwrap(), "delayed-token");
        assert_eq!(started.elapsed(), delay);
    }

    async fn spawn_token_route(
        cc_token: String,
    ) -> (
        orbit_server_config::GitlabClientConfiguration,
        Arc<AtomicUsize>,
    ) {
        use axum::Router;
        use axum::routing::get;
        use base64::Engine;

        let hits = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&hits);
        let app = Router::new().route(
            "/api/v4/internal/orbit/cloud_connector_token",
            get(move || {
                counter.fetch_add(1, Ordering::SeqCst);
                let cc_token = cc_token.clone();
                async move { axum::Json(serde_json::json!({ "token": cc_token })) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let config = orbit_server_config::GitlabClientConfiguration {
            base_url: format!("http://{addr}"),
            signing_key: base64::engine::general_purpose::STANDARD
                .encode(b"test-secret-that-is-long-enough!"),
            resolve_host: None,
        };
        (config, hits)
    }

    fn jwt_with_exp(exp: i64) -> String {
        use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};

        encode(
            &Header::new(Algorithm::HS256),
            &serde_json::json!({ "exp": exp }),
            &EncodingKey::from_secret(b"any-secret"),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn caches_a_token_fetched_through_a_real_gitlab_client() {
        let cc_token = jwt_with_exp(now() + 3_600 * 24);
        let (config, hits) = spawn_token_route(cc_token.clone()).await;
        let cache = CloudConnectorTokenCache::new(Arc::new(GitlabClient::new(config).unwrap()));

        assert_eq!(cache.token().await.unwrap(), cc_token);
        assert_eq!(cache.token().await.unwrap(), cc_token);
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_near_expiry_token_from_rails_is_not_refetched_per_call() {
        let cc_token = jwt_with_exp(now() + 10);
        let (config, hits) = spawn_token_route(cc_token.clone()).await;
        let cache = CloudConnectorTokenCache::new(Arc::new(GitlabClient::new(config).unwrap()));

        for _ in 0..20 {
            assert_eq!(cache.token().await.unwrap(), cc_token);
        }
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }
}
