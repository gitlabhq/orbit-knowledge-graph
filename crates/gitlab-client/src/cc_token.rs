use std::future::Future;
use std::pin::Pin;
use std::sync::RwLock;

use chrono::Utc;
use rand::RngExt;
use tokio::sync::Mutex;
use tracing::info;

use crate::client::GitlabClient;
use crate::error::GitlabClientError;
use crate::types::CloudConnectorToken;

/// Random lead time subtracted from `expires_at` (itself already buffered
/// against the real `exp`, see `CC_TOKEN_EXPIRY_BUFFER_SECS` in `client.rs`),
/// so a fleet with identical `expires_at` values doesn't stampede the Rails
/// route at once.
const REFRESH_JITTER_MAX_SECS: i64 = 30;

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

struct Cached {
    token: String,
    refresh_at: i64,
}

pub struct CloudConnectorTokenCache {
    fetcher: std::sync::Arc<dyn CloudConnectorTokenFetcher>,
    cached: RwLock<Option<Cached>>,
    refresh_lock: Mutex<()>,
}

impl CloudConnectorTokenCache {
    pub fn new(fetcher: std::sync::Arc<dyn CloudConnectorTokenFetcher>) -> Self {
        Self {
            fetcher,
            cached: RwLock::new(None),
            refresh_lock: Mutex::new(()),
        }
    }

    pub async fn token(&self) -> Result<String, GitlabClientError> {
        if let Some(token) = self.fresh_token(Utc::now().timestamp()) {
            return Ok(token);
        }

        let _guard = self.refresh_lock.lock().await;
        if let Some(token) = self.fresh_token(Utc::now().timestamp()) {
            return Ok(token);
        }

        let fetched = self.fetcher.fetch().await?;
        let refresh_at = compute_refresh_at(fetched.expires_at, jitter_secs());
        info!(
            expires_at = fetched.expires_at,
            refresh_at, "cloud connector token refreshed"
        );
        let token = fetched.token.clone();
        *self.cached.write().unwrap() = Some(Cached {
            token: fetched.token,
            refresh_at,
        });
        Ok(token)
    }

    fn fresh_token(&self, now: i64) -> Option<String> {
        let guard = self.cached.read().unwrap();
        guard
            .as_ref()
            .filter(|c| now < c.refresh_at)
            .map(|c| c.token.clone())
    }
}

fn compute_refresh_at(expires_at: i64, jitter_secs: i64) -> i64 {
    expires_at - jitter_secs
}

fn jitter_secs() -> i64 {
    rand::rng().random_range(0..=REFRESH_JITTER_MAX_SECS)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

    use super::*;

    struct StubFetcher {
        calls: AtomicUsize,
        expires_at: AtomicI64,
    }

    impl StubFetcher {
        fn new(expires_at: i64) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                expires_at: AtomicI64::new(expires_at),
            }
        }
    }

    impl CloudConnectorTokenFetcher for StubFetcher {
        fn fetch(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<CloudConnectorToken, GitlabClientError>> + Send + '_>>
        {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            let expires_at = self.expires_at.load(Ordering::SeqCst);
            Box::pin(async move {
                Ok(CloudConnectorToken {
                    token: format!("token-{n}"),
                    expires_at,
                })
            })
        }
    }

    #[test]
    fn refresh_at_subtracts_jitter() {
        assert_eq!(compute_refresh_at(1_000, 0), 1_000);
        assert_eq!(compute_refresh_at(1_000, 30), 970);
    }

    #[test]
    fn the_expiry_buffer_and_jitter_compose_to_60_90_seconds_of_lead_time() {
        use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};

        let real_exp = Utc::now().timestamp() + 3_600;
        let key = EncodingKey::from_secret(b"any-secret");
        let token = encode(
            &Header::new(Algorithm::HS256),
            &serde_json::json!({ "exp": real_exp }),
            &key,
        )
        .unwrap();

        let expires_at = crate::client::decode_token_expiry(&token).unwrap();
        assert_eq!(expires_at, real_exp - 60);

        for jitter in [0, REFRESH_JITTER_MAX_SECS] {
            let refresh_at = compute_refresh_at(expires_at, jitter);
            let lead_time = real_exp - refresh_at;
            assert!(
                (60..=90).contains(&lead_time),
                "lead_time={lead_time} out of [60, 90] for jitter={jitter}"
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
    async fn caches_token_until_refresh_window() {
        let far_future = Utc::now().timestamp() + 3_600;
        let fetcher = Arc::new(StubFetcher::new(far_future));
        let cache = CloudConnectorTokenCache::new(fetcher.clone());

        let first = cache.token().await.unwrap();
        let second = cache.token().await.unwrap();

        assert_eq!(first, "token-0");
        assert_eq!(second, "token-0");
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn refetches_when_cached_token_is_within_refresh_window() {
        let near_expiry = Utc::now().timestamp() - 5;
        let fetcher = Arc::new(StubFetcher::new(near_expiry));
        let cache = CloudConnectorTokenCache::new(fetcher.clone());

        let first = cache.token().await.unwrap();
        fetcher
            .expires_at
            .store(Utc::now().timestamp() + 3_600, Ordering::SeqCst);
        let second = cache.token().await.unwrap();

        assert_eq!(first, "token-0");
        assert_eq!(second, "token-1");
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn propagates_fetch_error_without_caching() {
        struct FailingFetcher;
        impl CloudConnectorTokenFetcher for FailingFetcher {
            fn fetch(
                &self,
            ) -> Pin<
                Box<
                    dyn Future<Output = Result<CloudConnectorToken, GitlabClientError>> + Send + '_,
                >,
            > {
                Box::pin(async { Err(GitlabClientError::Unauthorized) })
            }
        }

        let cache = CloudConnectorTokenCache::new(Arc::new(FailingFetcher));
        assert!(matches!(
            cache.token().await,
            Err(GitlabClientError::Unauthorized)
        ));
        assert!(cache.cached.read().unwrap().is_none());
    }
}
