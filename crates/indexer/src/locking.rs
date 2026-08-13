use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use tracing::{debug, warn};

use crate::nats::{KvPutOptions, KvPutResult};

pub const INDEXING_LOCKS_BUCKET: &str = "indexing_locks";

pub type LockRevision = u64;

#[derive(Debug, thiserror::Error)]
pub enum LockError {
    #[error("lock operation failed: {0}")]
    Backend(String),
    #[error("lock lost: another holder owns it")]
    Lost,
}

#[async_trait]
pub trait LockService: Send + Sync {
    async fn try_acquire(&self, key: &str, ttl: Duration) -> Result<bool, LockError>;

    async fn try_acquire_renewable(
        &self,
        key: &str,
        ttl: Duration,
    ) -> Result<Option<LockRevision>, LockError>;

    /// `Ok(None)`: the lease was lost to another holder.
    async fn renew(
        &self,
        key: &str,
        ttl: Duration,
        revision: LockRevision,
    ) -> Result<Option<LockRevision>, LockError>;

    async fn release(&self, key: &str) -> Result<(), LockError>;
}

struct Lease {
    expires_at: tokio::time::Instant,
    revision: LockRevision,
}

pub struct LockGuard {
    service: Option<Arc<dyn LockService>>,
    key: String,
    // std Mutex, never held across an await: renew is &self because index_project borrows the guard.
    lease: std::sync::Mutex<Lease>,
}

impl LockGuard {
    pub async fn acquire(
        service: Arc<dyn LockService>,
        key: &str,
        ttl: Duration,
    ) -> Result<Option<Self>, LockError> {
        match service.try_acquire_renewable(key, ttl).await? {
            Some(revision) => Ok(Some(Self {
                service: Some(service),
                key: key.to_string(),
                lease: std::sync::Mutex::new(Lease {
                    expires_at: tokio::time::Instant::now() + ttl,
                    revision,
                }),
            })),
            None => Ok(None),
        }
    }

    pub fn time_left(&self) -> Duration {
        self.lease
            .lock()
            .unwrap()
            .expires_at
            .saturating_duration_since(tokio::time::Instant::now())
    }

    pub async fn renew(&self, ttl: Duration) -> Result<(), LockError> {
        let Some(service) = &self.service else {
            return Ok(());
        };
        let revision = self.lease.lock().unwrap().revision;
        match service.renew(&self.key, ttl, revision).await? {
            Some(new_revision) => {
                let mut lease = self.lease.lock().unwrap();
                lease.revision = new_revision;
                lease.expires_at = tokio::time::Instant::now() + ttl;
                Ok(())
            }
            None => Err(LockError::Lost),
        }
    }

    pub async fn release(mut self) -> Result<(), LockError> {
        if let Some(service) = self.service.take() {
            service.release(&self.key).await
        } else {
            Ok(())
        }
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if let Some(service) = self.service.take() {
            let key = std::mem::take(&mut self.key);
            tokio::spawn(async move {
                if let Err(e) = service.release(&key).await {
                    warn!(key = %key, error = %e, "lock release on guard drop failed");
                }
            });
        }
    }
}

pub struct NatsLockService {
    nats: std::sync::Arc<dyn crate::nats::NatsServices>,
}

impl NatsLockService {
    pub fn new(nats: std::sync::Arc<dyn crate::nats::NatsServices>) -> Self {
        Self { nats }
    }
}

fn encode_expiration(at: DateTime<Utc>) -> Bytes {
    Bytes::from(at.to_rfc3339())
}

fn decode_expiration(value: &[u8]) -> Option<DateTime<Utc>> {
    let s = std::str::from_utf8(value).ok()?;
    if s.is_empty() {
        return None;
    }
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.with_timezone(&Utc))
}

#[async_trait]
impl LockService for NatsLockService {
    async fn try_acquire(&self, key: &str, ttl: Duration) -> Result<bool, LockError> {
        Ok(self.try_acquire_renewable(key, ttl).await?.is_some())
    }

    async fn try_acquire_renewable(
        &self,
        key: &str,
        ttl: Duration,
    ) -> Result<Option<LockRevision>, LockError> {
        let chrono_ttl =
            chrono::Duration::from_std(ttl).map_err(|e| LockError::Backend(e.to_string()))?;
        let expiration = Utc::now() + chrono_ttl;
        let value = encode_expiration(expiration);

        match self
            .nats
            .kv_put(
                INDEXING_LOCKS_BUCKET,
                key,
                value.clone(),
                KvPutOptions::create_only(),
            )
            .await
            .map_err(|e| LockError::Backend(e.to_string()))?
        {
            KvPutResult::Success(revision) => {
                debug!(key, "lock acquired");
                return Ok(Some(revision));
            }
            KvPutResult::RevisionMismatch => return Ok(None),
            KvPutResult::AlreadyExists => {}
        }

        let entry = self
            .nats
            .kv_get(INDEXING_LOCKS_BUCKET, key)
            .await
            .map_err(|e| LockError::Backend(e.to_string()))?;
        let Some(entry) = entry else { return Ok(None) };

        match decode_expiration(&entry.value) {
            Some(at) if Utc::now() < at => {
                debug!(key, expires_at = %at, "lock contention, still valid");
                Ok(None)
            }
            Some(_) | None => {
                match self
                    .nats
                    .kv_put(
                        INDEXING_LOCKS_BUCKET,
                        key,
                        value,
                        KvPutOptions::update_revision(entry.revision),
                    )
                    .await
                    .map_err(|e| LockError::Backend(e.to_string()))?
                {
                    KvPutResult::Success(revision) => {
                        debug!(key, "lock acquired after expiry");
                        Ok(Some(revision))
                    }
                    KvPutResult::AlreadyExists | KvPutResult::RevisionMismatch => Ok(None),
                }
            }
        }
    }

    async fn renew(
        &self,
        key: &str,
        ttl: Duration,
        revision: LockRevision,
    ) -> Result<Option<LockRevision>, LockError> {
        let chrono_ttl =
            chrono::Duration::from_std(ttl).map_err(|e| LockError::Backend(e.to_string()))?;
        let value = encode_expiration(Utc::now() + chrono_ttl);

        match self
            .nats
            .kv_put(
                INDEXING_LOCKS_BUCKET,
                key,
                value,
                KvPutOptions::update_revision(revision),
            )
            .await
            .map_err(|e| LockError::Backend(e.to_string()))?
        {
            KvPutResult::Success(new_revision) => Ok(Some(new_revision)),
            KvPutResult::AlreadyExists | KvPutResult::RevisionMismatch => Ok(None),
        }
    }

    async fn release(&self, key: &str) -> Result<(), LockError> {
        let result = self
            .nats
            .kv_delete(INDEXING_LOCKS_BUCKET, key)
            .await
            .map_err(|e| LockError::Backend(e.to_string()));
        debug!(key, "lock released");
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::mocks::MockLockService;

    async fn settle() {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn lock_guard_release_consumes_and_releases() {
        let svc = Arc::new(MockLockService::new());
        let guard = LockGuard::acquire(svc.clone(), "k1", Duration::from_secs(1))
            .await
            .expect("acquire ok")
            .expect("acquired");
        assert!(svc.is_held("k1"));
        guard.release().await.expect("release ok");
        assert!(!svc.is_held("k1"));
    }

    #[tokio::test]
    async fn lock_guard_drop_spawns_release() {
        let svc = Arc::new(MockLockService::new());
        {
            let _guard = LockGuard::acquire(svc.clone(), "k2", Duration::from_secs(1))
                .await
                .expect("acquire ok")
                .expect("acquired");
            assert!(svc.is_held("k2"));
        }
        settle().await;
        assert!(!svc.is_held("k2"), "drop must release the lock");
    }

    #[tokio::test]
    async fn lock_guard_drop_releases_on_cancellation() {
        let svc = Arc::new(MockLockService::new());
        let (acquired_tx, acquired_rx) = tokio::sync::oneshot::channel();

        let work = tokio::spawn({
            let svc = svc.clone();
            async move {
                let _guard = LockGuard::acquire(svc, "k3", Duration::from_secs(1))
                    .await
                    .expect("acquire ok")
                    .expect("acquired");
                acquired_tx.send(()).unwrap();
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        });

        acquired_rx.await.unwrap();
        assert!(svc.is_held("k3"));

        work.abort();
        let _ = work.await;
        settle().await;

        assert!(
            !svc.is_held("k3"),
            "cancelling the holding task must release the lock via Drop",
        );
    }

    #[tokio::test]
    async fn lock_guard_acquire_returns_none_when_held() {
        let svc = Arc::new(MockLockService::new());
        svc.set_lock("k5");
        let result = LockGuard::acquire(svc.clone(), "k5", Duration::from_secs(1))
            .await
            .expect("acquire ok");
        assert!(result.is_none(), "contended acquire must return None");
    }

    #[tokio::test]
    async fn lock_guard_renew_extends_the_lease() {
        let svc = Arc::new(MockLockService::new());
        let guard = LockGuard::acquire(svc.clone(), "k6", Duration::from_millis(50))
            .await
            .expect("acquire ok")
            .expect("acquired");
        let before = guard.time_left();
        guard
            .renew(Duration::from_secs(30))
            .await
            .expect("renew ok");
        assert!(
            guard.time_left() > before,
            "renew must push the expiry further out",
        );
    }

    #[tokio::test]
    async fn lock_guard_renew_reports_lost_when_stolen() {
        let svc = Arc::new(MockLockService::new());
        let guard = LockGuard::acquire(svc.clone(), "k7", Duration::from_secs(1))
            .await
            .expect("acquire ok")
            .expect("acquired");
        svc.steal_leases();
        assert!(
            matches!(
                guard.renew(Duration::from_secs(1)).await,
                Err(LockError::Lost)
            ),
            "a stolen lease must surface as LockError::Lost",
        );
    }

    mod nats_lock_service {
        use super::*;
        use crate::testkit::mocks::MockNatsServices;

        fn new_service() -> (Arc<MockNatsServices>, NatsLockService) {
            let nats = Arc::new(MockNatsServices::new());
            let svc = NatsLockService::new(nats.clone());
            (nats, svc)
        }

        #[tokio::test]
        async fn first_acquire_succeeds_and_stores_future_expiration() {
            let (nats, svc) = new_service();
            let acquired = svc
                .try_acquire("p1", Duration::from_secs(30))
                .await
                .expect("acquire");
            assert!(acquired);

            let stored = nats.get_kv(INDEXING_LOCKS_BUCKET, "p1").expect("value");
            let expires = decode_expiration(&stored).expect("rfc3339 expiry");
            assert!(expires > Utc::now(), "stored expiry must be in the future");
        }

        #[tokio::test]
        async fn reacquire_while_still_valid_returns_false() {
            let (_, svc) = new_service();
            assert!(
                svc.try_acquire("p2", Duration::from_secs(30))
                    .await
                    .unwrap()
            );
            assert!(
                !svc.try_acquire("p2", Duration::from_secs(30))
                    .await
                    .unwrap()
            );
        }

        #[tokio::test]
        async fn reacquire_after_expiry_succeeds() {
            let (nats, svc) = new_service();
            assert!(svc.try_acquire("p3", Duration::from_secs(1)).await.unwrap());

            nats.set_kv(
                INDEXING_LOCKS_BUCKET,
                "p3",
                encode_expiration(Utc::now() - chrono::Duration::seconds(1)),
            );

            assert!(
                svc.try_acquire("p3", Duration::from_secs(30))
                    .await
                    .unwrap(),
                "expired lock must be reclaimable",
            );
        }

        #[tokio::test]
        async fn release_then_acquire_succeeds() {
            let (_, svc) = new_service();
            assert!(
                svc.try_acquire("p4", Duration::from_secs(30))
                    .await
                    .unwrap()
            );
            svc.release("p4").await.expect("release");
            assert!(
                svc.try_acquire("p4", Duration::from_secs(30))
                    .await
                    .unwrap(),
                "fresh acquire after release must succeed",
            );
        }

        #[tokio::test]
        async fn malformed_value_is_treated_as_stale() {
            let (nats, svc) = new_service();
            nats.set_kv(
                INDEXING_LOCKS_BUCKET,
                "p5",
                Bytes::from_static(b"not-a-timestamp"),
            );
            assert!(
                svc.try_acquire("p5", Duration::from_secs(30))
                    .await
                    .unwrap(),
                "unparseable lock value must not pin the lock forever",
            );
        }

        #[tokio::test]
        async fn renew_advances_revision_and_pushes_expiry() {
            let (nats, svc) = new_service();
            let revision = svc
                .try_acquire_renewable("p6", Duration::from_secs(1))
                .await
                .unwrap()
                .expect("acquired");
            let before =
                decode_expiration(&nats.get_kv(INDEXING_LOCKS_BUCKET, "p6").unwrap()).unwrap();

            let renewed = svc
                .renew("p6", Duration::from_secs(30), revision)
                .await
                .unwrap();

            assert_eq!(
                renewed,
                Some(revision + 1),
                "renew returns the next revision"
            );
            let after =
                decode_expiration(&nats.get_kv(INDEXING_LOCKS_BUCKET, "p6").unwrap()).unwrap();
            assert!(after > before, "renew must push the stored expiry forward");
        }

        #[tokio::test]
        async fn renew_reports_none_when_revision_is_stale() {
            let (_, svc) = new_service();
            let revision = svc
                .try_acquire_renewable("p7", Duration::from_secs(30))
                .await
                .unwrap()
                .expect("acquired");
            svc.renew("p7", Duration::from_secs(30), revision)
                .await
                .unwrap()
                .expect("first renew supersedes the revision");

            let stale = svc
                .renew("p7", Duration::from_secs(30), revision)
                .await
                .unwrap();

            assert!(
                stale.is_none(),
                "renewing against a superseded revision reports the lease lost",
            );
        }
    }
}
