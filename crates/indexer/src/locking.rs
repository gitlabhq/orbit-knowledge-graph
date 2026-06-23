use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::nats::{KvPutOptions, KvPutResult};

pub const INDEXING_LOCKS_BUCKET: &str = "indexing_locks";

#[derive(Debug, thiserror::Error)]
pub enum LockError {
    #[error("lock operation failed: {0}")]
    Backend(String),
}

#[async_trait]
pub trait LockService: Send + Sync {
    async fn try_acquire(&self, key: &str, ttl: Duration) -> Result<bool, LockError>;
    /// Extend the lease on a lock we hold. `Ok(false)` means the lease was lost
    /// (someone else now holds it), so the caller should stop renewing.
    async fn renew(&self, key: &str, ttl: Duration) -> Result<bool, LockError>;
    async fn release(&self, key: &str) -> Result<(), LockError>;
}

pub struct LockGuard {
    service: Option<Arc<dyn LockService>>,
    key: String,
    renewer: Option<JoinHandle<()>>,
}

impl LockGuard {
    pub async fn acquire(
        service: Arc<dyn LockService>,
        key: &str,
        ttl: Duration,
        renew_interval: Duration,
    ) -> Result<Option<Self>, LockError> {
        if service.try_acquire(key, ttl).await? {
            let renewer = Some(Self::spawn_renewer(
                service.clone(),
                key.to_string(),
                ttl,
                renew_interval,
            ));
            Ok(Some(Self {
                service: Some(service),
                key: key.to_string(),
                renewer,
            }))
        } else {
            Ok(None)
        }
    }

    /// Renew the lease on `interval` (configured, independent of NATS ack timing)
    /// — so a job that outruns `ack_wait` can't have its lock stolen by a
    /// redelivered copy. Stops if the lease is lost.
    fn spawn_renewer(
        service: Arc<dyn LockService>,
        key: String,
        ttl: Duration,
        interval: Duration,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                match service.renew(&key, ttl).await {
                    Ok(true) => {}
                    Ok(false) => {
                        warn!(key = %key, "lock lease lost; stopping renewal");
                        break;
                    }
                    Err(e) => warn!(key = %key, error = %e, "lock lease renewal failed"),
                }
            }
        })
    }

    pub async fn release(mut self) -> Result<(), LockError> {
        if let Some(renewer) = self.renewer.take() {
            renewer.abort();
        }
        if let Some(service) = self.service.take() {
            service.release(&self.key).await
        } else {
            Ok(())
        }
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if let Some(renewer) = self.renewer.take() {
            renewer.abort();
        }
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
    /// Revision of each lock we currently hold, so `renew` can CAS on the exact
    /// revision we own — a renewal that lost the race (lock stolen after expiry)
    /// hits a revision mismatch and reports the loss instead of stealing back.
    revisions: Arc<Mutex<HashMap<String, u64>>>,
}

impl NatsLockService {
    pub fn new(nats: std::sync::Arc<dyn crate::nats::NatsServices>) -> Self {
        Self {
            nats,
            revisions: Arc::new(Mutex::new(HashMap::new())),
        }
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
                self.revisions.lock().insert(key.to_string(), revision);
                debug!(key, "lock acquired");
                return Ok(true);
            }
            KvPutResult::RevisionMismatch => return Ok(false),
            KvPutResult::AlreadyExists => {}
        }

        let entry = self
            .nats
            .kv_get(INDEXING_LOCKS_BUCKET, key)
            .await
            .map_err(|e| LockError::Backend(e.to_string()))?;
        let Some(entry) = entry else { return Ok(false) };

        match decode_expiration(&entry.value) {
            Some(at) if Utc::now() < at => {
                debug!(key, expires_at = %at, "lock contention, still valid");
                Ok(false)
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
                        self.revisions.lock().insert(key.to_string(), revision);
                        debug!(key, "lock acquired after expiry");
                        Ok(true)
                    }
                    KvPutResult::AlreadyExists | KvPutResult::RevisionMismatch => Ok(false),
                }
            }
        }
    }

    async fn renew(&self, key: &str, ttl: Duration) -> Result<bool, LockError> {
        let Some(revision) = self.revisions.lock().get(key).copied() else {
            return Ok(false);
        };
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
            KvPutResult::Success(revision) => {
                self.revisions.lock().insert(key.to_string(), revision);
                Ok(true)
            }
            KvPutResult::AlreadyExists | KvPutResult::RevisionMismatch => {
                self.revisions.lock().remove(key);
                Ok(false)
            }
        }
    }

    async fn release(&self, key: &str) -> Result<(), LockError> {
        self.revisions.lock().remove(key);
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

    // Long enough that the renewer never fires during these sub-second tests.
    const RENEW: Duration = Duration::from_secs(60);

    async fn settle() {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn lock_guard_release_consumes_and_releases() {
        let svc = Arc::new(MockLockService::new());
        let guard = LockGuard::acquire(svc.clone(), "k1", Duration::from_secs(1), RENEW)
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
            let _guard = LockGuard::acquire(svc.clone(), "k2", Duration::from_secs(1), RENEW)
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
                let _guard = LockGuard::acquire(svc, "k3", Duration::from_secs(1), RENEW)
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
        let result = LockGuard::acquire(svc.clone(), "k5", Duration::from_secs(1), RENEW)
            .await
            .expect("acquire ok");
        assert!(result.is_none(), "contended acquire must return None");
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
        async fn renew_extends_a_held_lease() {
            let (nats, svc) = new_service();
            assert!(
                svc.try_acquire("p6", Duration::from_secs(30))
                    .await
                    .unwrap()
            );
            let before =
                decode_expiration(&nats.get_kv(INDEXING_LOCKS_BUCKET, "p6").unwrap()).unwrap();
            assert!(svc.renew("p6", Duration::from_secs(120)).await.unwrap());
            let after =
                decode_expiration(&nats.get_kv(INDEXING_LOCKS_BUCKET, "p6").unwrap()).unwrap();
            assert!(after > before, "renew must push the expiry forward");
        }

        #[tokio::test]
        async fn renew_unheld_lock_reports_no_lease() {
            let (_, svc) = new_service();
            assert!(
                !svc.renew("never-acquired", Duration::from_secs(30))
                    .await
                    .unwrap(),
                "renewing a lock we never held must report loss, not steal it",
            );
        }
    }
}
