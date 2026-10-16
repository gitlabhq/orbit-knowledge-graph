//! Cross-pod cap on concurrent archive downloads per Gitaly storage.
//!
//! Losing a lease drops the work future, which only stops the download if the work stops on
//! drop; the legacy extractor carries its own cancel flag (`cache.rs`).

use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use code_graph::v2::CancellationToken;
use rand::RngExt;
use tokio::time::Instant;
use tracing::{debug, warn};

use crate::locking::{LockError, LockGuard, LockService};
use crate::modules::code::metrics::CodeMetrics;

const TENANT_LABEL: &str = "_tenant";
const TENANT_KEY_SEGMENT: &str = "tenant";
const POLL_INTERVAL: Duration = Duration::from_millis(500);
const POLL_JITTER_MS: u64 = 250;

#[derive(Debug, thiserror::Error)]
pub enum LeaseError {
    #[error("no archive lease for storage {storage} within {waited:?}")]
    Timeout { storage: String, waited: Duration },
    #[error("archive lease wait for storage {0} cancelled: the project lock was lost")]
    Cancelled(String),
    #[error("archive lease for storage {0} lost mid-download")]
    Lost(String),
    #[error("archive lease backend error: {0}")]
    Backend(String),
}

#[derive(Debug, Clone, Copy)]
pub struct LeaseSettings {
    pub storage_budget: usize,
    pub tenant_budget: usize,
    pub ttl: Duration,
    pub max_wait: Duration,
}

#[derive(Clone)]
pub struct ArchiveLeases {
    locks: Arc<dyn LockService>,
    settings: LeaseSettings,
    cancel: CancellationToken,
    metrics: CodeMetrics,
}

impl ArchiveLeases {
    pub fn new(
        locks: Arc<dyn LockService>,
        settings: LeaseSettings,
        cancel: CancellationToken,
        metrics: CodeMetrics,
    ) -> Self {
        Self {
            locks,
            settings,
            cancel,
            metrics,
        }
    }

    pub async fn run<T>(
        &self,
        storage: Option<&str>,
        work: impl Future<Output = T>,
    ) -> Result<T, LeaseError> {
        let budget = match storage {
            Some(_) => self.settings.storage_budget,
            None => self.settings.tenant_budget,
        };
        if budget == 0 {
            return Ok(work.await);
        }
        let scope = storage.unwrap_or(TENANT_LABEL);

        let queued = Instant::now();
        let mut slot = HeldSlot(None);
        let guard = &*slot.0.insert(self.acquire(storage, scope, budget).await?);
        self.metrics
            .record_archive_lease_wait(scope, queued.elapsed());
        let _slot = SlotInUse::new(&self.metrics, scope);

        let started = Instant::now();
        match self.hold(guard, scope, work).await {
            Ok(output) => {
                self.metrics
                    .record_archive_duration(scope, started.elapsed());
                slot.release(scope).await;
                Ok(output)
            }
            Err(error) => {
                self.metrics.record_archive_lease_lost(scope);
                slot.abandon();
                Err(error)
            }
        }
    }

    async fn acquire(
        &self,
        storage: Option<&str>,
        scope: &str,
        budget: usize,
    ) -> Result<LockGuard, LeaseError> {
        let queued = Instant::now();
        loop {
            if self.cancel.is_cancelled() {
                return Err(LeaseError::Cancelled(scope.to_string()));
            }
            let first = rand::rng().random_range(0..budget);
            for offset in 0..budget {
                let key = slot_key(storage, (first + offset) % budget);
                let acquired = LockGuard::acquire(Arc::clone(&self.locks), &key, self.settings.ttl)
                    .await
                    .map_err(|e| LeaseError::Backend(e.to_string()))?;
                if let Some(guard) = acquired {
                    debug!(storage = scope, key, "archive lease acquired");
                    return Ok(guard);
                }
            }
            if queued.elapsed() + POLL_INTERVAL > self.settings.max_wait {
                return Err(LeaseError::Timeout {
                    storage: scope.to_string(),
                    waited: queued.elapsed(),
                });
            }
            let jitter = rand::rng().random_range(0..POLL_JITTER_MS);
            tokio::time::sleep(POLL_INTERVAL + Duration::from_millis(jitter)).await;
        }
    }

    async fn hold<T>(
        &self,
        guard: &LockGuard,
        scope: &str,
        work: impl Future<Output = T>,
    ) -> Result<T, LeaseError> {
        let ttl = self.settings.ttl;
        let heartbeat = async {
            let mut ticker = tokio::time::interval((ttl / 3).max(Duration::from_millis(1)));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                match guard.renew(ttl).await {
                    Ok(()) => {}
                    Err(LockError::Lost) => return LeaseError::Lost(scope.to_string()),
                    // An expired lease may already be another pod's slot, so fence even without a Lost reply.
                    Err(error) if guard.time_left().is_zero() => {
                        warn!(storage = scope, %error, "archive lease expired while renewals failed");
                        return LeaseError::Lost(scope.to_string());
                    }
                    Err(error) => {
                        warn!(storage = scope, %error, "archive lease renewal failed transiently; retrying on the next tick");
                    }
                }
            }
        };
        tokio::select! {
            output = work => Ok(output),
            lost = heartbeat => Err(lost),
        }
    }
}

/// Releases the slot when the run future is dropped mid-work, unless the stored expiry has passed:
/// the key may then belong to another pod, so it is left to its TTL.
struct HeldSlot(Option<LockGuard>);

impl HeldSlot {
    /// Takes the guard unless its stored expiry has passed, in which case the key may be another
    /// pod's by now and is abandoned instead.
    fn take_unless_expired(&mut self) -> Option<LockGuard> {
        let guard = self.0.take()?;
        if guard.time_left().is_zero() {
            guard.abandon();
            return None;
        }
        Some(guard)
    }

    async fn release(&mut self, scope: &str) {
        let Some(guard) = self.take_unless_expired() else {
            return;
        };
        if let Err(error) = guard.release().await {
            warn!(storage = scope, %error, "archive lease release failed; its TTL will free the slot");
        }
    }

    fn abandon(&mut self) {
        if let Some(guard) = self.0.take() {
            guard.abandon();
        }
    }
}

impl Drop for HeldSlot {
    fn drop(&mut self) {
        drop(self.take_unless_expired());
    }
}

pub fn slot_key(storage: Option<&str>, slot: usize) -> String {
    let scope = match storage {
        Some(name) => base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(name),
        None => TENANT_KEY_SEGMENT.to_string(),
    };
    format!("archive.{scope}.{slot}")
}

struct SlotInUse<'a> {
    metrics: &'a CodeMetrics,
    scope: &'a str,
}

impl<'a> SlotInUse<'a> {
    fn new(metrics: &'a CodeMetrics, scope: &'a str) -> Self {
        metrics.add_archive_lease_slots(scope, 1);
        Self { metrics, scope }
    }
}

impl Drop for SlotInUse<'_> {
    fn drop(&mut self) {
        self.metrics.add_archive_lease_slots(self.scope, -1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::mocks::MockLockService;

    fn leases(locks: &Arc<MockLockService>, budget: usize, max_wait: Duration) -> ArchiveLeases {
        ArchiveLeases::new(
            locks.clone(),
            LeaseSettings {
                storage_budget: budget,
                tenant_budget: budget,
                ttl: Duration::from_secs(30),
                max_wait,
            },
            CancellationToken::new(),
            CodeMetrics::default(),
        )
    }

    #[test]
    fn slot_keys_are_scoped_per_storage_and_never_collide_with_the_tenant_key() {
        assert_ne!(slot_key(Some("default"), 0), slot_key(Some("storage2"), 0));
        assert_ne!(slot_key(Some("default"), 0), slot_key(Some("default"), 1));
        assert_ne!(slot_key(Some("tenant"), 0), slot_key(None, 0));
        assert!(!slot_key(Some("we ird/name"), 0).contains(' '));
    }

    #[tokio::test]
    async fn work_holds_a_slot_and_frees_it_afterwards() {
        let locks = Arc::new(MockLockService::new());
        let leases = leases(&locks, 1, Duration::from_secs(1));

        let held_inside = leases
            .run(Some("default"), async {
                locks.is_held(&slot_key(Some("default"), 0))
            })
            .await
            .unwrap();

        assert!(held_inside);
        assert!(!locks.is_held(&slot_key(Some("default"), 0)));
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_a_run_after_its_lease_expired_leaves_the_slot_to_its_new_owner() {
        let locks = Arc::new(MockLockService::new());
        let leases = leases(&locks, 1, Duration::from_secs(1));
        let key = slot_key(Some("default"), 0);

        let mut run = Box::pin(leases.run(Some("default"), std::future::pending::<()>()));
        let first = tokio::time::timeout(Duration::from_millis(10), &mut run).await;
        assert!(first.is_err(), "the work is still running");
        assert!(locks.is_held(&key), "the slot is held while the work runs");
        drop(run);
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert!(!locks.is_held(&key), "a live lease is released on drop");

        let mut run = Box::pin(leases.run(Some("default"), std::future::pending::<()>()));
        let second = tokio::time::timeout(Duration::from_millis(10), &mut run).await;
        assert!(second.is_err(), "the work is still running");
        locks.set_lock(&key);
        tokio::time::advance(Duration::from_secs(60)).await;
        drop(run);
        tokio::time::sleep(Duration::from_millis(1)).await;

        assert!(
            locks.is_held(&key),
            "an expired lease must not delete the new owner's key"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_full_storage_times_out_while_other_storages_proceed() {
        let locks = Arc::new(MockLockService::new());
        locks.set_lock(&slot_key(Some("default"), 0));
        let leases = leases(&locks, 1, Duration::from_secs(2));

        let busy = leases.run(Some("default"), async {}).await;
        let other = leases.run(Some("storage2"), async {}).await;

        assert!(matches!(busy, Err(LeaseError::Timeout { .. })));
        assert!(other.is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn a_waiter_gets_the_slot_once_the_holder_finishes() {
        let locks = Arc::new(MockLockService::new());
        let leases = leases(&locks, 1, Duration::from_secs(10));
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();

        let holder = tokio::spawn({
            let leases = leases.clone();
            async move {
                leases
                    .run(None, async {
                        let _ = release_rx.await;
                    })
                    .await
            }
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        let waiter = tokio::spawn({
            let leases = leases.clone();
            async move { leases.run(None, async { "ran" }).await }
        });
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(!waiter.is_finished(), "waiter must queue behind the holder");

        release_tx.send(()).unwrap();
        holder.await.unwrap().unwrap();
        assert_eq!(waiter.await.unwrap().unwrap(), "ran");
    }

    #[tokio::test(start_paused = true)]
    async fn losing_the_lease_cancels_the_work_and_spares_the_new_owners_key() {
        let locks = Arc::new(MockLockService::new());
        let leases = leases(&locks, 1, Duration::from_secs(1));

        let result = leases
            .run(Some("default"), async {
                locks.steal_leases();
                std::future::pending::<()>().await
            })
            .await;

        assert!(matches!(result, Err(LeaseError::Lost(_))));
        assert!(
            locks.is_held(&slot_key(Some("default"), 0)),
            "a lost lease must not delete the key its new owner holds"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn transient_renew_failures_do_not_cancel_until_the_lease_expires() {
        let locks = Arc::new(MockLockService::new());
        let leases = leases(&locks, 1, Duration::from_secs(1));
        locks.fail_renews_transiently();

        let outcome = leases
            .run(Some("default"), async {
                tokio::time::sleep(Duration::from_secs(20)).await;
                "done"
            })
            .await;
        assert_eq!(
            outcome.unwrap(),
            "done",
            "ttl is 30s; 20s of failed renews is still safe"
        );

        let outcome = leases
            .run(Some("default"), async {
                tokio::time::sleep(Duration::from_secs(60)).await;
            })
            .await;
        assert!(matches!(outcome, Err(LeaseError::Lost(_))));
    }

    #[tokio::test]
    async fn zero_budget_runs_without_touching_the_lock_service() {
        let locks = Arc::new(MockLockService::new());
        let leases = leases(&locks, 0, Duration::from_secs(1));

        let ran = leases.run(Some("default"), async { true }).await.unwrap();

        assert!(ran);
        assert_eq!(locks.renew_count(), 0);
        assert!(!locks.is_held(&slot_key(Some("default"), 0)));
    }

    #[tokio::test(start_paused = true)]
    async fn a_cancelled_wait_stops_queueing() {
        let locks = Arc::new(MockLockService::new());
        locks.set_lock(&slot_key(None, 0));
        let cancel = CancellationToken::new();
        let leases = ArchiveLeases::new(
            locks,
            LeaseSettings {
                storage_budget: 1,
                tenant_budget: 1,
                ttl: Duration::from_secs(30),
                max_wait: Duration::from_secs(300),
            },
            cancel.clone(),
            CodeMetrics::default(),
        );
        cancel.cancel();

        let started = Instant::now();
        let result = leases.run(None, async {}).await;

        assert!(matches!(result, Err(LeaseError::Cancelled(_))));
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn unknown_storage_uses_its_own_budget() {
        let locks = Arc::new(MockLockService::new());
        let leases = ArchiveLeases::new(
            locks.clone(),
            LeaseSettings {
                storage_budget: 0,
                tenant_budget: 1,
                ttl: Duration::from_secs(30),
                max_wait: Duration::from_secs(1),
            },
            CancellationToken::new(),
            CodeMetrics::default(),
        );
        locks.set_lock(&slot_key(None, 0));

        assert!(matches!(
            leases.run(None, async {}).await,
            Err(LeaseError::Timeout { .. })
        ));
        assert!(
            leases.run(Some("gitaly-1"), async {}).await.is_ok(),
            "storage budget 0 leaves known storages unthrottled"
        );
    }
}

#[cfg(test)]
mod fencing {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::modules::code::repository::cache::{LocalRepositoryCache, RepositoryCache};
    use crate::modules::code::repository::service::ByteStream;
    use crate::modules::code::repository::service::test_utils::build_tar_gz;
    use crate::testkit::mocks::MockLockService;

    fn throttled_stream(archive: Vec<u8>, pulled: Arc<AtomicUsize>) -> ByteStream {
        let chunks: Vec<bytes::Bytes> = archive
            .chunks(64)
            .map(bytes::Bytes::copy_from_slice)
            .collect();
        Box::pin(futures::stream::unfold(
            (chunks.into_iter(), pulled),
            |(mut chunks, pulled)| async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let next = chunks.next()?;
                pulled.fetch_add(1, Ordering::SeqCst);
                Some((Ok(next), (chunks, pulled)))
            },
        ))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_legacy_extractor_stops_pulling_the_archive_once_the_lease_is_lost() {
        let locks = Arc::new(MockLockService::new());
        let leases = ArchiveLeases::new(
            locks.clone(),
            LeaseSettings {
                storage_budget: 1,
                tenant_budget: 1,
                ttl: Duration::from_millis(300),
                max_wait: Duration::from_secs(1),
            },
            CancellationToken::new(),
            CodeMetrics::default(),
        );
        let dir = tempfile::TempDir::new().unwrap();
        let cache = LocalRepositoryCache::new(
            dir.path().to_path_buf(),
            u64::MAX,
            0,
            CodeMetrics::default(),
        );
        let content: Vec<u8> = (0..200_000u32)
            .map(|i| (i.wrapping_mul(2654435761) >> 24) as u8)
            .collect();
        let archive = build_tar_gz(&[("src/a.rs", &content)]);
        let total_chunks = archive.len().div_ceil(64);
        let pulled = Arc::new(AtomicUsize::new(0));
        let stream = throttled_stream(archive, Arc::clone(&pulled));

        let stolen = locks.clone();
        let outcome = leases
            .run(Some("default"), async move {
                stolen.steal_leases();
                cache.extract_archive(stream).await.map(|_| ())
            })
            .await;
        let at_loss = pulled.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(1)).await;
        let later = pulled.load(Ordering::SeqCst);

        assert!(matches!(outcome, Err(LeaseError::Lost(_))));
        assert!(
            at_loss < total_chunks,
            "the archive must still be streaming at loss"
        );
        assert!(
            later <= at_loss + 2,
            "stream kept being pulled after the lease was lost: {at_loss} -> {later} of {total_chunks}"
        );
    }
}
