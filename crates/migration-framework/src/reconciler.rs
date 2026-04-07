use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use clickhouse_client::ArrowClickHouseClient;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::{
    LedgerMigrationRecord, Migration, MigrationContext, MigrationLedger, MigrationLedgerError,
    MigrationRegistry, MigrationStatus, MigrationType,
};

pub const MIGRATION_RECONCILER_LOCK_KEY: &str = "migration.reconciler";
pub const MIGRATION_VERSION_KEY: &str = "migration.version";
pub const INDEXING_LOCKS_BUCKET: &str = "indexing_locks";
pub const DEFAULT_LOCK_TTL: Duration = Duration::from_secs(60);
pub const DEFAULT_RECONCILE_INTERVAL: Duration = Duration::from_secs(30);
pub const DEFAULT_MAX_RETRIES: u32 = 5;

#[derive(Debug, thiserror::Error)]
pub enum ReconcilerError {
    #[error("ledger operation failed: {0}")]
    Ledger(#[from] MigrationLedgerError),
    #[error("lock operation failed: {0}")]
    Lock(String),
    #[error("nats operation failed: {0}")]
    Nats(String),
}

#[derive(Clone, Debug)]
pub struct ReconcilerConfig {
    pub lock_ttl: Duration,
    pub reconcile_interval: Duration,
    pub max_retries: u32,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            lock_ttl: DEFAULT_LOCK_TTL,
            reconcile_interval: DEFAULT_RECONCILE_INTERVAL,
            max_retries: DEFAULT_MAX_RETRIES,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LockLease {
    pub key: String,
    pub revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvRecord {
    pub revision: u64,
    pub value: Bytes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KvWrite {
    Written { revision: u64 },
    AlreadyExists,
    RevisionMismatch,
}

#[async_trait]
pub trait KvStore: Send + Sync {
    async fn get(&self, bucket: &str, key: &str) -> Result<Option<KvRecord>>;
    async fn create(&self, bucket: &str, key: &str, value: Bytes, ttl: Duration)
    -> Result<KvWrite>;
    async fn update(
        &self,
        bucket: &str,
        key: &str,
        value: Bytes,
        expected_revision: u64,
    ) -> Result<KvWrite>;
    async fn put(&self, bucket: &str, key: &str, value: Bytes) -> Result<KvWrite>;
    async fn delete(&self, bucket: &str, key: &str) -> Result<()>;
}

#[async_trait]
pub trait MigrationLock: Send + Sync {
    async fn try_acquire(
        &self,
        key: &str,
        ttl: Duration,
    ) -> Result<Option<LockLease>, ReconcilerError>;
    async fn refresh(&self, lease: &LockLease) -> Result<Option<LockLease>, ReconcilerError>;
    async fn release(&self, key: &str) -> Result<(), ReconcilerError>;
}

pub struct NatsMigrationLock {
    kv: Arc<dyn KvStore>,
}

impl NatsMigrationLock {
    pub fn new(kv: Arc<dyn KvStore>) -> Self {
        Self { kv }
    }
}

#[async_trait]
impl MigrationLock for NatsMigrationLock {
    async fn try_acquire(
        &self,
        key: &str,
        ttl: Duration,
    ) -> Result<Option<LockLease>, ReconcilerError> {
        match self
            .kv
            .create(INDEXING_LOCKS_BUCKET, key, Bytes::new(), ttl)
            .await
            .map_err(|e| ReconcilerError::Lock(e.to_string()))?
        {
            KvWrite::Written { revision } => Ok(Some(LockLease {
                key: key.to_string(),
                revision,
            })),
            KvWrite::AlreadyExists | KvWrite::RevisionMismatch => Ok(None),
        }
    }

    async fn refresh(&self, lease: &LockLease) -> Result<Option<LockLease>, ReconcilerError> {
        match self
            .kv
            .update(
                INDEXING_LOCKS_BUCKET,
                &lease.key,
                Bytes::new(),
                lease.revision,
            )
            .await
            .map_err(|e| ReconcilerError::Lock(e.to_string()))?
        {
            KvWrite::Written { revision } => Ok(Some(LockLease {
                key: lease.key.clone(),
                revision,
            })),
            KvWrite::AlreadyExists | KvWrite::RevisionMismatch => Ok(None),
        }
    }

    async fn release(&self, key: &str) -> Result<(), ReconcilerError> {
        self.kv
            .delete(INDEXING_LOCKS_BUCKET, key)
            .await
            .map_err(|e| ReconcilerError::Lock(e.to_string()))
    }
}

#[async_trait]
pub trait LedgerStore: Send + Sync {
    async fn ensure_table(&self) -> Result<(), MigrationLedgerError>;
    async fn list(&self) -> Result<Vec<LedgerMigrationRecord>, MigrationLedgerError>;
    async fn mark_pending(&self, migration: &dyn Migration) -> Result<(), MigrationLedgerError>;
    async fn mark_preparing(
        &self,
        migration: &dyn Migration,
        retry_count: u32,
    ) -> Result<(), MigrationLedgerError>;
    async fn mark_completed(
        &self,
        migration: &dyn Migration,
        retry_count: u32,
    ) -> Result<(), MigrationLedgerError>;
    async fn mark_failed(
        &self,
        migration: &dyn Migration,
        error_message: &str,
        retry_count: u32,
    ) -> Result<(), MigrationLedgerError>;
}

#[async_trait]
impl LedgerStore for MigrationLedger {
    async fn ensure_table(&self) -> Result<(), MigrationLedgerError> {
        MigrationLedger::ensure_table(self).await
    }

    async fn list(&self) -> Result<Vec<LedgerMigrationRecord>, MigrationLedgerError> {
        MigrationLedger::list(self).await
    }

    async fn mark_pending(&self, migration: &dyn Migration) -> Result<(), MigrationLedgerError> {
        MigrationLedger::mark_pending(self, migration).await
    }

    async fn mark_preparing(
        &self,
        migration: &dyn Migration,
        retry_count: u32,
    ) -> Result<(), MigrationLedgerError> {
        MigrationLedger::mark_preparing(self, migration, retry_count).await
    }

    async fn mark_completed(
        &self,
        migration: &dyn Migration,
        retry_count: u32,
    ) -> Result<(), MigrationLedgerError> {
        MigrationLedger::mark_completed(self, migration, retry_count).await
    }

    async fn mark_failed(
        &self,
        migration: &dyn Migration,
        error_message: &str,
        retry_count: u32,
    ) -> Result<(), MigrationLedgerError> {
        MigrationLedger::mark_failed(self, migration, error_message, retry_count).await
    }
}

pub struct Reconciler {
    registry: Arc<MigrationRegistry>,
    ledger: Arc<dyn LedgerStore>,
    lock: Arc<dyn MigrationLock>,
    kv: Arc<dyn KvStore>,
    clickhouse: ArrowClickHouseClient,
    config: ReconcilerConfig,
}

impl Reconciler {
    pub fn new(
        registry: Arc<MigrationRegistry>,
        ledger: Arc<dyn LedgerStore>,
        lock: Arc<dyn MigrationLock>,
        kv: Arc<dyn KvStore>,
        clickhouse: ArrowClickHouseClient,
        config: ReconcilerConfig,
    ) -> Self {
        Self {
            registry,
            ledger,
            lock,
            kv,
            clickhouse,
            config,
        }
    }

    pub async fn run(&self, shutdown: CancellationToken) {
        let mut interval = tokio::time::interval(self.config.reconcile_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("migration reconciler shutting down");
                    return;
                }
                _ = interval.tick() => {
                    if let Err(error) = self.reconcile_once().await {
                        warn!(error = %error, "migration reconciler iteration failed");
                    }
                }
            }
        }
    }

    pub async fn reconcile_once(&self) -> Result<(), ReconcilerError> {
        let Some(mut lease) = self
            .lock
            .try_acquire(MIGRATION_RECONCILER_LOCK_KEY, self.config.lock_ttl)
            .await?
        else {
            debug!("migration reconciler lock unavailable");
            return Ok(());
        };

        self.ledger.ensure_table().await?;

        let mut ledger_records = self.ledger.list().await?;
        if !self.is_lock_holder_eligible(&ledger_records) {
            info!("migration reconciler not eligible for current ledger state, releasing lock");
            self.lock.release(MIGRATION_RECONCILER_LOCK_KEY).await?;
            return Ok(());
        }

        for migration in self.registry.migrations() {
            let Some(refreshed) = self.lock.refresh(&lease).await? else {
                warn!("migration reconciler lost lease before processing migration");
                return Ok(());
            };
            lease = refreshed;

            let state = ledger_record_for(&ledger_records, migration.version());
            match self.process_migration(migration.as_ref(), state).await? {
                StepOutcome::Continue => {
                    ledger_records = self.ledger.list().await?;
                }
                StepOutcome::Stop => break,
            }
        }

        self.lock.release(MIGRATION_RECONCILER_LOCK_KEY).await?;
        Ok(())
    }

    pub fn is_lock_holder_eligible(&self, ledger_records: &[LedgerMigrationRecord]) -> bool {
        let max_known_version = self
            .registry
            .migrations()
            .iter()
            .map(|migration| migration.version())
            .max()
            .unwrap_or(0);

        if ledger_records.iter().any(|record| {
            matches!(
                record.status,
                MigrationStatus::Pending | MigrationStatus::Preparing | MigrationStatus::Failed
            ) && record.version > max_known_version
        }) {
            return false;
        }

        let highest_completed = ledger_records
            .iter()
            .filter(|record| record.status == MigrationStatus::Completed)
            .map(|record| record.version)
            .max()
            .unwrap_or(0);

        let resumable: HashMap<u64, &LedgerMigrationRecord> = ledger_records
            .iter()
            .filter(|record| {
                matches!(
                    record.status,
                    MigrationStatus::Pending | MigrationStatus::Preparing | MigrationStatus::Failed
                )
            })
            .map(|record| (record.version, record))
            .collect();

        self.registry.migrations().iter().any(|migration| {
            migration.version() > highest_completed || resumable.contains_key(&migration.version())
        })
    }

    async fn process_migration(
        &self,
        migration: &dyn Migration,
        state: Option<LedgerMigrationRecord>,
    ) -> Result<StepOutcome, ReconcilerError> {
        if migration.migration_type() != MigrationType::Additive {
            warn!(
                version = migration.version(),
                name = migration.name(),
                "non-additive migration encountered; stopping reconciler"
            );
            return Ok(StepOutcome::Stop);
        }

        match state {
            Some(record) if record.status == MigrationStatus::Completed => {
                Ok(StepOutcome::Continue)
            }
            Some(record) if record.status == MigrationStatus::Failed => {
                if record.retry_count >= self.config.max_retries {
                    warn!(
                        version = migration.version(),
                        retry_count = record.retry_count,
                        max_retries = self.config.max_retries,
                        "migration reached max retries; stopping reconciler"
                    );
                    return Ok(StepOutcome::Stop);
                }

                self.run_prepare(migration, record.retry_count + 1).await
            }
            Some(record) if record.status == MigrationStatus::Preparing => {
                self.run_prepare(migration, record.retry_count.max(1)).await
            }
            Some(record) if record.status == MigrationStatus::Pending => {
                self.run_prepare(migration, record.retry_count.max(1)).await
            }
            Some(_) | None => {
                self.ledger.mark_pending(migration).await?;
                self.run_prepare(migration, 1).await
            }
        }
    }

    async fn run_prepare(
        &self,
        migration: &dyn Migration,
        retry_count: u32,
    ) -> Result<StepOutcome, ReconcilerError> {
        self.ledger.mark_preparing(migration, retry_count).await?;
        let ctx = MigrationContext::new(self.clickhouse.clone());

        match migration.prepare(&ctx).await {
            Ok(()) => {
                self.ledger.mark_completed(migration, retry_count).await?;
                self.publish_schema_version(migration.version()).await?;
                info!(
                    version = migration.version(),
                    name = migration.name(),
                    "migration completed"
                );
                Ok(StepOutcome::Continue)
            }
            Err(error) => {
                error!(
                    version = migration.version(),
                    name = migration.name(),
                    retry_count,
                    error = %error,
                    "migration failed"
                );
                self.ledger
                    .mark_failed(migration, &error.to_string(), retry_count)
                    .await?;
                Ok(StepOutcome::Stop)
            }
        }
    }

    async fn publish_schema_version(&self, version: u64) -> Result<(), ReconcilerError> {
        self.kv
            .put(
                INDEXING_LOCKS_BUCKET,
                MIGRATION_VERSION_KEY,
                Bytes::from(version.to_string()),
            )
            .await
            .map(|_| ())
            .map_err(|e| ReconcilerError::Nats(e.to_string()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum StepOutcome {
    Continue,
    Stop,
}

fn ledger_record_for(
    records: &[LedgerMigrationRecord],
    version: u64,
) -> Option<LedgerMigrationRecord> {
    records
        .iter()
        .find(|record| record.version == version)
        .cloned()
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::Arc;

    use anyhow::{Result, anyhow};
    use async_trait::async_trait;
    use chrono::Utc;
    use parking_lot::Mutex;

    use super::*;

    struct TestMigration {
        version: u64,
        name: &'static str,
        outcome: Mutex<VecDeque<Result<()>>>,
    }

    impl TestMigration {
        fn success(version: u64, name: &'static str) -> Self {
            Self {
                version,
                name,
                outcome: Mutex::new(VecDeque::from([Ok(())])),
            }
        }

        fn with_outcomes(version: u64, name: &'static str, outcomes: Vec<Result<()>>) -> Self {
            Self {
                version,
                name,
                outcome: Mutex::new(outcomes.into()),
            }
        }
    }

    #[async_trait]
    impl Migration for TestMigration {
        fn version(&self) -> u64 {
            self.version
        }

        fn name(&self) -> &str {
            self.name
        }

        fn migration_type(&self) -> MigrationType {
            MigrationType::Additive
        }

        async fn prepare(&self, _ctx: &MigrationContext) -> Result<()> {
            self.outcome.lock().pop_front().unwrap_or(Ok(()))
        }
    }

    #[derive(Default)]
    struct TestLedger {
        records: Mutex<HashMap<u64, LedgerMigrationRecord>>,
    }

    #[async_trait]
    impl LedgerStore for TestLedger {
        async fn ensure_table(&self) -> Result<(), MigrationLedgerError> {
            Ok(())
        }

        async fn list(&self) -> Result<Vec<LedgerMigrationRecord>, MigrationLedgerError> {
            Ok(self.records.lock().values().cloned().collect())
        }

        async fn mark_pending(
            &self,
            migration: &dyn Migration,
        ) -> Result<(), MigrationLedgerError> {
            self.records.lock().insert(
                migration.version(),
                record(migration, MigrationStatus::Pending, None, 0),
            );
            Ok(())
        }

        async fn mark_preparing(
            &self,
            migration: &dyn Migration,
            retry_count: u32,
        ) -> Result<(), MigrationLedgerError> {
            self.records.lock().insert(
                migration.version(),
                record(migration, MigrationStatus::Preparing, None, retry_count),
            );
            Ok(())
        }

        async fn mark_completed(
            &self,
            migration: &dyn Migration,
            retry_count: u32,
        ) -> Result<(), MigrationLedgerError> {
            self.records.lock().insert(
                migration.version(),
                record(migration, MigrationStatus::Completed, None, retry_count),
            );
            Ok(())
        }

        async fn mark_failed(
            &self,
            migration: &dyn Migration,
            error_message: &str,
            retry_count: u32,
        ) -> Result<(), MigrationLedgerError> {
            self.records.lock().insert(
                migration.version(),
                record(
                    migration,
                    MigrationStatus::Failed,
                    Some(error_message.to_string()),
                    retry_count,
                ),
            );
            Ok(())
        }
    }

    #[derive(Default)]
    struct TestLock {
        held: Mutex<bool>,
        revision: Mutex<u64>,
        lose_on_refresh: Mutex<bool>,
        release_count: Mutex<u32>,
    }

    #[async_trait]
    impl MigrationLock for TestLock {
        async fn try_acquire(
            &self,
            key: &str,
            _ttl: Duration,
        ) -> Result<Option<LockLease>, ReconcilerError> {
            let mut held = self.held.lock();
            if *held {
                return Ok(None);
            }

            *held = true;
            let mut revision = self.revision.lock();
            *revision += 1;
            Ok(Some(LockLease {
                key: key.to_string(),
                revision: *revision,
            }))
        }

        async fn refresh(&self, lease: &LockLease) -> Result<Option<LockLease>, ReconcilerError> {
            if *self.lose_on_refresh.lock() {
                *self.held.lock() = false;
                return Ok(None);
            }

            if !*self.held.lock() {
                return Ok(None);
            }

            let mut revision = self.revision.lock();
            *revision += 1;
            Ok(Some(LockLease {
                key: lease.key.clone(),
                revision: *revision,
            }))
        }

        async fn release(&self, _key: &str) -> Result<(), ReconcilerError> {
            *self.held.lock() = false;
            *self.release_count.lock() += 1;
            Ok(())
        }
    }

    #[derive(Default)]
    struct TestKvStore {
        data: Mutex<HashMap<String, HashMap<String, KvRecord>>>,
    }

    #[async_trait]
    impl KvStore for TestKvStore {
        async fn get(&self, bucket: &str, key: &str) -> Result<Option<KvRecord>> {
            Ok(self
                .data
                .lock()
                .get(bucket)
                .and_then(|entries: &HashMap<String, KvRecord>| entries.get(key).cloned()))
        }

        async fn create(
            &self,
            bucket: &str,
            key: &str,
            value: Bytes,
            _ttl: Duration,
        ) -> Result<KvWrite> {
            let mut data = self.data.lock();
            let bucket_data = data.entry(bucket.to_string()).or_default();
            if bucket_data.contains_key(key) {
                return Ok(KvWrite::AlreadyExists);
            }

            bucket_data.insert(key.to_string(), KvRecord { revision: 1, value });
            Ok(KvWrite::Written { revision: 1 })
        }

        async fn update(
            &self,
            bucket: &str,
            key: &str,
            value: Bytes,
            expected_revision: u64,
        ) -> Result<KvWrite> {
            let mut data = self.data.lock();
            let Some(record) = data.entry(bucket.to_string()).or_default().get_mut(key) else {
                return Ok(KvWrite::RevisionMismatch);
            };

            if record.revision != expected_revision {
                return Ok(KvWrite::RevisionMismatch);
            }

            record.revision += 1;
            record.value = value;
            Ok(KvWrite::Written {
                revision: record.revision,
            })
        }

        async fn put(&self, bucket: &str, key: &str, value: Bytes) -> Result<KvWrite> {
            let mut data = self.data.lock();
            let bucket_data = data.entry(bucket.to_string()).or_default();
            let revision = bucket_data
                .get(key)
                .map(|record| record.revision + 1)
                .unwrap_or(1);
            bucket_data.insert(key.to_string(), KvRecord { revision, value });
            Ok(KvWrite::Written { revision })
        }

        async fn delete(&self, bucket: &str, key: &str) -> Result<()> {
            if let Some(bucket_data) = self
                .data
                .lock()
                .get_mut(bucket)
                .map(|entries: &mut HashMap<String, KvRecord>| entries)
            {
                bucket_data.remove(key);
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn lock_acquisition_and_refresh_use_compare_and_swap() {
        let kv = Arc::new(TestKvStore::default());
        let lock = NatsMigrationLock::new(kv.clone());

        let lease = lock
            .try_acquire(MIGRATION_RECONCILER_LOCK_KEY, DEFAULT_LOCK_TTL)
            .await
            .expect("acquire should succeed")
            .expect("lease should exist");
        assert_eq!(lease.revision, 1);

        let refreshed = lock
            .refresh(&lease)
            .await
            .expect("refresh should succeed")
            .expect("lease should remain held");
        assert_eq!(refreshed.revision, 2);

        kv.delete(INDEXING_LOCKS_BUCKET, MIGRATION_RECONCILER_LOCK_KEY)
            .await
            .expect("delete should succeed");
        assert!(
            lock.refresh(&refreshed)
                .await
                .expect("refresh should return")
                .is_none()
        );
    }

    #[test]
    fn eligibility_requires_advancement_or_resumable_version() {
        let mut registry = MigrationRegistry::new();
        registry.register(Box::new(TestMigration::success(1, "one")));
        registry.register(Box::new(TestMigration::success(2, "two")));

        let reconciler = build_reconciler(Arc::new(registry), Arc::new(TestLedger::default()));

        let older_binary_view = vec![LedgerMigrationRecord {
            version: 3,
            name: "three".to_string(),
            migration_type: MigrationType::Additive,
            status: MigrationStatus::Pending,
            started_at: None,
            completed_at: None,
            error_message: None,
            retry_count: 0,
        }];
        assert!(!reconciler.is_lock_holder_eligible(&older_binary_view));

        let resumable_view = vec![LedgerMigrationRecord {
            version: 2,
            name: "two".to_string(),
            migration_type: MigrationType::Additive,
            status: MigrationStatus::Failed,
            started_at: None,
            completed_at: None,
            error_message: Some("boom".to_string()),
            retry_count: 1,
        }];
        assert!(reconciler.is_lock_holder_eligible(&resumable_view));
    }

    #[tokio::test]
    async fn reconciler_applies_migrations_sequentially() {
        let mut registry = MigrationRegistry::new();
        registry.register(Box::new(TestMigration::success(1, "one")));
        registry.register(Box::new(TestMigration::success(2, "two")));

        let ledger = Arc::new(TestLedger::default());
        let kv = Arc::new(TestKvStore::default());
        let lock = Arc::new(TestLock::default());
        let reconciler =
            build_reconciler_with_deps(Arc::new(registry), ledger.clone(), kv.clone(), lock);

        reconciler
            .reconcile_once()
            .await
            .expect("reconcile should succeed");

        let records = ledger.list().await.expect("ledger should load");
        assert_eq!(records.len(), 2);
        assert!(
            records
                .iter()
                .all(|record| record.status == MigrationStatus::Completed)
        );

        let version = kv
            .get(INDEXING_LOCKS_BUCKET, MIGRATION_VERSION_KEY)
            .await
            .expect("kv get should succeed")
            .expect("migration version should exist");
        assert_eq!(version.value, Bytes::from("2"));
    }

    #[tokio::test]
    async fn reconciler_records_failures_and_respects_retry_limit() {
        let mut registry = MigrationRegistry::new();
        registry.register(Box::new(TestMigration::with_outcomes(
            1,
            "one",
            vec![Err(anyhow!("boom")), Err(anyhow!("boom again"))],
        )));

        let ledger = Arc::new(TestLedger::default());
        let reconciler = build_reconciler_with_deps(
            Arc::new(registry),
            ledger.clone(),
            Arc::new(TestKvStore::default()),
            Arc::new(TestLock::default()),
        );

        reconciler
            .reconcile_once()
            .await
            .expect("first reconcile should complete");
        let first = ledger_record_for(&ledger.list().await.expect("ledger should load"), 1)
            .expect("record should exist");
        assert_eq!(first.status, MigrationStatus::Failed);
        assert_eq!(first.retry_count, 1);

        reconciler
            .reconcile_once()
            .await
            .expect("second reconcile should complete");
        let second = ledger_record_for(&ledger.list().await.expect("ledger should load"), 1)
            .expect("record should exist");
        assert_eq!(second.status, MigrationStatus::Failed);
        assert_eq!(second.retry_count, 2);
    }

    fn build_reconciler(registry: Arc<MigrationRegistry>, ledger: Arc<TestLedger>) -> Reconciler {
        build_reconciler_with_deps(
            registry,
            ledger,
            Arc::new(TestKvStore::default()),
            Arc::new(TestLock::default()),
        )
    }

    fn build_reconciler_with_deps(
        registry: Arc<MigrationRegistry>,
        ledger: Arc<TestLedger>,
        kv: Arc<TestKvStore>,
        lock: Arc<TestLock>,
    ) -> Reconciler {
        let settings = HashMap::new();
        let clickhouse = ArrowClickHouseClient::new(
            "http://localhost:8123",
            "default",
            "default",
            None,
            &settings,
        );
        Reconciler::new(
            registry,
            ledger,
            lock,
            kv,
            clickhouse,
            ReconcilerConfig {
                max_retries: 2,
                ..Default::default()
            },
        )
    }

    fn record(
        migration: &dyn Migration,
        status: MigrationStatus,
        error_message: Option<String>,
        retry_count: u32,
    ) -> LedgerMigrationRecord {
        LedgerMigrationRecord {
            version: migration.version(),
            name: migration.name().to_string(),
            migration_type: migration.migration_type(),
            status,
            started_at: Some(Utc::now()),
            completed_at: matches!(status, MigrationStatus::Completed | MigrationStatus::Failed)
                .then(Utc::now),
            error_message,
            retry_count,
        }
    }
}
