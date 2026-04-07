use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use migration_framework::{
    KvRecord, KvStore, KvWrite, NatsMigrationLock, Reconciler, ReconcilerConfig,
    build_migration_registry,
};
use tokio_util::sync::CancellationToken;

use crate::nats::{KvEntry, KvPutOptions, KvPutResult, NatsServices};

pub async fn run_reconciler(
    nats: Arc<dyn NatsServices>,
    clickhouse: clickhouse_client::ArrowClickHouseClient,
    shutdown: CancellationToken,
) {
    let kv: Arc<dyn KvStore> = Arc::new(IndexerKvStore::new(nats));
    let reconciler = Reconciler::new(
        Arc::new(build_migration_registry()),
        Arc::new(migration_framework::MigrationLedger::new(
            clickhouse.clone(),
        )),
        Arc::new(NatsMigrationLock::new(kv.clone())),
        kv,
        clickhouse,
        ReconcilerConfig::default(),
    );

    reconciler.run(shutdown).await;
}

struct IndexerKvStore {
    nats: Arc<dyn NatsServices>,
}

impl IndexerKvStore {
    fn new(nats: Arc<dyn NatsServices>) -> Self {
        Self { nats }
    }
}

#[async_trait]
impl KvStore for IndexerKvStore {
    async fn get(&self, bucket: &str, key: &str) -> anyhow::Result<Option<KvRecord>> {
        let entry = self.nats.kv_get(bucket, key).await?;
        Ok(entry.map(kv_entry))
    }

    async fn create(
        &self,
        bucket: &str,
        key: &str,
        value: Bytes,
        ttl: Duration,
    ) -> anyhow::Result<KvWrite> {
        let result = self
            .nats
            .kv_put(bucket, key, value, KvPutOptions::create_with_ttl(ttl))
            .await?;
        Ok(kv_write(result))
    }

    async fn update(
        &self,
        bucket: &str,
        key: &str,
        value: Bytes,
        expected_revision: u64,
    ) -> anyhow::Result<KvWrite> {
        let result = self
            .nats
            .kv_put(
                bucket,
                key,
                value,
                KvPutOptions::update_revision(expected_revision),
            )
            .await?;
        Ok(kv_write(result))
    }

    async fn put(&self, bucket: &str, key: &str, value: Bytes) -> anyhow::Result<KvWrite> {
        let result = self
            .nats
            .kv_put(bucket, key, value, KvPutOptions::default())
            .await?;
        Ok(kv_write(result))
    }

    async fn delete(&self, bucket: &str, key: &str) -> anyhow::Result<()> {
        self.nats.kv_delete(bucket, key).await?;
        Ok(())
    }
}

fn kv_entry(entry: KvEntry) -> KvRecord {
    KvRecord {
        revision: entry.revision,
        value: entry.value,
    }
}

fn kv_write(result: KvPutResult) -> KvWrite {
    match result {
        KvPutResult::Success(revision) => KvWrite::Written { revision },
        KvPutResult::AlreadyExists => KvWrite::AlreadyExists,
        KvPutResult::RevisionMismatch => KvWrite::RevisionMismatch,
    }
}
