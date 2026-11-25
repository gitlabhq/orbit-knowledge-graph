use async_trait::async_trait;

use crate::task::WorkerKey;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Revision(pub u64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Versioned<V> {
    pub value: Option<V>,
    pub revision: Revision,
}

pub enum WriteOutcome {
    Written(Revision),
    Conflict,
}

pub enum DeleteOutcome {
    Deleted,
    Conflict,
}

#[async_trait]
pub trait VersionedStore<V>: Send + Sync {
    async fn read(&self, key: &WorkerKey) -> anyhow::Result<Versioned<V>>;
    async fn write(
        &self,
        key: &WorkerKey,
        expected: Revision,
        value: &V,
    ) -> anyhow::Result<WriteOutcome>;
    async fn delete(&self, key: &WorkerKey, expected: Revision) -> anyhow::Result<DeleteOutcome>;
}
