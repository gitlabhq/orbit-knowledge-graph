use async_trait::async_trait;

use crate::lease::Revision;
use crate::task::{TaskMessage, WorkerKey};

#[async_trait]
pub(crate) trait PendingStore: Send + Sync {
    async fn read(&self, key: &WorkerKey) -> anyhow::Result<PendingEntry>;
    async fn write(
        &self,
        key: &WorkerKey,
        expected: Revision,
        task: Option<&TaskMessage>,
    ) -> anyhow::Result<PendingWrite>;
}

pub(crate) struct PendingEntry {
    pub(crate) task: Option<TaskMessage>,
    pub(crate) revision: Revision,
}

pub(crate) enum PendingWrite {
    Written,
    Conflict,
}
