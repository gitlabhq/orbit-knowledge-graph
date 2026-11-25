use anyhow::bail;
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

pub(crate) enum PendingTask {
    Added(TaskMessage),
    AlreadyPending(TaskMessage),
}

impl dyn PendingStore {
    pub(crate) async fn add_unless_pending(
        &self,
        message: TaskMessage,
    ) -> anyhow::Result<PendingTask> {
        let key = message.worker_key();
        let entry = self.read(&key).await?;
        if let Some(already_pending) = entry.task {
            return Ok(PendingTask::AlreadyPending(already_pending));
        }
        let written = self.write(&key, entry.revision, Some(&message)).await?;
        if let PendingWrite::Conflict = written {
            bail!("the pending task changed during the submit, so the trigger submits again");
        }
        Ok(PendingTask::Added(message))
    }

    pub(crate) async fn clear_if_still_pending(
        &self,
        message: &TaskMessage,
    ) -> anyhow::Result<bool> {
        let key = message.worker_key();
        let entry = self.read(&key).await?;
        let still_pending = entry
            .task
            .is_some_and(|pending| pending.task == message.task);
        if !still_pending {
            return Ok(false);
        }
        let written = self.write(&key, entry.revision, None).await?;
        Ok(matches!(written, PendingWrite::Written))
    }
}
