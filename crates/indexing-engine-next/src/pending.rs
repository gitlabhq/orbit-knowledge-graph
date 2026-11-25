use anyhow::bail;

use crate::task::TaskMessage;
use crate::versioned::{DeleteOutcome, VersionedStore, WriteOutcome};

pub(crate) enum PendingTask {
    Added(TaskMessage),
    AlreadyPending(TaskMessage),
}

impl dyn VersionedStore<TaskMessage> {
    pub(crate) async fn add_unless_pending(
        &self,
        message: TaskMessage,
    ) -> anyhow::Result<PendingTask> {
        let key = message.worker_key();
        let entry = self.read(&key).await?;
        if let Some(already_pending) = entry.value {
            return Ok(PendingTask::AlreadyPending(already_pending));
        }
        let written = self.write(&key, entry.revision, &message).await?;
        if let WriteOutcome::Conflict = written {
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
            .value
            .is_some_and(|pending| pending.task == message.task);
        if !still_pending {
            return Ok(false);
        }
        let deleted = self.delete(&key, entry.revision).await?;
        Ok(matches!(deleted, DeleteOutcome::Deleted))
    }
}
