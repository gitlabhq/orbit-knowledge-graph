use anyhow::bail;

use crate::event::ProtocolEvent;
use crate::pending::PendingWrite;
use crate::ports::Ports;
use crate::task::{TaskId, TaskMessage};

pub(crate) async fn submit_task(ports: &Ports, message: TaskMessage) -> anyhow::Result<TaskId> {
    let key = message.worker_key();
    let now = ports.clock.now().await;
    let entry = ports.pending.read(&key).await?;
    let (pending, found_pending) = match entry.task {
        Some(already_pending) => (already_pending, true),
        None => {
            let written = ports
                .pending
                .write(&key, entry.revision, Some(&message))
                .await?;
            if let PendingWrite::Conflict = written {
                bail!("the pending task changed during the submit, so the trigger submits again");
            }
            (message, false)
        }
    };
    ports.log.record(
        now,
        ProtocolEvent::Submitted {
            task: pending.task,
            found_pending,
        },
    );

    let task = pending.task;
    let now = ports.clock.now().await;
    ports.queue.publish(pending).await?;
    ports.log.record(now, ProtocolEvent::Published(task));
    Ok(task)
}
