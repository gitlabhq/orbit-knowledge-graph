use anyhow::bail;

use crate::port::{MarkerWrite, Ports, TaskMessage};
use crate::protocol::{ProtocolEvent, TaskId};

pub(crate) async fn submit_task(ports: &Ports, message: TaskMessage) -> anyhow::Result<TaskId> {
    let key = message.worker_key();
    let now = ports.clock.now().await;
    let entry = ports.markers.read(&key).await?;
    let (pending, found_marker) = match entry.pending {
        Some(already_pending) => (already_pending, true),
        None => {
            let written = ports
                .markers
                .write(&key, entry.revision, Some(message.clone()))
                .await?;
            if let MarkerWrite::Conflict = written {
                bail!("the marker changed during the submit, so the trigger submits again");
            }
            (message, false)
        }
    };
    ports.log.record(
        now,
        ProtocolEvent::Submitted {
            task: pending.task,
            found_marker,
        },
    );

    let now = ports.clock.now().await;
    ports.tasks.publish(pending.clone()).await?;
    ports
        .log
        .record(now, ProtocolEvent::Published(pending.task));
    Ok(pending.task)
}
