use crate::port::{MarkerWrite, Ports, TaskMessage};
use crate::protocol::{ProtocolEvent, Publish, TaskId, decide_submit};

pub(crate) async fn submit_task(ports: &Ports, message: TaskMessage) -> anyhow::Result<TaskId> {
    let pending = claim_marker(ports, message).await?;

    let now = ports.clock.now().await;
    ports.tasks.publish(pending.clone()).await?;
    ports
        .log
        .record(now, ProtocolEvent::Published(pending.task));
    Ok(pending.task)
}

async fn claim_marker(ports: &Ports, message: TaskMessage) -> anyhow::Result<TaskMessage> {
    let key = message.worker_key();
    loop {
        let now = ports.clock.now().await;
        let entry = ports.markers.read(&key).await?;
        match decide_submit(entry.pending, message.clone()) {
            Publish::Pending(found) => {
                ports.log.record(
                    now,
                    ProtocolEvent::Submitted {
                        task: found.task,
                        found_marker: true,
                    },
                );
                return Ok(found);
            }
            Publish::New(new) => {
                match ports
                    .markers
                    .write(&key, entry.revision, Some(new.clone()))
                    .await?
                {
                    MarkerWrite::Written => {
                        ports.log.record(
                            now,
                            ProtocolEvent::Submitted {
                                task: new.task,
                                found_marker: false,
                            },
                        );
                        return Ok(new);
                    }
                    MarkerWrite::Conflict => continue,
                }
            }
        }
    }
}
