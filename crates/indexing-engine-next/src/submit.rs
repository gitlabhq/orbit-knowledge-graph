use crate::event::TaskStep;
use crate::pending::PendingTask;
use crate::ports::Ports;
use crate::task::{TaskId, TaskMessage};

pub(crate) async fn submit_task(ports: &Ports, message: TaskMessage) -> anyhow::Result<TaskId> {
    let now = ports.clock.now().await;
    let (to_publish, already_pending) = match ports.pending.add_unless_pending(message).await? {
        PendingTask::Added(message) => (message, false),
        PendingTask::AlreadyPending(old) => (old, true),
    };
    let submitted = TaskStep::Submitted { already_pending };
    ports
        .observer
        .observe(&to_publish.build_event(now, submitted));

    let task = to_publish.task;
    let now = ports.clock.now().await;
    let published = to_publish.build_event(now, TaskStep::Published);
    ports.queue.publish(to_publish).await?;
    ports.observer.observe(&published);
    Ok(task)
}
