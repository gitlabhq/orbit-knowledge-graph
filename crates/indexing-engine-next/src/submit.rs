use crate::event::TaskStep;
use crate::pending::PendingTask;
use crate::ports::Ports;
use crate::task::{TaskId, TaskMessage};

pub(crate) async fn submit_task(ports: &Ports, message: TaskMessage) -> anyhow::Result<TaskId> {
    let now = ports.clock.now().await;
    let (pending, found_pending) = match ports.pending.add_unless_pending(message).await? {
        PendingTask::Added(message) => (message, false),
        PendingTask::AlreadyPending(already_pending) => (already_pending, true),
    };
    let submitted = TaskStep::Submitted { found_pending };
    ports.observer.observe(&pending.build_event(now, submitted));

    let task = pending.task;
    let now = ports.clock.now().await;
    let published = pending.build_event(now, TaskStep::Published);
    ports.queue.publish(pending).await?;
    ports.observer.observe(&published);
    Ok(task)
}
