use anyhow::bail;

use crate::builder::Engine;
use crate::event::{Observer, TaskStep};
use crate::lane::Lane;
use crate::payload::{TaskInput, Tenant};
use crate::pending::PendingTask;
use crate::ports::Ports;
use crate::task::{TaskId, TaskMessage};
use crate::worker::Worker;

impl Engine {
    pub async fn submit<W: Worker>(
        &self,
        ports: &Ports,
        tenant: Tenant,
        input: &W::Input,
        lane: Lane,
    ) -> anyhow::Result<TaskId> {
        if self.registry.find_worker(W::NAME).is_none() {
            bail!("no module registered the worker {}", W::NAME);
        }
        let message = TaskMessage::from_input(tenant, TaskInput::encode::<W>(input)?, lane);
        submit_task(ports, self.build_observer().as_ref(), message).await
    }
}

pub(crate) async fn submit_task(
    ports: &Ports,
    observer: &dyn Observer,
    message: TaskMessage,
) -> anyhow::Result<TaskId> {
    let now = ports.clock.now().await;
    let (to_publish, already_pending) = match ports.pending.add_unless_pending(message).await? {
        PendingTask::Added(message) => (message, false),
        PendingTask::AlreadyPending(old) => (old, true),
    };
    let submitted = TaskStep::Submitted { already_pending };
    observer.observe(&to_publish.build_event(now, submitted));

    let task = to_publish.task;
    let now = ports.clock.now().await;
    let published = to_publish.build_event(now, TaskStep::Published);
    ports.queue.publish(to_publish).await?;
    observer.observe(&published);
    Ok(task)
}
