use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use uuid::Uuid;

use crate::event::{EngineEvent, TaskEvent, TaskIdentity, TaskStep};
use crate::lane::{Lane, LaneId};
use crate::payload::{Key, TaskInput, Tenant};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskId(Uuid);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TaskMessage {
    pub(crate) task: TaskId,
    pub(crate) tenant: Tenant,
    pub(crate) worker: String,
    pub(crate) key: Key,
    pub(crate) lane: Lane,
    pub(crate) input: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct WorkerKey {
    pub(crate) worker: String,
    pub(crate) key: Key,
}

pub(crate) struct Delivery {
    pub(crate) message: TaskMessage,
    pub(crate) in_flight: Box<dyn InFlight>,
}

#[async_trait]
pub(crate) trait TaskQueue: Send + Sync {
    async fn publish(&self, message: TaskMessage) -> anyhow::Result<()>;
    async fn pull(&self, lane: &LaneId, max: usize) -> anyhow::Result<Vec<Delivery>>;
    async fn wait_for_ready(&self, lane: &LaneId);
}

#[async_trait]
pub(crate) trait InFlight: Send + Sync {
    async fn acknowledge(&self) -> anyhow::Result<()>;
    async fn acknowledge_progress(&self) -> anyhow::Result<()>;
    async fn negative_acknowledge(&self, delay: Duration) -> anyhow::Result<()>;
}

impl TaskId {
    pub fn new(id: Uuid) -> Self {
        Self(id)
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl TaskMessage {
    pub(crate) fn from_input(tenant: Tenant, input: TaskInput, lane: Lane) -> Self {
        Self {
            task: TaskId::new(Uuid::now_v7()),
            tenant,
            worker: input.worker.to_owned(),
            key: input.key,
            lane,
            input: input.bytes,
        }
    }

    pub(crate) fn build_event(&self, at: SystemTime, step: TaskStep) -> EngineEvent {
        EngineEvent::Task(TaskEvent {
            at,
            task: TaskIdentity {
                id: self.task,
                tenant: self.tenant.clone(),
                worker: self.worker.clone(),
                key: self.key.clone(),
                lane: self.lane.clone(),
            },
            step,
        })
    }

    pub(crate) fn worker_key(&self) -> WorkerKey {
        WorkerKey {
            worker: self.worker.clone(),
            key: self.key.clone(),
        }
    }
}
