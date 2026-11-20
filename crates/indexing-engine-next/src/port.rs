use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;

use crate::lane::Lane;
use crate::payload::{Key, TaskInput, Tenant};
use crate::protocol::{Fence, Lease, LeaseEntry, ProtocolEvent, Revision, TaskId};

#[derive(Clone)]
pub(crate) struct Ports {
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) tasks: Arc<dyn TaskStream>,
    pub(crate) leases: Arc<dyn LeaseStore>,
    pub(crate) markers: Arc<dyn MarkerStore>,
    pub(crate) log: Arc<dyn ProtocolLog>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct WorkerKey {
    pub(crate) worker: String,
    pub(crate) key: Key,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TaskMessage {
    pub(crate) task: TaskId,
    pub(crate) tenant: Tenant,
    pub(crate) worker: String,
    pub(crate) key: Key,
    pub(crate) lane: Lane,
    pub(crate) input: Vec<u8>,
}

pub(crate) struct Delivery {
    pub(crate) message: TaskMessage,
    pub(crate) handle: Box<dyn InFlight>,
}

pub(crate) enum LeaseWrite {
    Written(Fence),
    Conflict,
}

pub(crate) struct MarkerEntry {
    pub(crate) pending: Option<TaskMessage>,
    pub(crate) revision: Revision,
}

pub(crate) enum MarkerWrite {
    Written,
    Conflict,
}

#[async_trait]
pub(crate) trait Clock: Send + Sync {
    async fn now(&self) -> SystemTime;
    async fn sleep_until(&self, local: SystemTime);
}

#[async_trait]
pub(crate) trait TaskStream: Send + Sync {
    async fn publish(&self, message: TaskMessage) -> anyhow::Result<()>;
    async fn pull(&self, lane_id: &str, max: usize) -> anyhow::Result<Vec<Delivery>>;
    async fn wait_for_ready(&self, lane_id: &str);
}

#[async_trait]
pub(crate) trait InFlight: Send + Sync {
    async fn acknowledge(&self) -> anyhow::Result<()>;
    async fn acknowledge_progress(&self) -> anyhow::Result<()>;
}

#[async_trait]
pub(crate) trait LeaseStore: Send + Sync {
    async fn read(&self, key: &WorkerKey) -> anyhow::Result<LeaseEntry>;
    async fn write(
        &self,
        key: &WorkerKey,
        expected: Revision,
        lease: Option<Lease>,
    ) -> anyhow::Result<LeaseWrite>;
}

#[async_trait]
pub(crate) trait MarkerStore: Send + Sync {
    async fn read(&self, key: &WorkerKey) -> anyhow::Result<MarkerEntry>;
    async fn write(
        &self,
        key: &WorkerKey,
        expected: Revision,
        pending: Option<TaskMessage>,
    ) -> anyhow::Result<MarkerWrite>;
}

pub(crate) trait ProtocolLog: Send + Sync {
    fn record(&self, at: SystemTime, event: ProtocolEvent);
}

impl TaskMessage {
    pub(crate) fn from_input(tenant: Tenant, input: TaskInput, lane: Lane) -> Self {
        Self {
            task: TaskId::new(uuid::Uuid::now_v7()),
            tenant,
            worker: input.worker.to_owned(),
            key: input.key,
            lane,
            input: input.bytes,
        }
    }

    pub(crate) fn worker_key(&self) -> WorkerKey {
        WorkerKey {
            worker: self.worker.clone(),
            key: self.key.clone(),
        }
    }
}
