use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::event::{EngineEvent, TaskEvent, TaskIdentity, TaskStep};
use crate::lane::{Lane, LaneId};
use crate::payload::{Key, TaskInput, Tenant};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TaskId(Uuid);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskMessage {
    pub task: TaskId,
    pub tenant: Tenant,
    pub worker: String,
    pub key: Key,
    pub lane: Lane,
    #[serde(with = "embedded_json")]
    pub input: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct WorkerKey {
    pub worker: String,
    pub key: Key,
}

pub struct Delivery {
    pub message: TaskMessage,
    pub in_flight: Box<dyn InFlight>,
}

#[async_trait]
pub trait TaskQueue: Send + Sync {
    async fn publish(&self, message: TaskMessage) -> anyhow::Result<()>;
    async fn pull(&self, lane: &LaneId, max: usize) -> anyhow::Result<Vec<Delivery>>;
    async fn wait_for_ready(&self, lane: &LaneId);
    async fn wait_until_closed(&self) -> anyhow::Error;
}

#[async_trait]
pub trait InFlight: Send + Sync {
    async fn ack(&self) -> anyhow::Result<()>;
    async fn ack_progress(&self) -> anyhow::Result<()>;
    async fn nack(&self, delay: Duration) -> anyhow::Result<()>;
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

mod embedded_json {
    use serde::de::Error as _;
    use serde::ser::Error as _;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use serde_json::Value;

    pub(super) fn serialize<S: Serializer>(input: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        let value: Value = serde_json::from_slice(input).map_err(S::Error::custom)?;
        value.serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<u8>, D::Error> {
        let value = Value::deserialize(deserializer)?;
        serde_json::to_vec(&value).map_err(D::Error::custom)
    }
}
