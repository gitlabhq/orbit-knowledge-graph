mod queue;
mod resources;
mod store;

use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream;

use crate::clock::SystemClock;
use crate::lease::Lease;
use crate::ports::Ports;
use crate::task::TaskMessage;
use queue::NatsTaskQueue;
use resources::{OpenResources, open_resources};
use store::NatsVersionedStore;

#[derive(Clone, Debug)]
pub struct NatsSettings {
    pub replicas: usize,
    pub ack_wait: Duration,
    pub idle_poll: Duration,
    pub stream_check: Duration,
}

pub struct NatsTransport {
    ports: Ports,
}

impl Default for NatsSettings {
    fn default() -> Self {
        Self {
            replicas: 1,
            ack_wait: Duration::from_secs(60),
            idle_poll: Duration::from_secs(1),
            stream_check: Duration::from_secs(5),
        }
    }
}

impl NatsTransport {
    pub async fn open(client: async_nats::Client, settings: NatsSettings) -> anyhow::Result<Self> {
        let jetstream = jetstream::new(client);
        let OpenResources { stream, bucket } = open_resources(&jetstream, &settings).await?;
        let queue = NatsTaskQueue::new(jetstream, stream, &settings);
        let ports = Ports {
            clock: Arc::new(SystemClock),
            queue: Arc::new(queue),
            leases: Arc::new(NatsVersionedStore::<Lease>::new(bucket.clone(), "lease")),
            pending: Arc::new(NatsVersionedStore::<TaskMessage>::new(bucket, "pending")),
        };
        Ok(Self { ports })
    }

    pub fn ports(&self) -> &Ports {
        &self.ports
    }
}
