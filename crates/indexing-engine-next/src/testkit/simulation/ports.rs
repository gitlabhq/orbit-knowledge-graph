use std::sync::Arc;
use std::time::SystemTime;

use anyhow::anyhow;
use async_trait::async_trait;
use rand::RngExt;

use crate::port::{
    Clock, Delivery, InFlight, LeaseStore, LeaseWrite, MarkerEntry, MarkerStore, MarkerWrite,
    Ports, ProtocolLog, TaskMessage, TaskStream, WorkerKey,
};
use crate::protocol::{Lease, LeaseEntry, ProtocolEvent, Revision};

use super::world::{Actor, SharedWorld, StepKind};

pub(super) struct SimulatedClock {
    world: SharedWorld,
    actor: Actor,
}

struct SimulatedStream {
    world: SharedWorld,
    actor: Actor,
}

struct SimulatedInFlight {
    world: SharedWorld,
    actor: Actor,
    message: usize,
    consumer: usize,
}

struct SimulatedLeases {
    world: SharedWorld,
    actor: Actor,
}

struct SimulatedMarkers {
    world: SharedWorld,
    actor: Actor,
}

struct SimulatedLog {
    world: SharedWorld,
    actor: Actor,
}

pub(super) fn build_ports(world: &SharedWorld, actor: Actor) -> Ports {
    Ports {
        clock: Arc::new(SimulatedClock::new(world, actor)),
        tasks: Arc::new(SimulatedStream {
            world: world.clone(),
            actor,
        }),
        leases: Arc::new(SimulatedLeases {
            world: world.clone(),
            actor,
        }),
        markers: Arc::new(SimulatedMarkers {
            world: world.clone(),
            actor,
        }),
        log: Arc::new(SimulatedLog {
            world: world.clone(),
            actor,
        }),
    }
}

impl SimulatedClock {
    pub(super) fn new(world: &SharedWorld, actor: Actor) -> Self {
        Self {
            world: world.clone(),
            actor,
        }
    }
}

#[async_trait]
impl Clock for SimulatedClock {
    async fn now(&self) -> SystemTime {
        self.world.begin_step(self.actor, StepKind::Any).await
    }

    async fn sleep_until(&self, local: SystemTime) {
        self.world.enter(self.actor).await;
        let wake_at = self.world.lock().instant_at(self.actor, local);
        tokio::time::sleep_until(wake_at).await;
    }
}

#[async_trait]
impl TaskStream for SimulatedStream {
    async fn publish(&self, message: TaskMessage) -> anyhow::Result<()> {
        self.world.enter(self.actor).await;
        {
            let mut world = self.world.lock();
            if world.take_planner_crash() {
                return Err(anyhow!("the planner crashed before it published the task"));
            }
            world.publish(&message);
        }
        self.world.notify_changed();
        Ok(())
    }

    async fn pull(&self, _lane_id: &str, max: usize) -> anyhow::Result<Vec<Delivery>> {
        self.world.enter(self.actor).await;
        let Actor::Process {
            index: consumer, ..
        } = self.actor
        else {
            return Ok(Vec::new());
        };
        let mut world = self.world.lock();
        let mut ready: Vec<usize> = world.broker.ready_messages().collect();
        let mut deliveries = Vec::new();
        while deliveries.len() < max && !ready.is_empty() {
            let pick = world.rng.random_range(0..ready.len());
            let message = ready.remove(pick);
            let Some(payload) = world.deliver(consumer, message) else {
                return Err(anyhow!("the broker holds a task that nobody published"));
            };
            deliveries.push(Delivery {
                message: payload,
                handle: Box::new(SimulatedInFlight {
                    world: self.world.clone(),
                    actor: self.actor,
                    message,
                    consumer,
                }),
            });
        }
        Ok(deliveries)
    }

    async fn wait_for_ready(&self, _lane_id: &str) {
        self.world.enter(self.actor).await;
        self.world.wait_for_ready_message().await;
    }
}

#[async_trait]
impl InFlight for SimulatedInFlight {
    async fn acknowledge(&self) -> anyhow::Result<()> {
        self.world.enter(self.actor).await;
        self.world.lock().broker.acknowledge(self.message);
        Ok(())
    }

    async fn acknowledge_progress(&self) -> anyhow::Result<()> {
        self.world.enter(self.actor).await;
        let mut world = self.world.lock();
        let now = world.now_tick();
        world
            .broker
            .acknowledge_progress(self.message, self.consumer, now);
        Ok(())
    }
}

#[async_trait]
impl LeaseStore for SimulatedLeases {
    async fn read(&self, _key: &WorkerKey) -> anyhow::Result<LeaseEntry> {
        self.world.enter_inside_step(self.actor).await;
        let world = self.world.lock();
        Ok(world.lease.entry(world.nats_generation))
    }

    async fn write(
        &self,
        _key: &WorkerKey,
        expected: Revision,
        lease: Option<Lease>,
    ) -> anyhow::Result<LeaseWrite> {
        self.world.enter_inside_step(self.actor).await;
        let mut world = self.world.lock();
        if let Actor::Process { index, .. } = self.actor
            && world.take_renewal_failure(index, lease.as_ref())
        {
            return Err(anyhow!("the renewal did not reach the key-value store"));
        }
        let Some(generation) = world.nats_generation else {
            world.count("LeaseConflict");
            return Ok(LeaseWrite::Conflict);
        };
        match world.lease.put(expected, lease) {
            Ok(revision) => Ok(LeaseWrite::Written {
                revision,
                generation,
            }),
            Err(_) => {
                world.count("LeaseConflict");
                Ok(LeaseWrite::Conflict)
            }
        }
    }
}

#[async_trait]
impl MarkerStore for SimulatedMarkers {
    async fn read(&self, _key: &WorkerKey) -> anyhow::Result<MarkerEntry> {
        self.world.enter_inside_step(self.actor).await;
        let world = self.world.lock();
        Ok(MarkerEntry {
            pending: world.marker.get().cloned(),
            revision: world.marker.revision(),
        })
    }

    async fn write(
        &self,
        _key: &WorkerKey,
        expected: Revision,
        pending: Option<TaskMessage>,
    ) -> anyhow::Result<MarkerWrite> {
        self.world.enter_inside_step(self.actor).await;
        let mut world = self.world.lock();
        match world.marker.put(expected, pending) {
            Ok(_) => Ok(MarkerWrite::Written),
            Err(_) => {
                world.count("MarkerConflict");
                Ok(MarkerWrite::Conflict)
            }
        }
    }
}

impl ProtocolLog for SimulatedLog {
    fn record(&self, at: SystemTime, event: ProtocolEvent) {
        let mut world = self.world.lock();
        if world.admit_call(self.actor) {
            world.apply_event(self.actor, at, event);
        }
    }
}
