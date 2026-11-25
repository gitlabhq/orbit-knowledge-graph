use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::anyhow;
use async_trait::async_trait;
use rand::RngExt;

use crate::clock::Clock;
use crate::event::{EngineEvent, Observer};
use crate::lane::LaneId;
use crate::lease::{Lease, LeaseEntry, LeaseStore, LeaseWrite, Revision};
use crate::pending::{PendingEntry, PendingStore, PendingWrite};
use crate::ports::Ports;
use crate::task::{Delivery, InFlight, TaskMessage, TaskQueue, WorkerKey};

use super::world::{Actor, SharedWorld, StepKind, ticks_in};

struct SimulatedPorts {
    world: SharedWorld,
    actor: Actor,
}

struct SimulatedInFlight {
    world: SharedWorld,
    actor: Actor,
    message: usize,
    consumer: usize,
}

pub(super) fn build_ports(world: &SharedWorld, actor: Actor) -> Ports {
    let ports = Arc::new(SimulatedPorts {
        world: world.clone(),
        actor,
    });
    Ports {
        clock: ports.clone(),
        queue: ports.clone(),
        leases: ports.clone(),
        pending: ports.clone(),
        observer: ports,
    }
}

#[async_trait]
impl Clock for SimulatedPorts {
    async fn now(&self) -> SystemTime {
        self.world.begin_step(self.actor, StepKind::Any).await
    }

    async fn sleep(&self, duration: Duration) {
        self.world.stop_unless_allowed(self.actor).await;
        tokio::time::sleep(duration).await;
    }
}

#[async_trait]
impl TaskQueue for SimulatedPorts {
    async fn publish(&self, message: TaskMessage) -> anyhow::Result<()> {
        self.world.stop_unless_allowed(self.actor).await;
        {
            let mut world = self.world.lock();
            if world.consume_planner_crash() {
                return Err(anyhow!("the planner crashed before it published the task"));
            }
            world.publish(&message);
        }
        self.world.notify_changed();
        Ok(())
    }

    async fn pull(&self, _lane: &LaneId, max: usize) -> anyhow::Result<Vec<Delivery>> {
        self.world.stop_unless_allowed(self.actor).await;
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
                in_flight: Box::new(SimulatedInFlight {
                    world: self.world.clone(),
                    actor: self.actor,
                    message,
                    consumer,
                }),
            });
        }
        Ok(deliveries)
    }

    async fn wait_for_ready(&self, _lane: &LaneId) {
        self.world.stop_unless_allowed(self.actor).await;
        self.world.wait_for_ready_message().await;
    }
}

#[async_trait]
impl InFlight for SimulatedInFlight {
    async fn ack(&self) -> anyhow::Result<()> {
        self.world.stop_unless_allowed(self.actor).await;
        self.world.lock().broker.ack(self.message);
        Ok(())
    }

    async fn ack_progress(&self) -> anyhow::Result<()> {
        self.world.stop_unless_allowed(self.actor).await;
        let mut world = self.world.lock();
        let now = world.now_tick();
        world.broker.ack_progress(self.message, self.consumer, now);
        Ok(())
    }

    async fn nack(&self, delay: Duration) -> anyhow::Result<()> {
        self.world.stop_unless_allowed(self.actor).await;
        let mut world = self.world.lock();
        let redeliver_at = world.now_tick() + ticks_in(delay);
        world.broker.nack(self.message, redeliver_at);
        Ok(())
    }
}

#[async_trait]
impl LeaseStore for SimulatedPorts {
    async fn read(&self, _key: &WorkerKey) -> anyhow::Result<LeaseEntry> {
        self.world.interleave_inside_step(self.actor).await;
        let world = self.world.lock();
        Ok(world.lease.entry())
    }

    async fn write(
        &self,
        _key: &WorkerKey,
        expected: Revision,
        lease: Option<&Lease>,
    ) -> anyhow::Result<LeaseWrite> {
        self.world.interleave_inside_step(self.actor).await;
        let mut world = self.world.lock();
        if let Actor::Process { index, .. } = self.actor
            && lease.is_some()
            && world.consume_renewal_failure(index)
        {
            return Err(anyhow!("the renewal did not reach the key-value store"));
        }
        match world.lease.put(expected, lease.cloned()) {
            Ok(revision) => Ok(LeaseWrite::Written(revision)),
            Err(_) => {
                world.count("LeaseConflict");
                Ok(LeaseWrite::Conflict)
            }
        }
    }
}

#[async_trait]
impl PendingStore for SimulatedPorts {
    async fn read(&self, _key: &WorkerKey) -> anyhow::Result<PendingEntry> {
        self.world.interleave_inside_step(self.actor).await;
        let world = self.world.lock();
        Ok(PendingEntry {
            task: world.pending.get().cloned(),
            revision: world.pending.revision(),
        })
    }

    async fn write(
        &self,
        _key: &WorkerKey,
        expected: Revision,
        task: Option<&TaskMessage>,
    ) -> anyhow::Result<PendingWrite> {
        self.world.interleave_inside_step(self.actor).await;
        let mut world = self.world.lock();
        match world.pending.put(expected, task.cloned()) {
            Ok(_) => Ok(PendingWrite::Written),
            Err(_) => {
                world.count("PendingConflict");
                Ok(PendingWrite::Conflict)
            }
        }
    }
}

impl Observer for SimulatedPorts {
    fn observe(&self, event: &EngineEvent) {
        match event {
            EngineEvent::Task(event) => {
                let mut world = self.world.lock();
                if world.allow_port_call(self.actor) {
                    world.apply_task_event(self.actor, event);
                }
            }
            EngineEvent::Lane(_) => {}
        }
    }
}
