use std::time::Duration;

use super::FaultBudget;
use super::ports::build_ports;
use super::world::{ACK_WAIT_TICKS, Actor, SharedWorld, StepOrder, TICK, ticks_in};
use crate::ports::Ports;

pub struct SimulatedNats {
    world: SharedWorld,
}

impl Default for SimulatedNats {
    fn default() -> Self {
        let no_faults = FaultBudget {
            stalls: 0,
            crashes: 0,
            wipes: 0,
            duplicates: 0,
            losses: 0,
        };
        Self {
            world: SharedWorld::new(0, 1, 0, no_faults, StepOrder::Atomic),
        }
    }
}

impl SimulatedNats {
    pub fn ports(&self) -> Ports {
        build_ports(
            &self.world,
            Actor::Process {
                index: 0,
                incarnation: 0,
            },
        )
    }

    pub fn ack_wait(&self) -> Duration {
        TICK * u32::try_from(ACK_WAIT_TICKS).unwrap_or(u32::MAX)
    }

    pub fn wipe(&self) {
        let mut world = self.world.lock();
        world.lease.wipe();
        world.pending.wipe();
        world.broker.wipe();
    }

    pub async fn wait_whole_ticks_in(&self, duration: Duration) {
        let target = {
            let world = self.world.lock();
            world.start_of_tick(world.now_tick() + ticks_in(duration))
        };
        tokio::time::sleep_until(target).await;
        self.world.lock().redeliver_expired_messages();
    }
}
