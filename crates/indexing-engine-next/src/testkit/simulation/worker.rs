use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::payload::Key;
use crate::worker::{Outcome, TaskContext, TaskError, Worker};

use super::world::{Actor, SharedWorld, StepKind, TICK};

#[derive(Serialize, Deserialize)]
pub(super) struct WriteInput;

pub(super) struct SimulatedWrite {
    store: SimulatedStore,
}

struct SimulatedStore {
    world: SharedWorld,
    actor: Actor,
    process: usize,
}

impl SimulatedWrite {
    pub(super) fn new(world: &SharedWorld, actor: Actor, process: usize) -> Self {
        Self {
            store: SimulatedStore {
                world: world.clone(),
                actor,
                process,
            },
        }
    }
}

#[async_trait]
impl Worker for SimulatedWrite {
    const NAME: &'static str = "simulation.write";
    const EXCLUSIVE: bool = true;
    type Input = WriteInput;
    type Output = ();

    fn key(_input: &WriteInput) -> Key {
        Key::new("simulation", 1)
    }

    async fn run(&self, _ctx: &TaskContext, _input: WriteInput) -> Result<Outcome<()>, TaskError> {
        let work_ticks = self.store.draw_work_ticks();
        tokio::time::sleep(TICK * work_ticks).await;
        self.store.finish_work().await;
        Ok(Outcome::done(()))
    }
}

impl SimulatedStore {
    fn draw_work_ticks(&self) -> u32 {
        let work = self.world.lock().draw_number(0..=2);
        u32::try_from(work).unwrap_or(0)
    }

    async fn finish_work(&self) {
        self.world.begin_step(self.actor, StepKind::Work).await;
        self.world.lock().finish_work(self.process);
    }
}
