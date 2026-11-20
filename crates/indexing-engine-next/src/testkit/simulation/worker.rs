use anyhow::anyhow;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::payload::Key;
use crate::protocol::Fence;
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

    async fn run(&self, ctx: &TaskContext, _input: WriteInput) -> Result<Outcome<()>, TaskError> {
        let Some(fence) = ctx.fence() else {
            return Err(TaskError::permanent(anyhow!(
                "an exclusive run got no fence"
            )));
        };
        let (work_ticks, write_ticks) = self.store.draw_durations();

        tokio::time::sleep(TICK * work_ticks).await;
        self.store.begin_write().await;
        tokio::time::sleep(TICK * write_ticks).await;
        if self.store.commit_write(fence).await {
            Ok(Outcome::done(()))
        } else {
            Err(TaskError::from(anyhow!("the database kept a newer write")))
        }
    }
}

impl SimulatedStore {
    fn draw_durations(&self) -> (u32, u32) {
        let mut world = self.world.lock();
        let work = world.draw(0..=2);
        let write = world.draw(0..=1);
        (
            u32::try_from(work).unwrap_or(0),
            u32::try_from(write).unwrap_or(0),
        )
    }

    async fn begin_write(&self) {
        self.world.begin_step(self.actor, StepKind::Any).await;
        self.world.lock().begin_write(self.process);
    }

    async fn commit_write(&self, fence: Fence) -> bool {
        self.world.begin_step(self.actor, StepKind::Commit).await;
        self.world.lock().commit_write(self.process, fence)
    }
}
