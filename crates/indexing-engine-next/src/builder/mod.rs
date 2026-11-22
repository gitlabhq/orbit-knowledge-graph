mod registry;
mod validate;

use std::sync::Arc;

use crate::config::EngineConfig;
use crate::planner::Planner;
use crate::reconciler::Reconciler;
use crate::worker::Worker;

pub use validate::BuildError;

pub(crate) use registry::{ErasedPlanner, Registry};

use registry::{PlannerEntry, ReconcilerEntry, WorkerEntry};

pub struct Engine {
    pub(crate) registry: Arc<Registry>,
    pub(crate) config: EngineConfig,
}

#[derive(Clone, Default)]
pub struct EngineBuilder {
    config: EngineConfig,
    workers: Vec<WorkerEntry>,
    planners: Vec<PlannerEntry>,
    reconcilers: Vec<ReconcilerEntry>,
    pools: Vec<&'static str>,
}

impl Engine {
    pub fn builder() -> EngineBuilder {
        EngineBuilder::default()
    }
}

impl EngineBuilder {
    pub fn config(mut self, config: EngineConfig) -> Self {
        self.config = config;
        self
    }

    pub fn module(mut self, register: impl FnOnce(&mut EngineBuilder)) -> Self {
        register(&mut self);
        self
    }

    pub fn worker<W: Worker>(&mut self, worker: W) -> &mut Self {
        self.workers.push(WorkerEntry::of(worker));
        self
    }

    pub fn planner<P: Planner>(&mut self, planner: P) -> &mut Self {
        self.planners.push(PlannerEntry::of(planner));
        self
    }

    pub fn reconciler<R: Reconciler>(&mut self, reconciler: R) -> &mut Self {
        self.reconcilers.push(ReconcilerEntry::of(reconciler));
        self
    }

    pub fn pool(&mut self, name: &'static str) -> &mut Self {
        self.pools.push(name);
        self
    }

    pub fn build(self) -> Result<Engine, BuildError> {
        validate::reject_invalid_or_duplicate_names(&self)?;
        validate::reject_unknown_lanes(&self)?;
        validate::reject_unregistered_pools(&self)?;
        validate::reject_done_triggers_on_unknown_workers(&self)?;
        validate::reject_tables_with_two_writers(&self)?;
        let registry =
            Registry::from_entries(self.workers, self.planners, self.reconcilers, self.pools);
        Ok(Engine {
            registry: Arc::new(registry),
            config: self.config,
        })
    }
}
