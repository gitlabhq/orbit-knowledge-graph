use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;

use crate::planner::{Plan, PlanContext, PlanError, Planner};
use crate::reconciler::{ReconcileContext, ReconcileError, Reconciler};
use crate::trigger::{Fired, Trigger};
use crate::worker::{EncodedOutcome, TaskContext, TaskError, Worker};

pub(crate) struct Registry {
    workers: BTreeMap<&'static str, WorkerEntry>,
    planners: BTreeMap<&'static str, PlannerEntry>,
    reconcilers: BTreeMap<&'static str, ReconcilerEntry>,
    pools: BTreeSet<&'static str>,
}

#[derive(Clone)]
pub(crate) struct WorkerEntry {
    pub(crate) name: &'static str,
    exclusive: bool,
    worker: Arc<dyn ErasedWorker>,
}

#[derive(Clone)]
pub(crate) struct PlannerEntry {
    pub(crate) name: &'static str,
    pub(crate) triggers: Vec<Trigger>,
    planner: Arc<dyn ErasedPlanner>,
}

#[derive(Clone)]
pub(crate) struct ReconcilerEntry {
    pub(crate) name: &'static str,
    pub(crate) writes: &'static [&'static str],
    reconciler: Arc<dyn ErasedReconciler>,
}

#[async_trait]
pub(crate) trait ErasedWorker: Send + Sync {
    async fn run(&self, ctx: &TaskContext, input: &[u8]) -> Result<EncodedOutcome, TaskError>;
}

#[async_trait]
pub(crate) trait ErasedPlanner: Send + Sync {
    async fn plan(&self, fired: Fired) -> Result<Plan, PlanError>;
}

#[async_trait]
pub(crate) trait ErasedReconciler: Send + Sync {
    async fn reconcile(&self, ctx: &ReconcileContext) -> Result<(), ReconcileError>;
}

impl Registry {
    pub(crate) fn from_entries(
        workers: Vec<WorkerEntry>,
        planners: Vec<PlannerEntry>,
        reconcilers: Vec<ReconcilerEntry>,
        pools: Vec<&'static str>,
    ) -> Self {
        Self {
            workers: workers
                .into_iter()
                .map(|entry| (entry.name, entry))
                .collect(),
            planners: planners
                .into_iter()
                .map(|entry| (entry.name, entry))
                .collect(),
            reconcilers: reconcilers
                .into_iter()
                .map(|entry| (entry.name, entry))
                .collect(),
            pools: pools.into_iter().collect(),
        }
    }

    pub(crate) fn find_worker(&self, name: &str) -> Option<&Arc<dyn ErasedWorker>> {
        self.workers.get(name).map(|entry| &entry.worker)
    }

    pub(crate) fn is_exclusive(&self, worker: &str) -> bool {
        self.workers
            .get(worker)
            .is_some_and(|entry| entry.exclusive)
    }

    pub(crate) fn find_planner(&self, name: &str) -> Option<&Arc<dyn ErasedPlanner>> {
        self.planners.get(name).map(|entry| &entry.planner)
    }

    pub(crate) fn find_reconciler(&self, name: &str) -> Option<&Arc<dyn ErasedReconciler>> {
        self.reconcilers.get(name).map(|entry| &entry.reconciler)
    }

    pub(crate) fn list_registered_pools(&self) -> &BTreeSet<&'static str> {
        &self.pools
    }

    pub(crate) fn find_planners_waiting_on(&self, worker: &str) -> Vec<Arc<dyn ErasedPlanner>> {
        self.planners
            .values()
            .filter(|entry| {
                entry.triggers.iter().any(
                    |trigger| matches!(trigger, Trigger::Done(done) if done.worker() == worker),
                )
            })
            .map(|entry| Arc::clone(&entry.planner))
            .collect()
    }
}

impl WorkerEntry {
    pub(crate) fn of<W: Worker>(worker: W) -> Self {
        Self {
            name: W::NAME,
            exclusive: W::EXCLUSIVE,
            worker: Arc::new(worker),
        }
    }
}

impl PlannerEntry {
    pub(crate) fn of<P: Planner>(planner: P) -> Self {
        Self {
            name: P::NAME,
            triggers: P::triggers(),
            planner: Arc::new(planner),
        }
    }
}

impl ReconcilerEntry {
    pub(crate) fn of<R: Reconciler>(reconciler: R) -> Self {
        Self {
            name: R::NAME,
            writes: R::WRITES,
            reconciler: Arc::new(reconciler),
        }
    }
}

#[async_trait]
impl<W: Worker> ErasedWorker for W {
    async fn run(&self, ctx: &TaskContext, input: &[u8]) -> Result<EncodedOutcome, TaskError> {
        let input: W::Input = serde_json::from_slice(input).map_err(TaskError::permanent)?;
        Worker::run(self, ctx, input).await?.encode()
    }
}

#[async_trait]
impl<P: Planner> ErasedPlanner for P {
    async fn plan(&self, fired: Fired) -> Result<Plan, PlanError> {
        let mut ctx = PlanContext::for_fired(&fired);
        Planner::plan(self, &mut ctx, fired).await?;
        ctx.into_plan()
    }
}

#[async_trait]
impl<R: Reconciler> ErasedReconciler for R {
    async fn reconcile(&self, ctx: &ReconcileContext) -> Result<(), ReconcileError> {
        Reconciler::reconcile(self, ctx).await
    }
}
