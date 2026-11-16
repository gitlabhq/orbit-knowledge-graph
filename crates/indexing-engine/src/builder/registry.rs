use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;

use crate::planner::{Plan, PlanContext, PlanError, Planner};
use crate::reconciler::{ReconcileContext, ReconcileError, Reconciler};
use crate::trigger::{Fired, Trigger};
use crate::worker::{EncodedOutcome, TaskContext, TaskError, Worker};

pub(crate) struct Registry {
    workers: BTreeMap<&'static str, Arc<dyn ErasedWorker>>,
    planners: BTreeMap<&'static str, RegisteredPlanner>,
    reconcilers: BTreeMap<&'static str, Arc<dyn ErasedReconciler>>,
    pools: BTreeSet<&'static str>,
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

#[derive(Clone)]
pub(crate) struct WorkerEntry {
    pub(crate) name: &'static str,
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

struct RegisteredPlanner {
    planner: Arc<dyn ErasedPlanner>,
    triggers: Vec<Trigger>,
}

struct WorkerAdapter<W>(W);
struct PlannerAdapter<P>(P);
struct ReconcilerAdapter<R>(R);

impl Registry {
    pub(crate) fn from_entries(
        workers: &[WorkerEntry],
        planners: &[PlannerEntry],
        reconcilers: &[ReconcilerEntry],
        pools: &[&'static str],
    ) -> Self {
        let planners = planners.iter().map(|entry| {
            let registered = RegisteredPlanner {
                planner: Arc::clone(&entry.planner),
                triggers: entry.triggers.clone(),
            };
            (entry.name, registered)
        });
        Self {
            workers: workers
                .iter()
                .map(|entry| (entry.name, Arc::clone(&entry.worker)))
                .collect(),
            planners: planners.collect(),
            reconcilers: reconcilers
                .iter()
                .map(|entry| (entry.name, Arc::clone(&entry.reconciler)))
                .collect(),
            pools: pools.iter().copied().collect(),
        }
    }

    pub(crate) fn find_worker(&self, name: &str) -> Option<&Arc<dyn ErasedWorker>> {
        self.workers.get(name)
    }

    pub(crate) fn find_planner(&self, name: &str) -> Option<&Arc<dyn ErasedPlanner>> {
        self.planners
            .get(name)
            .map(|registered| &registered.planner)
    }

    pub(crate) fn find_reconciler(&self, name: &str) -> Option<&Arc<dyn ErasedReconciler>> {
        self.reconcilers.get(name)
    }

    pub(crate) fn list_registered_pools(&self) -> &BTreeSet<&'static str> {
        &self.pools
    }

    pub(crate) fn find_planners_waiting_on(&self, worker: &str) -> Vec<Arc<dyn ErasedPlanner>> {
        self.planners
            .values()
            .filter(|registered| {
                registered.triggers.iter().any(
                    |trigger| matches!(trigger, Trigger::Done(done) if done.worker() == worker),
                )
            })
            .map(|registered| Arc::clone(&registered.planner))
            .collect()
    }
}

impl WorkerEntry {
    pub(crate) fn of<W: Worker>(worker: W) -> Self {
        Self {
            name: W::NAME,
            worker: Arc::new(WorkerAdapter(worker)),
        }
    }
}

impl PlannerEntry {
    pub(crate) fn of<P: Planner>(planner: P) -> Self {
        Self {
            name: P::NAME,
            triggers: P::triggers(),
            planner: Arc::new(PlannerAdapter(planner)),
        }
    }
}

impl ReconcilerEntry {
    pub(crate) fn of<R: Reconciler>(reconciler: R) -> Self {
        Self {
            name: R::NAME,
            writes: R::WRITES,
            reconciler: Arc::new(ReconcilerAdapter(reconciler)),
        }
    }
}

#[async_trait]
impl<W: Worker> ErasedWorker for WorkerAdapter<W> {
    async fn run(&self, ctx: &TaskContext, input: &[u8]) -> Result<EncodedOutcome, TaskError> {
        let input: W::Input = serde_json::from_slice(input).map_err(TaskError::permanent)?;
        self.0.run(ctx, input).await?.encode()
    }
}

#[async_trait]
impl<P: Planner> ErasedPlanner for PlannerAdapter<P> {
    async fn plan(&self, fired: Fired) -> Result<Plan, PlanError> {
        let mut ctx = PlanContext::for_fired(&fired);
        self.0.plan(&mut ctx, fired).await?;
        ctx.into_plan()
    }
}

#[async_trait]
impl<R: Reconciler> ErasedReconciler for ReconcilerAdapter<R> {
    async fn reconcile(&self, ctx: &ReconcileContext) -> Result<(), ReconcileError> {
        self.0.reconcile(ctx).await
    }
}
