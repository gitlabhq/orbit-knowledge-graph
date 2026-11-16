use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;

use crate::planner::{Plan, PlanContext, PlanError, Planner};
use crate::reconciler::{ReconcileContext, ReconcileError, Reconciler};
use crate::resources::{MissingResource, Resources};
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
    make: fn(&Resources) -> Result<Arc<dyn ErasedWorker>, MissingResource>,
}

#[derive(Clone)]
pub(crate) struct PlannerEntry {
    pub(crate) name: &'static str,
    pub(crate) triggers: fn() -> Vec<Trigger>,
    make: fn(&Resources) -> Result<Arc<dyn ErasedPlanner>, MissingResource>,
}

#[derive(Clone)]
pub(crate) struct ReconcilerEntry {
    pub(crate) name: &'static str,
    pub(crate) writes: &'static [&'static str],
    make: fn(&Resources) -> Result<Arc<dyn ErasedReconciler>, MissingResource>,
}

struct RegisteredPlanner {
    planner: Arc<dyn ErasedPlanner>,
    triggers: Vec<Trigger>,
}

struct WorkerAdapter<W>(W);
struct PlannerAdapter<P>(P);
struct ReconcilerAdapter<R>(R);

impl Registry {
    pub(crate) fn instantiate(
        workers: &[WorkerEntry],
        planners: &[PlannerEntry],
        reconcilers: &[ReconcilerEntry],
        pools: &[&'static str],
        resources: &Resources,
    ) -> Result<Self, MissingResource> {
        let mut registry = Self {
            workers: BTreeMap::new(),
            planners: BTreeMap::new(),
            reconcilers: BTreeMap::new(),
            pools: pools.iter().copied().collect(),
        };
        for entry in workers {
            registry
                .workers
                .insert(entry.name, (entry.make)(resources)?);
        }
        for entry in planners {
            let planner = RegisteredPlanner {
                planner: (entry.make)(resources)?,
                triggers: (entry.triggers)(),
            };
            registry.planners.insert(entry.name, planner);
        }
        for entry in reconcilers {
            registry
                .reconcilers
                .insert(entry.name, (entry.make)(resources)?);
        }
        Ok(registry)
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
    pub(crate) fn of<W: Worker>() -> Self {
        Self {
            name: W::NAME,
            make: |resources| Ok(Arc::new(WorkerAdapter(W::from_resources(resources)?))),
        }
    }
}

impl PlannerEntry {
    pub(crate) fn of<P: Planner>() -> Self {
        Self {
            name: P::NAME,
            triggers: P::triggers,
            make: |resources| Ok(Arc::new(PlannerAdapter(P::from_resources(resources)?))),
        }
    }
}

impl ReconcilerEntry {
    pub(crate) fn of<R: Reconciler>() -> Self {
        Self {
            name: R::NAME,
            writes: R::WRITES,
            make: |resources| Ok(Arc::new(ReconcilerAdapter(R::from_resources(resources)?))),
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
