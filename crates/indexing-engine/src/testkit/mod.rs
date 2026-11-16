mod ledger;
mod runtime;

use std::sync::{Arc, OnceLock};

use crate::builder::EngineBuilder;
use crate::config::EngineConfig;
use crate::lane::Lane;
use crate::ledger::JobSummary;
use crate::payload::{Key, Tenant};
use crate::planner::{Plan, PlanError, Planner};
use crate::reconciler::{ReconcileContext, ReconcileError, Reconciler};
use crate::resources::Resources;
use crate::trigger::Fired;
use crate::worker::Worker;

pub use ledger::TaskSummary;

use runtime::KitRuntime;

#[derive(Default)]
pub struct Testkit {
    builder: EngineBuilder,
    resources: Resources,
    runtime: OnceLock<Arc<KitRuntime>>,
}

pub struct PlannedTasks {
    plan: Plan,
}

impl Testkit {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn provide<T: Send + Sync + 'static>(mut self, value: T) -> Self {
        self.resources = self.resources.provide(value);
        self
    }

    pub fn config(mut self, config: EngineConfig) -> Self {
        self.builder = self.builder.config(config);
        self
    }

    pub fn module(mut self, register: impl FnOnce(&mut EngineBuilder)) -> Self {
        self.builder = self.builder.module(register);
        self
    }

    pub async fn plan<P: Planner>(&self, fired: Fired) -> Result<PlannedTasks, PlanError> {
        let plan = self.runtime().plan(P::NAME, fired).await?;
        Ok(PlannedTasks { plan })
    }

    pub async fn trigger<P: Planner>(&self, fired: Fired) -> Result<(), PlanError> {
        self.runtime().plan_and_submit(P::NAME, fired).await?;
        Ok(())
    }

    pub async fn campaign<P: Planner>(&self, tag: &str) -> Result<(), PlanError> {
        self.runtime().run_campaign(P::NAME, tag).await
    }

    pub async fn reconcile<R: Reconciler>(&self) -> Result<(), ReconcileError> {
        let runtime = Arc::clone(self.runtime());
        let ctx = ReconcileContext::new(runtime.clone());
        runtime.reconcile(R::NAME, &ctx).await
    }

    pub async fn settle(&self) {
        self.runtime().settle().await;
    }

    pub fn last_job<W: Worker>(&self, tenant: &Tenant, key: &Key) -> Option<JobSummary> {
        self.runtime().find_last_job(tenant, W::NAME, key)
    }

    pub fn find_tasks<W: Worker>(&self) -> Vec<TaskSummary> {
        self.runtime().find_tasks(W::NAME)
    }

    fn runtime(&self) -> &Arc<KitRuntime> {
        self.runtime.get_or_init(|| {
            let built = self
                .builder
                .clone()
                .resources(self.resources.clone())
                .build();
            match built {
                Ok(engine) => Arc::new(KitRuntime::new(engine)),
                Err(error) => panic!("the testkit engine failed to build: {error}"),
            }
        })
    }
}

impl PlannedTasks {
    pub fn lane_of<W: Worker>(&self) -> Option<Lane> {
        self.plan
            .tasks
            .iter()
            .find(|task| task.input.worker == W::NAME)
            .map(|task| task.lane.clone())
    }

    pub fn inputs<W: Worker>(&self) -> Result<Vec<W::Input>, serde_json::Error> {
        self.plan
            .tasks
            .iter()
            .filter(|task| task.input.worker == W::NAME)
            .map(|task| serde_json::from_slice(&task.input.bytes))
            .collect()
    }

    pub fn next_page(&self) -> Option<&str> {
        self.plan.next_page.as_deref()
    }
}
