mod ledger;
mod runtime;
mod timeline;

use std::sync::{Arc, OnceLock};

use crate::builder::EngineBuilder;
use crate::config::EngineConfig;
use crate::lane::Lane;
use crate::ledger::JobSummary;
use crate::payload::{Key, Tenant};
use crate::planner::{Plan, Planner};
use crate::reconciler::{ReconcileContext, Reconciler};
use crate::trigger::Fired;
use crate::worker::Worker;

pub use ledger::TaskSummary;

use runtime::KitRuntime;

#[derive(Default)]
pub struct Testkit {
    builder: EngineBuilder,
    runtime: OnceLock<Arc<KitRuntime>>,
}

pub struct PlannedTasks {
    plan: Plan,
}

impl Testkit {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config(mut self, config: EngineConfig) -> Self {
        self.builder = self.builder.config(config);
        self
    }

    pub fn module(mut self, register: impl FnOnce(&mut EngineBuilder)) -> Self {
        self.builder = self.builder.module(register);
        self
    }

    pub async fn plan<P: Planner>(&self, fired: Fired) -> PlannedTasks {
        match self.get_or_build_runtime().plan(P::NAME, fired).await {
            Ok(plan) => PlannedTasks { plan },
            Err(error) => panic!("the planner {} failed: {error}", P::NAME),
        }
    }

    pub async fn fire<P: Planner>(&self, fired: Fired) {
        if let Err(error) = self.get_or_build_runtime().fire(P::NAME, fired).await {
            panic!("the planner {} failed: {error}", P::NAME);
        }
    }

    pub async fn campaign<P: Planner>(&self, tag: &str) {
        if let Err(error) = self.get_or_build_runtime().run_campaign(P::NAME, tag).await {
            panic!("the campaign {tag} of {} failed: {error}", P::NAME);
        }
    }

    pub async fn reconcile<R: Reconciler>(&self) {
        let runtime = Arc::clone(self.get_or_build_runtime());
        let ctx = ReconcileContext::new(runtime.clone());
        if let Err(error) = runtime.reconcile(R::NAME, &ctx).await {
            panic!("the reconciler {} failed: {error}", R::NAME);
        }
    }

    pub async fn run_until_idle(&self) {
        self.get_or_build_runtime().run_until_idle().await;
    }

    pub fn timeline(&self) -> Vec<String> {
        self.get_or_build_runtime().list_timeline()
    }

    pub fn last_job<W: Worker>(&self, tenant: &Tenant, key: &Key) -> Option<JobSummary> {
        self.get_or_build_runtime()
            .find_last_job(tenant, W::NAME, key)
    }

    pub fn find_tasks<W: Worker>(&self) -> Vec<TaskSummary> {
        self.get_or_build_runtime().find_tasks(W::NAME)
    }

    fn get_or_build_runtime(&self) -> &Arc<KitRuntime> {
        self.runtime.get_or_init(|| {
            let built = self.builder.clone().build();
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

    pub fn inputs<W: Worker>(&self) -> Vec<W::Input> {
        self.plan
            .tasks
            .iter()
            .filter(|task| task.input.worker == W::NAME)
            .map(|task| match serde_json::from_slice(&task.input.bytes) {
                Ok(input) => input,
                Err(error) => panic!("a planned input of {} does not decode: {error}", W::NAME),
            })
            .collect()
    }

    pub fn next_page(&self) -> Option<&str> {
        self.plan.next_page.as_deref()
    }
}
