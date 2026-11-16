use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::time::Instant;
use uuid::Uuid;

use crate::builder::{Engine, ErasedPlanner};
use crate::config::LaneConfig;
use crate::disposition::{Attempt, Disposition};
use crate::lane::Lane;
use crate::ledger::JobSummary;
use crate::payload::{Key, TaskInput, Tenant};
use crate::planner::{Plan, PlanError, PlannedTask};
use crate::reconciler::{ReconcileBackend, ReconcileContext, ReconcileError};
use crate::testkit::ledger::{MemoryLedger, TaskSummary};
use crate::trigger::{EventBatch, Fired};
use crate::worker::{Completion, EncodedOutcome, Pools, TaskContext, TaskError};

const UNCONFIGURED_LANE_TIMEOUT: Duration = Duration::from_secs(15 * 60);

pub(super) struct KitRuntime {
    engine: Engine,
    pools: Arc<Pools>,
    state: Mutex<KitState>,
}

#[derive(Default)]
struct KitState {
    queue: Vec<QueuedTask>,
    pending: HashSet<(&'static str, Key)>,
    paused_lanes: HashMap<String, Instant>,
    ledger: MemoryLedger,
}

struct QueuedTask {
    job_id: Uuid,
    tenant: Tenant,
    input: TaskInput,
    lane: Lane,
    attempts_used: u16,
    first_started: Option<Instant>,
    ready_at: Instant,
}

impl KitRuntime {
    pub(super) fn new(engine: Engine) -> Self {
        let pools = Pools::new(
            engine.registry.registered_pools().clone(),
            engine.config.pools.clone(),
        );
        Self {
            engine,
            pools: Arc::new(pools),
            state: Mutex::new(KitState::default()),
        }
    }

    pub(super) async fn plan(&self, planner: &str, fired: Fired) -> Result<Plan, PlanError> {
        self.find_planner(planner)?.plan(fired).await
    }

    pub(super) async fn plan_and_submit(
        &self,
        planner: &str,
        fired: Fired,
    ) -> Result<Option<String>, PlanError> {
        let plan = self.plan(planner, fired).await?;
        let mut state = self.state.lock();
        for task in plan.tasks {
            state.submit(task);
        }
        Ok(plan.next_page)
    }

    pub(super) async fn run_campaign(&self, planner: &str, tag: &str) -> Result<(), PlanError> {
        let mut cursor = None;
        loop {
            let sweep = Fired::Sweep {
                cursor,
                tag: tag.to_owned(),
            };
            cursor = self.plan_and_submit(planner, sweep).await?;
            if cursor.is_none() {
                return Ok(());
            }
        }
    }

    pub(super) async fn reconcile(
        &self,
        reconciler: &str,
        ctx: &ReconcileContext,
    ) -> Result<(), ReconcileError> {
        let Some(reconciler) = self.engine.registry.find_reconciler(reconciler).cloned() else {
            return Err(anyhow!("no module registered the reconciler {reconciler}").into());
        };
        reconciler.reconcile(ctx).await
    }

    pub(super) async fn settle(&self) {
        loop {
            let Some((ready_at, task)) = self.state.lock().take_next() else {
                return;
            };
            tokio::time::sleep_until(ready_at).await;
            let (task, first_started) = self.state.lock().start(task);
            self.run(task, first_started).await;
        }
    }

    pub(super) fn find_last_job(
        &self,
        tenant: &Tenant,
        worker: &str,
        key: &Key,
    ) -> Option<JobSummary> {
        self.state.lock().ledger.find_last_job(tenant, worker, key)
    }

    pub(super) fn find_tasks(&self, worker: &str) -> Vec<TaskSummary> {
        self.state.lock().ledger.find_tasks(worker)
    }

    async fn run(&self, task: QueuedTask, first_started: Instant) {
        let lane = self.find_lane_config(&task);
        let attempt = self.run_attempt(&task, &lane).await;
        match Disposition::of(attempt, task.attempts_used, first_started.elapsed(), &lane) {
            Disposition::Finish(outcome) => self.finish(task, outcome).await,
            Disposition::Retry { after } => self.state.lock().requeue(task.use_attempt(), after),
            Disposition::Redeliver { after } => self.state.lock().requeue(task, after),
            Disposition::PauseLane { after } => {
                let mut state = self.state.lock();
                state.pause_lane(&task, after);
                state.requeue(task, after);
            }
            Disposition::MoveLane { to } => self
                .state
                .lock()
                .requeue(task.use_attempt().move_to(to), Duration::ZERO),
            Disposition::DeadLetter { class, message } => {
                self.state.lock().dead_letter(task, class, message)
            }
        }
    }

    async fn run_attempt(&self, task: &QueuedTask, lane: &LaneConfig) -> Attempt {
        let Some(worker) = self.engine.registry.find_worker(task.input.worker).cloned() else {
            let error = anyhow!("no module registered the worker {}", task.input.worker);
            return Attempt::Failed(TaskError::permanent(error));
        };
        let ctx = TaskContext::new(Arc::clone(&self.pools));
        match tokio::time::timeout(lane.timeout, worker.run(&ctx, &task.input.bytes)).await {
            Ok(Ok(outcome)) => Attempt::Finished(outcome),
            Ok(Err(error)) => Attempt::Failed(error),
            Err(_) => Attempt::TimedOut,
        }
    }

    async fn finish(&self, task: QueuedTask, outcome: EncodedOutcome) {
        let output = self.state.lock().complete(&task, outcome);
        let Some(output) = output else {
            return;
        };
        for planner in self.engine.registry.find_planners_after(task.input.worker) {
            let batch = EventBatch::done(task.input.worker, output.clone(), task.lane.clone());
            match planner.plan(Fired::Events(batch)).await {
                Ok(plan) => {
                    let mut state = self.state.lock();
                    for planned in plan.tasks {
                        state.submit(planned);
                    }
                }
                Err(error) => panic!(
                    "a planner failed on a done event of {}: {error}",
                    task.input.worker
                ),
            }
        }
    }

    fn find_planner(&self, name: &str) -> Result<Arc<dyn ErasedPlanner>, PlanError> {
        self.engine
            .registry
            .find_planner(name)
            .cloned()
            .ok_or_else(|| anyhow!("no module registered the planner {name}").into())
    }

    fn find_lane_config(&self, task: &QueuedTask) -> LaneConfig {
        self.engine
            .config
            .lanes
            .get(&task.lane.format_id(task.input.worker))
            .cloned()
            .unwrap_or_else(|| LaneConfig::new(1, UNCONFIGURED_LANE_TIMEOUT))
    }
}

#[async_trait]
impl ReconcileBackend for KitRuntime {
    async fn last_job(
        &self,
        tenant: &Tenant,
        worker: &'static str,
        key: &Key,
    ) -> anyhow::Result<Option<JobSummary>> {
        Ok(KitRuntime::find_last_job(self, tenant, worker, key))
    }

    async fn replan(
        &self,
        planner: &'static str,
        tenant: &Tenant,
        keys: Vec<Key>,
    ) -> anyhow::Result<()> {
        let manual = Fired::Manual {
            tenant: tenant.clone(),
            keys,
        };
        self.plan_and_submit(planner, manual)
            .await
            .map_err(|error| anyhow!("{error}"))?;
        Ok(())
    }

    async fn campaign(&self, planner: &'static str, tag: &str) -> anyhow::Result<()> {
        self.run_campaign(planner, tag)
            .await
            .map_err(|error| anyhow!("{error}"))
    }
}

impl KitState {
    fn submit(&mut self, task: PlannedTask) {
        if self
            .pending
            .contains(&(task.input.worker, task.input.key.clone()))
        {
            return;
        }
        let job_id = self
            .ledger
            .open_job(&task.tenant, task.input.worker, &task.input.key);
        self.enqueue(job_id, task.tenant, task.input, task.lane);
    }

    fn enqueue(&mut self, job_id: Uuid, tenant: Tenant, input: TaskInput, lane: Lane) {
        if !self.pending.insert((input.worker, input.key.clone())) {
            return;
        }
        self.ledger.add_task(job_id);
        self.queue.push(QueuedTask {
            job_id,
            tenant,
            input,
            lane,
            attempts_used: 0,
            first_started: None,
            ready_at: Instant::now(),
        });
    }

    fn take_next(&mut self) -> Option<(Instant, QueuedTask)> {
        let (index, ready_at) = self
            .queue
            .iter()
            .enumerate()
            .map(|(index, task)| (index, self.ready_at(task)))
            .min_by_key(|(_, ready_at)| *ready_at)?;
        Some((ready_at, self.queue.remove(index)))
    }

    fn start(&mut self, mut task: QueuedTask) -> (QueuedTask, Instant) {
        self.pending
            .remove(&(task.input.worker, task.input.key.clone()));
        let first_started = *task.first_started.get_or_insert_with(Instant::now);
        (task, first_started)
    }

    fn requeue(&mut self, mut task: QueuedTask, after: Duration) {
        task.ready_at = Instant::now() + after;
        self.queue.push(task);
    }

    fn pause_lane(&mut self, task: &QueuedTask, after: Duration) {
        self.paused_lanes.insert(
            task.lane.format_id(task.input.worker),
            Instant::now() + after,
        );
    }

    fn complete(&mut self, task: &QueuedTask, outcome: EncodedOutcome) -> Option<Vec<u8>> {
        for child in outcome.children {
            self.enqueue(task.job_id, task.tenant.clone(), child, task.lane.clone());
        }
        self.ledger.finish_task(task.job_id, &outcome.rows);
        match outcome.completion {
            Completion::Done(output) => {
                self.ledger
                    .record_task(task.input.worker, task.summarize(None));
                Some(output)
            }
            Completion::NoChange => None,
        }
    }

    fn dead_letter(&mut self, task: QueuedTask, class: &'static str, message: String) {
        self.ledger.fail_task(task.job_id);
        self.ledger
            .record_task(task.input.worker, task.summarize(Some((class, message))));
    }

    fn ready_at(&self, task: &QueuedTask) -> Instant {
        let paused_until = self
            .paused_lanes
            .get(&task.lane.format_id(task.input.worker));
        match paused_until {
            Some(paused_until) => task.ready_at.max(*paused_until),
            None => task.ready_at,
        }
    }
}

impl QueuedTask {
    fn use_attempt(mut self) -> Self {
        self.attempts_used += 1;
        self
    }

    fn move_to(mut self, lane: Lane) -> Self {
        self.lane = lane;
        self
    }

    fn summarize(&self, failure: Option<(&'static str, String)>) -> TaskSummary {
        let (error_class, error_message) = failure.unzip();
        TaskSummary {
            lane: self.lane.clone(),
            attempts: self.attempts_used + 1,
            error_class,
            error_message,
        }
    }
}
