use std::collections::{HashMap, HashSet};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::runtime::{Handle, RuntimeFlavor};
use tokio::sync::Notify;
use tokio::task::{JoinError, JoinSet};
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
use crate::testkit::timeline::{TaskEvent, Timeline};
use crate::trigger::{EventBatch, Fired};
use crate::worker::{Completion, EncodedOutcome, Pools, TaskContext, TaskError};

const UNCONFIGURED_LANE_TIMEOUT: Duration = Duration::from_secs(15 * 60);

pub(super) struct KitRuntime {
    engine: Engine,
    pools: Arc<Pools>,
    submitted: Notify,
    state: Mutex<KitState>,
}

#[derive(Default)]
struct KitState {
    queue: Vec<QueuedTask>,
    pending: HashSet<(&'static str, Key)>,
    running_keys: HashSet<(&'static str, Key)>,
    busy_slots: HashMap<String, u32>,
    paused_lanes: HashMap<String, Instant>,
    ledger: MemoryLedger,
    timeline: Timeline,
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
            engine.registry.list_registered_pools().clone(),
            engine.config.pools.clone(),
        );
        Self {
            engine,
            pools: Arc::new(pools),
            submitted: Notify::new(),
            state: Mutex::new(KitState::default()),
        }
    }

    pub(super) async fn plan(&self, planner: &str, fired: Fired) -> Result<Plan, PlanError> {
        self.find_planner(planner)?.plan(fired).await
    }

    pub(super) async fn fire(
        &self,
        planner: &str,
        fired: Fired,
    ) -> Result<Option<String>, PlanError> {
        let plan = self.plan(planner, fired).await?;
        {
            let mut state = self.state.lock();
            for task in plan.tasks {
                state.submit(task);
            }
        }
        self.submitted.notify_one();
        Ok(plan.next_page)
    }

    pub(super) async fn run_campaign(&self, planner: &str, tag: &str) -> Result<(), PlanError> {
        let mut cursor = None;
        loop {
            let sweep = Fired::Sweep {
                cursor,
                tag: tag.to_owned(),
            };
            cursor = self.fire(planner, sweep).await?;
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

    pub(super) async fn run_until_idle(self: &Arc<Self>) {
        if Handle::current().runtime_flavor() != RuntimeFlavor::CurrentThread {
            panic!(
                "the testkit keeps a fixed order of events only on a current-thread runtime, so use #[tokio::test(start_paused = true)]"
            );
        }
        let mut running = JoinSet::new();
        loop {
            self.start_ready_tasks(&mut running);
            let wake_at = self.state.lock().find_next_wake(Instant::now());
            if running.is_empty() && wake_at.is_none() {
                return;
            }
            let wake = async {
                match wake_at {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                Some(joined) = running.join_next() => resume_panic(joined),
                () = wake => {}
                () = self.submitted.notified() => {}
            }
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

    pub(super) fn list_timeline(&self) -> Vec<String> {
        self.state.lock().timeline.list_lines()
    }

    fn start_ready_tasks(self: &Arc<Self>, running: &mut JoinSet<()>) {
        while let Some((task, first_started)) = self.take_ready_task() {
            let runtime = Arc::clone(self);
            running.spawn(async move { runtime.run(task, first_started).await });
        }
    }

    fn take_ready_task(&self) -> Option<(QueuedTask, Instant)> {
        let now = Instant::now();
        let mut state = self.state.lock();
        let index = state
            .queue
            .iter()
            .enumerate()
            .filter(|(_, task)| state.compute_ready_at(task) <= now)
            .filter(|(_, task)| {
                state.count_busy_slots(task) < self.find_lane_config(task).slots.get()
            })
            .filter(|(_, task)| {
                !self.engine.registry.is_exclusive(task.input.worker)
                    || !state.running_keys.contains(&task.worker_key())
            })
            .min_by_key(|(_, task)| state.compute_ready_at(task))
            .map(|(index, _)| index)?;
        Some(state.start(index, now))
    }

    async fn run(&self, task: QueuedTask, first_started: Instant) {
        let lane = self.find_lane_config(&task);
        let attempt = self.run_attempt(&task, &lane).await;
        let ended = attempt.describe();
        let disposition =
            Disposition::decide(attempt, task.attempts_used, first_started.elapsed(), &lane);
        self.state
            .lock()
            .finish_attempt(&task, ended, disposition.describe_next_step());
        match disposition {
            Disposition::Finish(outcome) => self.apply_outcome(task, outcome).await,
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

    async fn apply_outcome(&self, task: QueuedTask, outcome: EncodedOutcome) {
        let output = self.state.lock().record_outcome(&task, outcome);
        let Some(output) = output else {
            return;
        };
        for planner in self
            .engine
            .registry
            .find_planners_waiting_on(task.input.worker)
        {
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
            .get(&task.format_lane_id())
            .cloned()
            .unwrap_or_else(|| LaneConfig::new(NonZeroU32::MIN, UNCONFIGURED_LANE_TIMEOUT))
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
        self.fire(planner, manual)
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
        if self.is_pending(&task.input) {
            self.timeline
                .record(&task.input, &task.lane, TaskEvent::AlreadyPending);
            return;
        }
        let job_id = self
            .ledger
            .open_job(&task.tenant, task.input.worker, &task.input.key);
        self.enqueue(
            job_id,
            task.tenant,
            task.input,
            task.lane,
            TaskEvent::Submitted,
        );
    }

    fn enqueue(
        &mut self,
        job_id: Uuid,
        tenant: Tenant,
        input: TaskInput,
        lane: Lane,
        event: TaskEvent,
    ) {
        if self.is_pending(&input) {
            self.timeline
                .record(&input, &lane, TaskEvent::AlreadyPending);
            return;
        }
        self.timeline.record(&input, &lane, event);
        self.pending.insert((input.worker, input.key.clone()));
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

    fn start(&mut self, index: usize, now: Instant) -> (QueuedTask, Instant) {
        let mut task = self.queue.remove(index);
        self.pending.remove(&task.worker_key());
        self.running_keys.insert(task.worker_key());
        *self.busy_slots.entry(task.format_lane_id()).or_default() += 1;
        let first_started = *task.first_started.get_or_insert(now);
        let attempt = task.attempts_used + 1;
        self.timeline
            .record(&task.input, &task.lane, TaskEvent::Started { attempt });
        (task, first_started)
    }

    fn is_pending(&self, input: &TaskInput) -> bool {
        self.pending.contains(&(input.worker, input.key.clone()))
    }

    fn finish_attempt(
        &mut self,
        task: &QueuedTask,
        attempt: &'static str,
        next_step: Option<String>,
    ) {
        self.timeline.record(
            &task.input,
            &task.lane,
            TaskEvent::Ended { attempt, next_step },
        );
        self.running_keys.remove(&task.worker_key());
        if let Some(busy) = self.busy_slots.get_mut(&task.format_lane_id()) {
            *busy -= 1;
        }
    }

    fn count_busy_slots(&self, task: &QueuedTask) -> u32 {
        self.busy_slots
            .get(&task.format_lane_id())
            .copied()
            .unwrap_or(0)
    }

    fn find_next_wake(&self, now: Instant) -> Option<Instant> {
        self.queue
            .iter()
            .map(|task| self.compute_ready_at(task))
            .filter(|ready_at| *ready_at > now)
            .min()
    }

    fn requeue(&mut self, mut task: QueuedTask, after: Duration) {
        task.ready_at = Instant::now() + after;
        self.queue.push(task);
    }

    fn pause_lane(&mut self, task: &QueuedTask, after: Duration) {
        self.paused_lanes
            .insert(task.format_lane_id(), Instant::now() + after);
    }

    fn record_outcome(&mut self, task: &QueuedTask, outcome: EncodedOutcome) -> Option<Vec<u8>> {
        for child in outcome.children {
            self.enqueue(
                task.job_id,
                task.tenant.clone(),
                child,
                task.lane.clone(),
                TaskEvent::Spawned,
            );
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

    fn compute_ready_at(&self, task: &QueuedTask) -> Instant {
        let paused_until = self.paused_lanes.get(&task.format_lane_id());
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

    fn worker_key(&self) -> (&'static str, Key) {
        (self.input.worker, self.input.key.clone())
    }

    fn format_lane_id(&self) -> String {
        self.lane.format_id(self.input.worker)
    }

    fn summarize(&self, failure: Option<(&'static str, String)>) -> TaskSummary {
        let (error_class, error_message) = failure.unzip();
        TaskSummary {
            key: self.input.key.clone(),
            lane: self.lane.clone(),
            attempts: self.attempts_used + 1,
            error_class,
            error_message,
        }
    }
}

fn resume_panic(joined: Result<(), JoinError>) {
    if let Err(error) = joined
        && error.is_panic()
    {
        std::panic::resume_unwind(error.into_panic());
    }
}
