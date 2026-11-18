#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::time::Duration;

use async_trait::async_trait;
use indexing_engine_next::prelude::*;
use parking_lot::Mutex;

pub const SECOND: Duration = Duration::from_secs(1);
pub const MINUTE: Duration = Duration::from_secs(60);
pub const HOUR: Duration = Duration::from_secs(60 * 60);

pub const BIG: Lane = Lane::new("big");

pub type TestWorker = ScriptedWorker<false>;
pub type ExclusiveWorker = ScriptedWorker<true>;

pub type TestPlanner = TaskPlanner<TestWorker>;
pub type ExclusivePlanner = TaskPlanner<ExclusiveWorker>;

pub enum Run {
    Done,
    DoneWithRows(&'static str, u64),
    NoChange,
    Spawn(Vec<u64>),
    Error(TaskError),
    Sleep(Duration),
    HoldPool(&'static str, Duration),
}

#[derive(Default)]
pub struct ScriptedWorker<const IS_EXCLUSIVE: bool> {
    runs: Mutex<HashMap<u64, VecDeque<Run>>>,
}

pub struct TaskPlanner<W> {
    keys: Vec<u64>,
    lanes: HashMap<u64, Lane>,
    page_size: usize,
    worker: PhantomData<fn() -> W>,
}

pub struct DownstreamWorker;

#[derive(Default)]
pub struct DownstreamPlanner {
    lane: Option<Lane>,
}

pub struct ReplanReconciler {
    keys: Vec<u64>,
}

pub fn tenant() -> Tenant {
    Tenant::new("tenant-1")
}

pub fn task_key(task: u64) -> Key {
    Key::new("task", task)
}

pub fn events(tasks: impl IntoIterator<Item = u64>) -> Fired {
    let tasks: Vec<u64> = tasks.into_iter().collect();
    Fired::Events(EventBatch::encode("tasks.changed", &tasks).unwrap())
}

pub fn manual(tasks: impl IntoIterator<Item = u64>) -> Fired {
    Fired::Manual {
        tenant: tenant(),
        keys: tasks.into_iter().map(task_key).collect(),
    }
}

pub fn sweep() -> Fired {
    Fired::Sweep {
        cursor: None,
        tag: "rebuild".to_owned(),
    }
}

impl<const IS_EXCLUSIVE: bool> ScriptedWorker<IS_EXCLUSIVE> {
    pub fn runs(self, task: u64, runs: impl IntoIterator<Item = Run>) -> Self {
        self.runs.lock().entry(task).or_default().extend(runs);
        self
    }

    fn take_next_run(&self, task: u64) -> Run {
        self.runs
            .lock()
            .get_mut(&task)
            .and_then(VecDeque::pop_front)
            .unwrap_or(Run::Done)
    }
}

impl<W> Default for TaskPlanner<W> {
    fn default() -> Self {
        Self {
            keys: Vec::new(),
            lanes: HashMap::new(),
            page_size: usize::MAX,
            worker: PhantomData,
        }
    }
}

impl<W> TaskPlanner<W> {
    pub fn with_keys(mut self, tasks: impl IntoIterator<Item = u64>) -> Self {
        self.keys.extend(tasks);
        self
    }

    pub fn with_lane(mut self, task: u64, lane: Lane) -> Self {
        self.lanes.insert(task, lane);
        self
    }

    pub fn with_page_size(mut self, page_size: usize) -> Self {
        self.page_size = page_size;
        self
    }

    fn read_page(&self, cursor: Option<String>) -> Result<(Vec<u64>, Option<String>), PlanError> {
        let start: usize = cursor.as_deref().unwrap_or("0").parse()?;
        let end = start.saturating_add(self.page_size).min(self.keys.len());
        let next_page = (end < self.keys.len()).then(|| end.to_string());
        Ok((self.keys[start..end].to_vec(), next_page))
    }
}

impl DownstreamPlanner {
    pub fn in_lane(lane: Lane) -> Self {
        Self { lane: Some(lane) }
    }
}

impl ReplanReconciler {
    pub fn for_keys(tasks: impl IntoIterator<Item = u64>) -> Self {
        Self {
            keys: tasks.into_iter().collect(),
        }
    }
}

#[async_trait]
impl<const IS_EXCLUSIVE: bool> Worker for ScriptedWorker<IS_EXCLUSIVE> {
    const NAME: &'static str = if IS_EXCLUSIVE {
        "exclusive.worker"
    } else {
        "test.worker"
    };
    const EXCLUSIVE: bool = IS_EXCLUSIVE;
    type Input = u64;
    type Output = u64;

    fn key(task: &u64) -> Key {
        task_key(*task)
    }

    async fn run(&self, ctx: &TaskContext, task: u64) -> Result<Outcome<u64>, TaskError> {
        match self.take_next_run(task) {
            Run::Done => Ok(Outcome::done(task)),
            Run::DoneWithRows(label, count) => Ok(Outcome::done(task).rows(label, count)),
            Run::NoChange => Ok(Outcome::no_change()),
            Run::Spawn(children) => Ok(Outcome::done(task).spawn::<Self>(children)),
            Run::Error(error) => Err(error),
            Run::Sleep(duration) => {
                tokio::time::sleep(duration).await;
                Ok(Outcome::done(task))
            }
            Run::HoldPool(pool, duration) => {
                let _permit = ctx.pool(pool).await?;
                tokio::time::sleep(duration).await;
                Ok(Outcome::done(task))
            }
        }
    }
}

#[async_trait]
impl<W: Worker<Input = u64>> Planner for TaskPlanner<W> {
    const NAME: &'static str = "test.planner";

    fn triggers() -> Vec<Trigger> {
        vec![Trigger::Stream {
            subjects: &["tasks.changed"],
            batch: 500,
            linger: Duration::from_secs(2),
        }]
    }

    async fn plan(&self, ctx: &mut PlanContext, fired: Fired) -> Result<(), PlanError> {
        let tasks: Vec<u64> = match fired {
            Fired::Events(batch) => batch.decode()?,
            Fired::Manual { keys, .. } => self
                .keys
                .iter()
                .copied()
                .filter(|task| keys.contains(&W::key(task)))
                .collect(),
            Fired::Sweep { cursor, .. } => {
                let (page, next_page) = self.read_page(cursor)?;
                ctx.continue_from(next_page);
                page
            }
            Fired::Cron => return Ok(()),
        };
        for task in tasks {
            let submit = ctx.submit::<W>(tenant(), task);
            if let Some(lane) = self.lanes.get(&task) {
                submit.lane(lane.clone());
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Worker for DownstreamWorker {
    const NAME: &'static str = "downstream.worker";
    type Input = u64;
    type Output = ();

    fn key(task: &u64) -> Key {
        task_key(*task)
    }

    async fn run(&self, _: &TaskContext, _: u64) -> Result<Outcome<()>, TaskError> {
        Ok(Outcome::done(()))
    }
}

#[async_trait]
impl Planner for DownstreamPlanner {
    const NAME: &'static str = "downstream.planner";

    fn triggers() -> Vec<Trigger> {
        vec![Trigger::done::<TestWorker>()]
    }

    async fn plan(&self, ctx: &mut PlanContext, fired: Fired) -> Result<(), PlanError> {
        match fired {
            Fired::Events(batch) => {
                for task in batch.outputs::<TestWorker>()? {
                    let submit = ctx.submit::<DownstreamWorker>(tenant(), task);
                    if let Some(lane) = &self.lane {
                        submit.lane(lane.clone());
                    }
                }
            }
            Fired::Manual { .. } | Fired::Sweep { .. } | Fired::Cron => {}
        }
        Ok(())
    }
}

#[async_trait]
impl Reconciler for ReplanReconciler {
    const NAME: &'static str = "replan.reconciler";
    const WRITES: &'static [&'static str] = &[];
    const EVERY: Duration = Duration::from_secs(60);

    async fn reconcile(&self, ctx: &ReconcileContext) -> Result<(), ReconcileError> {
        for task in &self.keys {
            let key = task_key(*task);
            if ctx.last_job::<TestWorker>(&tenant(), &key).await?.is_none() {
                ctx.replan::<TestPlanner>(&tenant(), vec![key]).await?;
            }
        }
        Ok(())
    }
}
