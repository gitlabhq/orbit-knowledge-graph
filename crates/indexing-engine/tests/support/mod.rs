#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use indexing_engine::prelude::*;
use indexing_engine::testkit::Testkit;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

pub const BIG: Lane = Lane::new("big");
pub const BIG_DOC_BYTES: u64 = 1_000;
pub const SWEEP_PAGE: usize = 2;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Doc {
    pub tenant: Tenant,
    pub id: u64,
    pub size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Shelf {
    pub tenant: Tenant,
    pub id: u64,
    pub docs: Vec<Doc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Indexed {
    pub doc: Doc,
}

pub enum Step {
    Done { rows: u64 },
    NoChange,
    Fail,
    Permanent,
    Defer(Duration),
    Throttle(Duration),
    Sleep(Duration),
    Pool(&'static str),
}

#[derive(Default)]
pub struct Script {
    steps: Mutex<VecDeque<Step>>,
}

#[derive(Default)]
pub struct Library {
    pub docs: Vec<Doc>,
}

#[derive(Clone, Default)]
pub struct StatusBoard {
    indexed: Arc<Mutex<Vec<u64>>>,
}

pub struct DocIndex {
    script: Arc<Script>,
}

pub struct ShelfSplit;

pub struct SummaryBuild;

pub struct DocPlanner {
    library: Arc<Library>,
}

pub struct ShelfPlanner;

pub struct SummaryPlanner;

pub struct DocStatus {
    library: Arc<Library>,
    board: StatusBoard,
}

pub struct Fixtures {
    pub script: Arc<Script>,
    pub library: Arc<Library>,
    pub board: StatusBoard,
}

pub fn tenant() -> Tenant {
    Tenant::new("tenant-1")
}

pub fn doc(id: u64, size: u64) -> Doc {
    Doc {
        tenant: tenant(),
        id,
        size,
    }
}

pub fn changed(docs: &[Doc]) -> Fired {
    match EventBatch::encode("docs.changed", docs) {
        Ok(batch) => Fired::Events(batch),
        Err(error) => panic!("test docs must encode: {error}"),
    }
}

pub fn kit(script: Script, docs: Vec<Doc>, board: StatusBoard) -> Testkit {
    let fixtures = fixtures(script, docs, board);
    Testkit::new().module(|engine| register(engine, &fixtures))
}

pub fn fixtures(script: Script, docs: Vec<Doc>, board: StatusBoard) -> Fixtures {
    Fixtures {
        script: Arc::new(script),
        library: Arc::new(Library { docs }),
        board,
    }
}

pub fn script(steps: impl IntoIterator<Item = Step>) -> Script {
    Script {
        steps: Mutex::new(steps.into_iter().collect()),
    }
}

pub fn register(engine: &mut EngineBuilder, fixtures: &Fixtures) {
    engine
        .worker(DocIndex {
            script: Arc::clone(&fixtures.script),
        })
        .worker(ShelfSplit)
        .worker(SummaryBuild)
        .planner(DocPlanner {
            library: Arc::clone(&fixtures.library),
        })
        .planner(ShelfPlanner)
        .planner(SummaryPlanner)
        .reconciler(DocStatus {
            library: Arc::clone(&fixtures.library),
            board: fixtures.board.clone(),
        })
        .pool("parse");
}

impl StatusBoard {
    pub fn indexed(&self) -> Vec<u64> {
        self.indexed.lock().clone()
    }
}

impl Script {
    fn take_next_step(&self) -> Step {
        self.steps
            .lock()
            .pop_front()
            .unwrap_or(Step::Done { rows: 1 })
    }
}

#[async_trait]
impl Worker for DocIndex {
    const NAME: &'static str = "doc.index";
    const EXCLUSIVE: bool = true;
    type Input = Doc;
    type Output = Indexed;

    fn key(doc: &Doc) -> Key {
        Key::new("doc", doc.id)
    }

    async fn run(&self, ctx: &TaskContext, doc: Doc) -> Result<Outcome<Indexed>, TaskError> {
        match self.script.take_next_step() {
            Step::Done { rows } => Ok(Outcome::done(Indexed { doc }).rows("pages", rows)),
            Step::NoChange => Ok(Outcome::no_change()),
            Step::Fail => Err(anyhow::anyhow!("the store did not answer").into()),
            Step::Permanent => Err(TaskError::permanent(anyhow::anyhow!("the doc is corrupt"))),
            Step::Defer(after) => Err(TaskError::defer(after, "not_ready")),
            Step::Throttle(after) => Err(TaskError::throttle(after, "store_busy")),
            Step::Sleep(duration) => {
                tokio::time::sleep(duration).await;
                Ok(Outcome::done(Indexed { doc }))
            }
            Step::Pool(name) => {
                let _permit = ctx.pool(name).await?;
                Ok(Outcome::done(Indexed { doc }))
            }
        }
    }
}

#[async_trait]
impl Worker for ShelfSplit {
    const NAME: &'static str = "shelf.split";
    type Input = Shelf;
    type Output = ();

    fn key(shelf: &Shelf) -> Key {
        Key::new("shelf", shelf.id)
    }

    async fn run(&self, _: &TaskContext, shelf: Shelf) -> Result<Outcome<()>, TaskError> {
        Ok(Outcome::done(()).spawn::<DocIndex>(shelf.docs))
    }
}

#[async_trait]
impl Worker for SummaryBuild {
    const NAME: &'static str = "summary.build";
    type Input = Doc;
    type Output = ();

    fn key(doc: &Doc) -> Key {
        Key::new("summary", doc.id)
    }

    async fn run(&self, _: &TaskContext, _: Doc) -> Result<Outcome<()>, TaskError> {
        Ok(Outcome::done(()))
    }
}

#[async_trait]
impl Planner for DocPlanner {
    const NAME: &'static str = "doc.planner";

    fn triggers() -> Vec<Trigger> {
        vec![Trigger::Stream {
            subjects: &["docs.changed"],
            batch: 500,
            linger: Duration::from_secs(2),
        }]
    }

    async fn plan(&self, ctx: &mut PlanContext, fired: Fired) -> Result<(), PlanError> {
        let docs: Vec<Doc> = match fired {
            Fired::Events(batch) => batch.decode()?,
            Fired::Manual { keys, .. } => self
                .library
                .docs
                .iter()
                .filter(|doc| keys.contains(&DocIndex::key(doc)))
                .cloned()
                .collect(),
            Fired::Sweep { cursor, .. } => {
                let start: usize = cursor.as_deref().unwrap_or("0").parse()?;
                let end = (start + SWEEP_PAGE).min(self.library.docs.len());
                ctx.continue_from((end < self.library.docs.len()).then(|| end.to_string()));
                self.library.docs[start..end].to_vec()
            }
            Fired::Cron => return Ok(()),
        };
        for doc in docs {
            let lane = if doc.size > BIG_DOC_BYTES {
                BIG
            } else {
                Lane::DEFAULT
            };
            ctx.submit::<DocIndex>(doc.tenant.clone(), doc).lane(lane);
        }
        Ok(())
    }
}

#[async_trait]
impl Planner for ShelfPlanner {
    const NAME: &'static str = "shelf.planner";

    fn triggers() -> Vec<Trigger> {
        vec![Trigger::Stream {
            subjects: &["shelves.changed"],
            batch: 100,
            linger: Duration::from_secs(2),
        }]
    }

    async fn plan(&self, ctx: &mut PlanContext, fired: Fired) -> Result<(), PlanError> {
        match fired {
            Fired::Events(batch) => {
                for shelf in batch.decode::<Shelf>()? {
                    ctx.submit::<ShelfSplit>(shelf.tenant.clone(), shelf);
                }
            }
            Fired::Manual { .. } | Fired::Sweep { .. } | Fired::Cron => {}
        }
        Ok(())
    }
}

#[async_trait]
impl Planner for SummaryPlanner {
    const NAME: &'static str = "summary.planner";

    fn triggers() -> Vec<Trigger> {
        vec![Trigger::done::<DocIndex>()]
    }

    async fn plan(&self, ctx: &mut PlanContext, fired: Fired) -> Result<(), PlanError> {
        match fired {
            Fired::Events(batch) => {
                for indexed in batch.outputs::<DocIndex>()? {
                    ctx.submit::<SummaryBuild>(indexed.doc.tenant.clone(), indexed.doc);
                }
            }
            Fired::Manual { .. } | Fired::Sweep { .. } | Fired::Cron => {}
        }
        Ok(())
    }
}

#[async_trait]
impl Reconciler for DocStatus {
    const NAME: &'static str = "doc.status";
    const WRITES: &'static [&'static str] = &["doc_status"];
    const EVERY: Duration = Duration::from_secs(60);

    async fn reconcile(&self, ctx: &ReconcileContext) -> Result<(), ReconcileError> {
        for doc in &self.library.docs {
            let key = DocIndex::key(doc);
            match ctx.last_job::<DocIndex>(&doc.tenant, &key).await? {
                Some(job) if job.is_done() => self.board.indexed.lock().push(doc.id),
                Some(_) => {}
                None => ctx.replan::<DocPlanner>(&doc.tenant, vec![key]).await?,
            }
        }
        Ok(())
    }
}
