#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use indexing_engine::prelude::*;
use indexing_engine::testkit::Testkit;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

pub const BIG: Lane = Lane::new("big");
pub const BIG_DOC_BYTES: u64 = 1_000;
pub const BIG_SHELF_DOCS: usize = 1;
pub const SWEEP_PAGE: usize = 2;

pub const SECOND: Duration = Duration::from_secs(1);
pub const MINUTE: Duration = Duration::from_secs(60);
pub const HOUR: Duration = Duration::from_secs(60 * 60);

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
    Done {
        rows: u64,
    },
    NoChange,
    Fail,
    Permanent,
    Defer(Duration),
    Throttle(Duration),
    Incompatible,
    Sleep(Duration),
    Pool {
        pool: &'static str,
        during: Duration,
    },
}

#[derive(Default)]
pub struct Library {
    docs: Arc<Vec<Doc>>,
    script: Arc<Script>,
    board: StatusBoard,
}

#[derive(Default)]
struct Script {
    steps: Mutex<HashMap<u64, VecDeque<Step>>>,
}

#[derive(Clone, Default)]
struct StatusBoard {
    indexed: Arc<Mutex<Vec<u64>>>,
}

pub struct DocIndex {
    script: Arc<Script>,
}

pub struct ShelfSplit;

pub struct SummaryBuild;

pub struct DocPlanner {
    docs: Arc<Vec<Doc>>,
}

pub struct ShelfPlanner;

pub struct SummaryPlanner;

pub struct DocStatus {
    docs: Arc<Vec<Doc>>,
    board: StatusBoard,
}

impl Library {
    pub fn with_docs(mut self, docs: impl IntoIterator<Item = Doc>) -> Self {
        self.docs = Arc::new(docs.into_iter().collect());
        self
    }

    pub fn with_steps(self, doc: u64, steps: impl IntoIterator<Item = Step>) -> Self {
        self.script
            .steps
            .lock()
            .entry(doc)
            .or_default()
            .extend(steps);
        self
    }

    pub fn kit(&self) -> Testkit {
        Testkit::new().module(|engine| self.register_docs(engine))
    }

    pub fn register_docs(&self, engine: &mut EngineBuilder) {
        engine
            .worker(DocIndex {
                script: Arc::clone(&self.script),
            })
            .planner(DocPlanner {
                docs: Arc::clone(&self.docs),
            })
            .reconciler(DocStatus {
                docs: Arc::clone(&self.docs),
                board: self.board.clone(),
            })
            .pool("parse");
    }

    pub fn list_indexed_docs(&self) -> Vec<u64> {
        self.board.indexed.lock().clone()
    }
}

pub fn register_shelves(engine: &mut EngineBuilder) {
    engine.worker(ShelfSplit).planner(ShelfPlanner);
}

pub fn register_summaries(engine: &mut EngineBuilder) {
    engine.worker(SummaryBuild).planner(SummaryPlanner);
}

pub fn tenant() -> Tenant {
    Tenant::new("tenant-1")
}

pub fn doc(id: u64) -> Doc {
    Doc {
        tenant: tenant(),
        id,
        size: 10,
    }
}

pub fn big_doc(id: u64) -> Doc {
    Doc {
        size: BIG_DOC_BYTES + 1,
        ..doc(id)
    }
}

pub fn doc_key(id: u64) -> Key {
    DocIndex::key(&doc(id))
}

pub fn shelf(id: u64, docs: impl IntoIterator<Item = Doc>) -> Shelf {
    Shelf {
        tenant: tenant(),
        id,
        docs: docs.into_iter().collect(),
    }
}

pub fn changed(docs: impl IntoIterator<Item = Doc>) -> Fired {
    let docs: Vec<Doc> = docs.into_iter().collect();
    Fired::Events(EventBatch::encode("docs.changed", &docs).unwrap())
}

pub fn shelved(shelf: &Shelf) -> Fired {
    Fired::Events(EventBatch::encode("shelves.changed", std::slice::from_ref(shelf)).unwrap())
}

pub fn manual(keys: Vec<Key>) -> Fired {
    Fired::Manual {
        tenant: tenant(),
        keys,
    }
}

pub fn sweep(cursor: Option<&str>) -> Fired {
    Fired::Sweep {
        cursor: cursor.map(str::to_owned),
        tag: "rebuild".to_owned(),
    }
}

pub fn hold(pool: &'static str, during: Duration) -> Step {
    Step::Pool { pool, during }
}

impl Script {
    fn take_next_step(&self, doc: u64) -> Step {
        self.steps
            .lock()
            .get_mut(&doc)
            .and_then(VecDeque::pop_front)
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
        match self.script.take_next_step(doc.id) {
            Step::Done { rows } => Ok(Outcome::done(Indexed { doc }).rows("pages", rows)),
            Step::NoChange => Ok(Outcome::no_change()),
            Step::Fail => Err(anyhow::anyhow!("the store did not answer").into()),
            Step::Permanent => Err(TaskError::permanent(anyhow::anyhow!("the doc is corrupt"))),
            Step::Defer(after) => Err(TaskError::defer(after, "not_ready")),
            Step::Throttle(after) => Err(TaskError::throttle(after, "store_busy")),
            Step::Incompatible => Err(TaskError::incompatible("new_doc_format")),
            Step::Sleep(duration) => {
                tokio::time::sleep(duration).await;
                Ok(Outcome::done(Indexed { doc }))
            }
            Step::Pool { pool, during } => {
                let _permit = ctx.pool(pool).await?;
                tokio::time::sleep(during).await;
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
                .docs
                .iter()
                .filter(|doc| keys.contains(&DocIndex::key(doc)))
                .cloned()
                .collect(),
            Fired::Sweep { cursor, .. } => {
                let start: usize = cursor.as_deref().unwrap_or("0").parse()?;
                let end = (start + SWEEP_PAGE).min(self.docs.len());
                ctx.continue_from((end < self.docs.len()).then(|| end.to_string()));
                self.docs[start..end].to_vec()
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
                    let big = shelf.docs.len() > BIG_SHELF_DOCS;
                    let submit = ctx.submit::<ShelfSplit>(shelf.tenant.clone(), shelf);
                    if big {
                        submit.lane(BIG);
                    }
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
        for doc in self.docs.iter() {
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
