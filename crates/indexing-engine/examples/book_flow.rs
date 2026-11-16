use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use indexing_engine::prelude::*;
use indexing_engine::testkit::Testkit;
use serde::{Deserialize, Serialize};

const BIG: Lane = Lane::new("big");
const BIG_BOOK_PAGES: u32 = 500;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Book {
    tenant: Tenant,
    id: u64,
    pages: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct Indexed {
    id: u64,
}

struct Catalog {
    books: Vec<Book>,
}

struct BookIndex;

struct BookPlanner {
    catalog: Arc<Catalog>,
}

struct BookFreshness {
    catalog: Arc<Catalog>,
}

#[tokio::main(flavor = "current_thread", start_paused = true)]
async fn main() -> Result<(), PlanError> {
    let catalog = Arc::new(Catalog {
        books: vec![book(1, 120), book(2, 900)],
    });
    let kit = Testkit::new().module(|engine| register(engine, &catalog));

    let changed = EventBatch::encode("books.changed", &catalog.books)?;
    kit.plan_and_submit::<BookPlanner>(Fired::Events(changed))
        .await?;
    kit.run_until_idle().await;

    for book in &catalog.books {
        let job = kit.last_job::<BookIndex>(&book.tenant, &BookIndex::key(book));
        println!("book {}: {job:?}", book.id);
    }
    Ok(())
}

fn register(engine: &mut EngineBuilder, catalog: &Arc<Catalog>) {
    engine
        .worker(BookIndex)
        .planner(BookPlanner {
            catalog: Arc::clone(catalog),
        })
        .reconciler(BookFreshness {
            catalog: Arc::clone(catalog),
        });
}

fn book(id: u64, pages: u32) -> Book {
    Book {
        tenant: Tenant::new("tenant-1"),
        id,
        pages,
    }
}

#[async_trait]
impl Worker for BookIndex {
    const NAME: &'static str = "book.index";
    const EXCLUSIVE: bool = true;
    type Input = Book;
    type Output = Indexed;

    fn key(book: &Book) -> Key {
        Key::new("book", book.id)
    }

    async fn run(&self, _: &TaskContext, book: Book) -> Result<Outcome<Indexed>, TaskError> {
        Ok(Outcome::done(Indexed { id: book.id }).rows("pages", u64::from(book.pages)))
    }
}

#[async_trait]
impl Planner for BookPlanner {
    const NAME: &'static str = "book.planner";

    fn triggers() -> Vec<Trigger> {
        vec![Trigger::Stream {
            subjects: &["books.changed"],
            batch: 500,
            linger: Duration::from_secs(2),
        }]
    }

    async fn plan(&self, ctx: &mut PlanContext, fired: Fired) -> Result<(), PlanError> {
        let books: Vec<Book> = match fired {
            Fired::Events(batch) => batch.decode()?,
            Fired::Manual { keys, .. } => self
                .catalog
                .books
                .iter()
                .filter(|book| keys.contains(&BookIndex::key(book)))
                .cloned()
                .collect(),
            Fired::Sweep { .. } => self.catalog.books.clone(),
            Fired::Cron => return Ok(()),
        };
        for book in books {
            let lane = if book.pages > BIG_BOOK_PAGES {
                BIG
            } else {
                Lane::DEFAULT
            };
            ctx.submit::<BookIndex>(book.tenant.clone(), book)
                .lane(lane);
        }
        Ok(())
    }
}

#[async_trait]
impl Reconciler for BookFreshness {
    const NAME: &'static str = "book.freshness";
    const WRITES: &'static [&'static str] = &[];
    const EVERY: Duration = Duration::from_secs(60);

    async fn reconcile(&self, ctx: &ReconcileContext) -> Result<(), ReconcileError> {
        for book in &self.catalog.books {
            let key = BookIndex::key(book);
            if ctx
                .last_job::<BookIndex>(&book.tenant, &key)
                .await?
                .is_none()
            {
                ctx.replan::<BookPlanner>(&book.tenant, vec![key]).await?;
            }
        }
        Ok(())
    }
}
