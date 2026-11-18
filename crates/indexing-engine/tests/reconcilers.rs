mod support;

use std::time::Duration;

use async_trait::async_trait;
use indexing_engine::prelude::*;
use support::{DocPlanner, DocStatus, Library, doc};

struct DocRebuild;

#[async_trait]
impl Reconciler for DocRebuild {
    const NAME: &'static str = "doc.rebuild";
    const WRITES: &'static [&'static str] = &[];
    const EVERY: Duration = Duration::from_secs(60);

    async fn reconcile(&self, ctx: &ReconcileContext) -> Result<(), ReconcileError> {
        ctx.campaign::<DocPlanner>("rebuild").await
    }
}

#[tokio::test(start_paused = true)]
async fn a_reconciler_asks_a_planner_to_replan_a_key_with_no_job() {
    let library = Library::default().with_docs([doc(1)]);
    let kit = library.kit();

    kit.reconcile::<DocStatus>().await;
    kit.run_until_idle().await;
    kit.reconcile::<DocStatus>().await;

    assert_eq!(library.list_indexed_docs(), [1]);
}

#[tokio::test(start_paused = true)]
async fn a_reconciler_asks_a_planner_to_start_a_campaign() {
    let kit = Library::default()
        .with_docs([doc(1), doc(2), doc(3)])
        .kit()
        .module(|engine| {
            engine.reconciler(DocRebuild);
        });

    kit.reconcile::<DocRebuild>().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s doc.index/backfill doc:1 submitted",
            "0s doc.index/backfill doc:2 submitted",
            "0s doc.index/backfill doc:3 submitted",
        ]
    );
}
