mod support;

use std::time::Duration;

use async_trait::async_trait;
use indexing_engine::prelude::*;
use indexing_engine::testkit::Testkit;
use support::{DocIndex, DocPlanner, DocStatus, Flows, doc};

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
    let flows = Flows::default().with_docs(vec![doc(1, 10)]);
    let kit = flows.kit();

    kit.reconcile::<DocStatus>().await.unwrap();
    kit.run_until_idle().await;
    kit.reconcile::<DocStatus>().await.unwrap();

    assert_eq!(flows.list_indexed_docs(), [1]);
}

#[tokio::test(start_paused = true)]
async fn a_reconciler_asks_a_planner_to_start_a_campaign() {
    let flows = Flows::default().with_docs(vec![doc(1, 10), doc(2, 10), doc(3, 10)]);
    let kit = Testkit::new()
        .module(|engine| flows.register_docs(engine))
        .module(|engine| {
            engine.reconciler(DocRebuild);
        });

    kit.reconcile::<DocRebuild>().await.unwrap();
    kit.run_until_idle().await;

    let tasks = kit.find_tasks::<DocIndex>();
    assert_eq!(tasks.len(), 3);
    assert!(tasks.iter().all(|task| task.lane == Lane::BACKFILL));
}
