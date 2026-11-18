mod support;

use async_trait::async_trait;
use indexing_engine::prelude::*;
use indexing_engine::testkit::Testkit;
use support::{
    BIG, DocIndex, DocPlanner, Flows, ShelfPlanner, ShelfSplit, SummaryBuild, changed, doc, shelf,
    shelved, submit_and_run, sweep,
};

struct PinnedSummaryPlanner;

#[async_trait]
impl Planner for PinnedSummaryPlanner {
    const NAME: &'static str = "summary.pinned.planner";

    fn triggers() -> Vec<Trigger> {
        vec![Trigger::done::<DocIndex>()]
    }

    async fn plan(&self, ctx: &mut PlanContext, fired: Fired) -> Result<(), PlanError> {
        match fired {
            Fired::Events(batch) => {
                for indexed in batch.outputs::<DocIndex>()? {
                    ctx.submit::<SummaryBuild>(indexed.doc.tenant.clone(), indexed.doc)
                        .lane(Lane::DEFAULT);
                }
            }
            Fired::Manual { .. } | Fired::Sweep { .. } | Fired::Cron => {}
        }
        Ok(())
    }
}

#[tokio::test]
async fn a_submit_runs_in_the_lane_that_its_planner_picks() {
    let kit = Flows::default().kit();

    let planned = kit
        .plan::<DocPlanner>(changed(&[doc(1, 5_000)]))
        .await
        .unwrap();

    assert_eq!(planned.lane_of::<DocIndex>(), Some(BIG));
}

#[tokio::test]
async fn a_submit_with_no_lane_runs_in_the_default_lane() {
    let kit = Flows::default().kit();

    let planned = kit
        .plan::<ShelfPlanner>(shelved(&shelf(7, vec![doc(1, 10)])))
        .await
        .unwrap();

    assert_eq!(planned.lane_of::<ShelfSplit>(), Some(Lane::DEFAULT));
}

#[tokio::test]
async fn a_submit_during_a_sweep_runs_in_the_backfill_lane_whatever_lane_its_planner_picks() {
    let kit = Flows::default().with_docs(vec![doc(1, 5_000)]).kit();

    let planned = kit.plan::<DocPlanner>(sweep(None)).await.unwrap();

    assert_eq!(planned.lane_of::<DocIndex>(), Some(Lane::BACKFILL));
}

#[tokio::test(start_paused = true)]
async fn a_submit_from_a_done_event_runs_in_the_lane_of_the_source_task() {
    let kit = Flows::default().kit();

    submit_and_run::<DocPlanner>(&kit, changed(&[doc(1, 5_000)])).await;

    let summaries = kit.find_tasks::<SummaryBuild>();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].lane, BIG);
}

#[tokio::test(start_paused = true)]
async fn a_submit_from_a_done_event_runs_in_the_lane_that_its_planner_picks() {
    let flows = Flows::default();
    let kit = Testkit::new()
        .module(|engine| flows.register_docs(engine))
        .module(|engine| {
            engine.worker(SummaryBuild).planner(PinnedSummaryPlanner);
        });

    submit_and_run::<DocPlanner>(&kit, changed(&[doc(1, 5_000)])).await;

    let summaries = kit.find_tasks::<SummaryBuild>();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].lane, Lane::DEFAULT);
}

#[tokio::test(start_paused = true)]
async fn a_spawned_child_runs_in_the_lane_of_its_parent() {
    let kit = Flows::default().kit();
    let docs = (1..=4).map(|id| doc(id, 10)).collect();

    submit_and_run::<ShelfPlanner>(&kit, shelved(&shelf(7, docs))).await;

    let lanes: Vec<Lane> = kit
        .find_tasks::<DocIndex>()
        .into_iter()
        .map(|task| task.lane)
        .collect();
    assert_eq!(lanes, [BIG, BIG, BIG, BIG]);
}
