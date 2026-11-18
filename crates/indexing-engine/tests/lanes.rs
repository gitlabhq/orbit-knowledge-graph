mod support;

use async_trait::async_trait;
use indexing_engine::prelude::*;
use support::{
    DocIndex, DocPlanner, Library, ShelfPlanner, SummaryBuild, big_doc, changed, doc,
    register_shelves, register_summaries, shelf, shelved, sweep,
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

#[tokio::test(start_paused = true)]
async fn a_submit_runs_in_the_lane_that_its_planner_picks() {
    let kit = Library::default().kit();

    kit.fire::<DocPlanner>(changed([big_doc(1)])).await;

    assert_eq!(kit.timeline(), ["0s doc.index/big doc:1 submitted"]);
}

#[tokio::test(start_paused = true)]
async fn a_submit_with_no_lane_runs_in_the_default_lane() {
    let kit = Library::default().kit().module(register_shelves);

    kit.fire::<ShelfPlanner>(shelved(&shelf(7, [doc(1)]))).await;

    assert_eq!(kit.timeline(), ["0s shelf.split/default shelf:7 submitted"]);
}

#[tokio::test(start_paused = true)]
async fn a_submit_during_a_sweep_runs_in_the_backfill_lane_whatever_lane_its_planner_picks() {
    let kit = Library::default().with_docs([big_doc(1)]).kit();

    kit.fire::<DocPlanner>(sweep(None)).await;

    assert_eq!(kit.timeline(), ["0s doc.index/backfill doc:1 submitted"]);
}

#[tokio::test(start_paused = true)]
async fn a_submit_from_a_done_event_runs_in_the_lane_of_the_source_task() {
    let kit = Library::default().kit().module(register_summaries);

    kit.fire::<DocPlanner>(changed([big_doc(1)])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s doc.index/big doc:1 submitted",
            "0s doc.index/big doc:1 started attempt 1",
            "0s doc.index/big doc:1 done",
            "0s summary.build/big summary:1 submitted",
            "0s summary.build/big summary:1 started attempt 1",
            "0s summary.build/big summary:1 done",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_submit_from_a_done_event_runs_in_the_lane_that_its_planner_picks() {
    let kit = Library::default().kit().module(|engine| {
        engine.worker(SummaryBuild).planner(PinnedSummaryPlanner);
    });

    kit.fire::<DocPlanner>(changed([big_doc(1)])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s doc.index/big doc:1 submitted",
            "0s doc.index/big doc:1 started attempt 1",
            "0s doc.index/big doc:1 done",
            "0s summary.build/default summary:1 submitted",
            "0s summary.build/default summary:1 started attempt 1",
            "0s summary.build/default summary:1 done",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_spawned_child_runs_in_the_lane_of_its_parent() {
    let kit = Library::default().kit().module(register_shelves);

    kit.fire::<ShelfPlanner>(shelved(&shelf(7, [doc(1), doc(2)])))
        .await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s shelf.split/big shelf:7 submitted",
            "0s shelf.split/big shelf:7 started attempt 1",
            "0s shelf.split/big shelf:7 done",
            "0s doc.index/big doc:1 spawned",
            "0s doc.index/big doc:2 spawned",
            "0s doc.index/big doc:1 started attempt 1",
            "0s doc.index/big doc:1 done",
            "0s doc.index/big doc:2 started attempt 1",
            "0s doc.index/big doc:2 done",
        ]
    );
}
