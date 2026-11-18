mod support;

use std::time::Duration;

use async_trait::async_trait;
use indexing_engine_next::prelude::*;
use indexing_engine_next::testkit::Testkit;
use support::{ReplanReconciler, TestPlanner, TestWorker};

struct CampaignReconciler;

#[async_trait]
impl Reconciler for CampaignReconciler {
    const NAME: &'static str = "campaign.reconciler";
    const WRITES: &'static [&'static str] = &[];
    const EVERY: Duration = Duration::from_secs(60);

    async fn reconcile(&self, ctx: &ReconcileContext) -> Result<(), ReconcileError> {
        ctx.campaign::<TestPlanner>("rebuild").await
    }
}

#[tokio::test(start_paused = true)]
async fn a_reconciler_asks_a_planner_to_replan_a_key_with_no_job() {
    let kit = Testkit::new().module(|engine| {
        engine
            .worker(TestWorker::default())
            .planner(TestPlanner::default().with_keys([1]))
            .reconciler(ReplanReconciler::for_keys([1]));
    });

    kit.reconcile::<ReplanReconciler>().await;
    kit.run_until_idle().await;
    kit.reconcile::<ReplanReconciler>().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s test.worker/default task:1 submitted",
            "0s test.worker/default task:1 started attempt 1",
            "0s test.worker/default task:1 done",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_reconciler_asks_a_planner_to_start_a_campaign() {
    let kit = Testkit::new().module(|engine| {
        engine
            .worker(TestWorker::default())
            .planner(TestPlanner::default().with_keys([1, 2, 3]))
            .reconciler(CampaignReconciler);
    });

    kit.reconcile::<CampaignReconciler>().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s test.worker/backfill task:1 submitted",
            "0s test.worker/backfill task:2 submitted",
            "0s test.worker/backfill task:3 submitted",
        ]
    );
}
