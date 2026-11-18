mod support;

use indexing_engine::testkit::Testkit;
use support::{TestPlanner, TestWorker, sweep};

#[tokio::test]
async fn a_sweep_plans_one_page_and_keeps_the_cursor_of_the_next_page() {
    let kit = Testkit::new().module(|engine| {
        engine.worker(TestWorker::default()).planner(
            TestPlanner::default()
                .with_keys([1, 2, 3])
                .with_page_size(2),
        );
    });

    let planned = kit.plan::<TestPlanner>(sweep()).await;

    assert_eq!(planned.inputs::<TestWorker>(), [1, 2]);
    assert_eq!(planned.next_page(), Some("2"));
}

#[tokio::test(start_paused = true)]
async fn a_campaign_plans_every_page_of_its_planner_in_the_backfill_lane() {
    let kit = Testkit::new().module(|engine| {
        engine.worker(TestWorker::default()).planner(
            TestPlanner::default()
                .with_keys([1, 2, 3])
                .with_page_size(2),
        );
    });

    kit.campaign::<TestPlanner>("rebuild").await;

    assert_eq!(
        kit.timeline(),
        [
            "0s test.worker/backfill task:1 submitted",
            "0s test.worker/backfill task:2 submitted",
            "0s test.worker/backfill task:3 submitted",
        ]
    );
}
