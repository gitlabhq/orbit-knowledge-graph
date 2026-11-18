mod support;

use indexing_engine::testkit::Testkit;
use support::{TestPlanner, TestWorker, events};

#[tokio::test(flavor = "multi_thread")]
#[should_panic(expected = "use #[tokio::test(start_paused = true)]")]
async fn the_testkit_refuses_a_runtime_that_runs_tasks_in_no_fixed_order() {
    let kit = Testkit::new().module(|engine| {
        engine
            .worker(TestWorker::default())
            .planner(TestPlanner::default());
    });

    kit.fire::<TestPlanner>(events([1])).await;
    kit.run_until_idle().await;
}
