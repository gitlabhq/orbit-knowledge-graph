mod support;

use support::{DocPlanner, Library, changed, doc};

#[tokio::test(flavor = "multi_thread")]
#[should_panic(expected = "use #[tokio::test(start_paused = true)]")]
async fn the_testkit_refuses_a_runtime_that_runs_tasks_in_no_fixed_order() {
    let kit = Library::default().kit();

    kit.fire::<DocPlanner>(changed([doc(1)])).await;
    kit.run_until_idle().await;
}
