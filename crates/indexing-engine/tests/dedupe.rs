mod support;

use indexing_engine::testkit::Testkit;
use indexing_engine::{EngineConfig, LaneConfig};
use support::{
    ExclusivePlanner, ExclusiveWorker, MINUTE, Run, SECOND, TestPlanner, TestWorker, events, manual,
};

#[tokio::test(start_paused = true)]
async fn many_submits_for_one_key_before_it_starts_give_one_task() {
    let kit = Testkit::new().module(|engine| {
        engine
            .worker(TestWorker::default())
            .planner(TestPlanner::default());
    });

    kit.fire::<TestPlanner>(events([1, 1])).await;
    kit.fire::<TestPlanner>(events([1])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s test.worker/default task:1 submitted",
            "0s test.worker/default task:1 already pending",
            "0s test.worker/default task:1 already pending",
            "0s test.worker/default task:1 started attempt 1",
            "0s test.worker/default task:1 done",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn many_manual_requests_for_one_key_give_one_run() {
    let kit = Testkit::new().module(|engine| {
        engine
            .worker(TestWorker::default())
            .planner(TestPlanner::default().with_keys([1]));
    });

    for _ in 0..3 {
        kit.fire::<TestPlanner>(manual([1])).await;
    }
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s test.worker/default task:1 submitted",
            "0s test.worker/default task:1 already pending",
            "0s test.worker/default task:1 already pending",
            "0s test.worker/default task:1 started attempt 1",
            "0s test.worker/default task:1 done",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_submit_while_the_task_runs_queues_one_more_task() {
    let kit = Testkit::new().module(|engine| {
        engine
            .worker(TestWorker::default().runs(1, [Run::Sleep(5 * SECOND)]))
            .planner(TestPlanner::default());
    });
    kit.fire::<TestPlanner>(events([1])).await;

    let submit_again = async {
        tokio::time::sleep(SECOND).await;
        kit.fire::<TestPlanner>(events([1])).await;
    };
    tokio::join!(kit.run_until_idle(), submit_again);

    assert_eq!(
        kit.timeline(),
        [
            "0s test.worker/default task:1 submitted",
            "0s test.worker/default task:1 started attempt 1",
            "1s test.worker/default task:1 submitted",
            "5s test.worker/default task:1 done",
            "5s test.worker/default task:1 started attempt 1",
            "5s test.worker/default task:1 done",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn two_runs_of_an_exclusive_key_never_overlap_but_other_keys_run_beside_it() {
    let two_slots = LaneConfig::new(2, MINUTE);
    let kit = Testkit::new()
        .config(EngineConfig::default().with_lane("exclusive.worker/default", two_slots))
        .module(|engine| {
            engine
                .worker(ExclusiveWorker::default().runs(1, [Run::Sleep(5 * SECOND)]))
                .planner(ExclusivePlanner::default());
        });
    kit.fire::<ExclusivePlanner>(events([1])).await;

    let submit_again = async {
        tokio::time::sleep(SECOND).await;
        kit.fire::<ExclusivePlanner>(events([1, 2])).await;
    };
    tokio::join!(kit.run_until_idle(), submit_again);

    assert_eq!(
        kit.timeline(),
        [
            "0s exclusive.worker/default task:1 submitted",
            "0s exclusive.worker/default task:1 started attempt 1",
            "1s exclusive.worker/default task:1 submitted",
            "1s exclusive.worker/default task:2 submitted",
            "1s exclusive.worker/default task:2 started attempt 1",
            "1s exclusive.worker/default task:2 done",
            "5s exclusive.worker/default task:1 done",
            "5s exclusive.worker/default task:1 started attempt 1",
            "5s exclusive.worker/default task:1 done",
        ]
    );
}
