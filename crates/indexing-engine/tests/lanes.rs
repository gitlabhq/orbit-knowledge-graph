mod support;

use indexing_engine::prelude::*;
use indexing_engine::testkit::Testkit;
use support::{
    BIG, DownstreamPlanner, DownstreamWorker, Run, TestPlanner, TestWorker, events, sweep,
};

#[tokio::test(start_paused = true)]
async fn a_submit_runs_in_the_lane_that_its_planner_picks() {
    let kit = Testkit::new().module(|engine| {
        engine
            .worker(TestWorker::default())
            .planner(TestPlanner::default().with_lane(1, BIG));
    });

    kit.fire::<TestPlanner>(events([1])).await;

    assert_eq!(kit.timeline(), ["0s test.worker/big task:1 submitted"]);
}

#[tokio::test(start_paused = true)]
async fn a_submit_with_no_lane_runs_in_the_default_lane() {
    let kit = Testkit::new().module(|engine| {
        engine
            .worker(TestWorker::default())
            .planner(TestPlanner::default());
    });

    kit.fire::<TestPlanner>(events([1])).await;

    assert_eq!(kit.timeline(), ["0s test.worker/default task:1 submitted"]);
}

#[tokio::test(start_paused = true)]
async fn a_submit_during_a_sweep_runs_in_the_backfill_lane_whatever_lane_its_planner_picks() {
    let kit = Testkit::new().module(|engine| {
        engine
            .worker(TestWorker::default())
            .planner(TestPlanner::default().with_keys([1]).with_lane(1, BIG));
    });

    kit.fire::<TestPlanner>(sweep()).await;

    assert_eq!(kit.timeline(), ["0s test.worker/backfill task:1 submitted"]);
}

#[tokio::test(start_paused = true)]
async fn a_submit_from_a_done_event_runs_in_the_lane_of_the_source_task() {
    let kit = Testkit::new().module(|engine| {
        engine
            .worker(TestWorker::default())
            .planner(TestPlanner::default().with_lane(1, BIG))
            .worker(DownstreamWorker)
            .planner(DownstreamPlanner::default());
    });

    kit.fire::<TestPlanner>(events([1])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s test.worker/big task:1 submitted",
            "0s test.worker/big task:1 started attempt 1",
            "0s test.worker/big task:1 done",
            "0s downstream.worker/big task:1 submitted",
            "0s downstream.worker/big task:1 started attempt 1",
            "0s downstream.worker/big task:1 done",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_submit_from_a_done_event_runs_in_the_lane_that_its_planner_picks() {
    let kit = Testkit::new().module(|engine| {
        engine
            .worker(TestWorker::default())
            .planner(TestPlanner::default().with_lane(1, BIG))
            .worker(DownstreamWorker)
            .planner(DownstreamPlanner::in_lane(Lane::DEFAULT));
    });

    kit.fire::<TestPlanner>(events([1])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s test.worker/big task:1 submitted",
            "0s test.worker/big task:1 started attempt 1",
            "0s test.worker/big task:1 done",
            "0s downstream.worker/default task:1 submitted",
            "0s downstream.worker/default task:1 started attempt 1",
            "0s downstream.worker/default task:1 done",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_spawned_child_runs_in_the_lane_of_its_parent() {
    let kit = Testkit::new().module(|engine| {
        engine
            .worker(TestWorker::default().runs(1, [Run::Spawn(vec![2, 3])]))
            .planner(TestPlanner::default().with_lane(1, BIG));
    });

    kit.fire::<TestPlanner>(events([1])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s test.worker/big task:1 submitted",
            "0s test.worker/big task:1 started attempt 1",
            "0s test.worker/big task:1 done",
            "0s test.worker/big task:2 spawned",
            "0s test.worker/big task:3 spawned",
            "0s test.worker/big task:2 started attempt 1",
            "0s test.worker/big task:2 done",
            "0s test.worker/big task:3 started attempt 1",
            "0s test.worker/big task:3 done",
        ]
    );
}
