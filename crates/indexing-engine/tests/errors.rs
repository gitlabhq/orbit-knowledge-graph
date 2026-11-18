mod support;

use anyhow::anyhow;
use indexing_engine::prelude::*;
use indexing_engine::testkit::Testkit;
use indexing_engine::{EngineConfig, LaneConfig};
use support::{BIG, HOUR, MINUTE, Run, SECOND, TestPlanner, TestWorker, events, task_key, tenant};

fn store_did_not_answer() -> Run {
    Run::Error(anyhow!("the store did not answer").into())
}

fn test_flow(config: EngineConfig, worker: TestWorker) -> Testkit {
    Testkit::new().config(config).module(|engine| {
        engine
            .worker(worker)
            .planner(TestPlanner::default().with_lane(3, BIG));
    })
}

fn with_default_lane(lane: LaneConfig) -> EngineConfig {
    EngineConfig::default().with_lane("test.worker/default", lane)
}

#[tokio::test(start_paused = true)]
async fn an_error_retries_after_1s_10s_1m_and_5m_then_dead_letters_and_fails_its_job() {
    let worker = TestWorker::default().runs(1, (0..5).map(|_| store_did_not_answer()));
    let kit = test_flow(EngineConfig::default(), worker);

    kit.fire::<TestPlanner>(events([1])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s test.worker/default task:1 submitted",
            "0s test.worker/default task:1 started attempt 1",
            "0s test.worker/default task:1 failed, retry in 1s",
            "1s test.worker/default task:1 started attempt 2",
            "1s test.worker/default task:1 failed, retry in 10s",
            "11s test.worker/default task:1 started attempt 3",
            "11s test.worker/default task:1 failed, retry in 1m",
            "1m11s test.worker/default task:1 started attempt 4",
            "1m11s test.worker/default task:1 failed, retry in 5m",
            "6m11s test.worker/default task:1 started attempt 5",
            "6m11s test.worker/default task:1 failed, dead-letter as failed: the store did not answer",
        ]
    );
    let job = kit.last_job::<TestWorker>(&tenant(), &task_key(1)).unwrap();
    assert!(job.is_failed());
}

#[tokio::test(start_paused = true)]
async fn a_retry_that_succeeds_finishes_the_task() {
    let worker = TestWorker::default().runs(1, [store_did_not_answer(), Run::Done]);
    let kit = test_flow(EngineConfig::default(), worker);

    kit.fire::<TestPlanner>(events([1])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s test.worker/default task:1 submitted",
            "0s test.worker/default task:1 started attempt 1",
            "0s test.worker/default task:1 failed, retry in 1s",
            "1s test.worker/default task:1 started attempt 2",
            "1s test.worker/default task:1 done",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_permanent_error_dead_letters_at_once_and_fails_its_job() {
    let corrupt = TaskError::permanent(anyhow!("the input is corrupt"));
    let worker = TestWorker::default().runs(1, [Run::Error(corrupt)]);
    let kit = test_flow(EngineConfig::default(), worker);

    kit.fire::<TestPlanner>(events([1])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s test.worker/default task:1 submitted",
            "0s test.worker/default task:1 started attempt 1",
            "0s test.worker/default task:1 permanent error, dead-letter as permanent: the input is corrupt",
        ]
    );
    let job = kit.last_job::<TestWorker>(&tenant(), &task_key(1)).unwrap();
    assert!(job.is_failed());
}

#[tokio::test(start_paused = true)]
async fn a_deferred_task_comes_back_after_its_delay_and_uses_no_attempt() {
    let not_ready = || Run::Error(TaskError::defer(30 * SECOND, "not_ready"));
    let worker = TestWorker::default().runs(1, [not_ready(), not_ready()]);
    let kit = test_flow(EngineConfig::default(), worker);

    kit.fire::<TestPlanner>(events([1])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s test.worker/default task:1 submitted",
            "0s test.worker/default task:1 started attempt 1",
            "0s test.worker/default task:1 deferred, redeliver in 30s",
            "30s test.worker/default task:1 started attempt 1",
            "30s test.worker/default task:1 deferred, redeliver in 30s",
            "1m test.worker/default task:1 started attempt 1",
            "1m test.worker/default task:1 done",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_throttle_pauses_only_its_own_lane_until_the_delay_ends_and_uses_no_attempt() {
    let store_busy = TaskError::throttle(MINUTE, "store_busy");
    let worker = TestWorker::default().runs(1, [Run::Error(store_busy)]);
    let kit = test_flow(EngineConfig::default(), worker);

    kit.fire::<TestPlanner>(events([1, 2, 3])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s test.worker/default task:1 submitted",
            "0s test.worker/default task:2 submitted",
            "0s test.worker/big task:3 submitted",
            "0s test.worker/default task:1 started attempt 1",
            "0s test.worker/big task:3 started attempt 1",
            "0s test.worker/default task:1 throttled, pause the lane for 1m",
            "0s test.worker/big task:3 done",
            "1m test.worker/default task:2 started attempt 1",
            "1m test.worker/default task:2 done",
            "1m test.worker/default task:1 started attempt 1",
            "1m test.worker/default task:1 done",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn an_incompatible_task_is_redelivered_and_uses_no_attempt() {
    let new_format = TaskError::incompatible("new_input_format");
    let worker = TestWorker::default().runs(1, [Run::Error(new_format)]);
    let kit = test_flow(EngineConfig::default(), worker);

    kit.fire::<TestPlanner>(events([1])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s test.worker/default task:1 submitted",
            "0s test.worker/default task:1 started attempt 1",
            "0s test.worker/default task:1 incompatible, redeliver in 30s",
            "30s test.worker/default task:1 started attempt 1",
            "30s test.worker/default task:1 done",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_timed_out_task_moves_to_its_on_timeout_lane_and_uses_an_attempt() {
    let lane = LaneConfig::new(1, SECOND).on_timeout("test.worker/big");
    let worker = TestWorker::default().runs(1, [Run::Sleep(5 * SECOND)]);
    let kit = test_flow(with_default_lane(lane), worker);

    kit.fire::<TestPlanner>(events([1])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s test.worker/default task:1 submitted",
            "0s test.worker/default task:1 started attempt 1",
            "1s test.worker/default task:1 timed out, move to big",
            "1s test.worker/big task:1 started attempt 2",
            "1s test.worker/big task:1 done",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_timed_out_task_with_no_on_timeout_lane_retries_and_uses_an_attempt() {
    let lane = LaneConfig::new(1, SECOND);
    let worker = TestWorker::default().runs(1, [Run::Sleep(5 * SECOND)]);
    let kit = test_flow(with_default_lane(lane), worker);

    kit.fire::<TestPlanner>(events([1])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s test.worker/default task:1 submitted",
            "0s test.worker/default task:1 started attempt 1",
            "1s test.worker/default task:1 timed out, retry in 1s",
            "2s test.worker/default task:1 started attempt 2",
            "2s test.worker/default task:1 done",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_task_older_than_its_max_age_dead_letters_as_expired() {
    let lane = LaneConfig {
        max_age: HOUR,
        ..LaneConfig::new(1, MINUTE)
    };
    let not_ready = || Run::Error(TaskError::defer(20 * MINUTE, "not_ready"));
    let worker = TestWorker::default().runs(1, (0..4).map(|_| not_ready()));
    let kit = test_flow(with_default_lane(lane), worker);

    kit.fire::<TestPlanner>(events([1])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s test.worker/default task:1 submitted",
            "0s test.worker/default task:1 started attempt 1",
            "0s test.worker/default task:1 deferred, redeliver in 20m",
            "20m test.worker/default task:1 started attempt 1",
            "20m test.worker/default task:1 deferred, redeliver in 20m",
            "40m test.worker/default task:1 started attempt 1",
            "40m test.worker/default task:1 deferred, redeliver in 20m",
            "1h test.worker/default task:1 started attempt 1",
            "1h test.worker/default task:1 deferred, dead-letter as expired: the task is older than its max age of 1h",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn the_age_of_a_task_counts_from_its_first_start_not_from_its_submit() {
    let lane = LaneConfig {
        max_age: HOUR,
        ..LaneConfig::new(1, 3 * HOUR)
    };
    let not_ready = TaskError::defer(MINUTE, "not_ready");
    let worker = TestWorker::default()
        .runs(1, [Run::Sleep(2 * HOUR)])
        .runs(2, [Run::Error(not_ready)]);
    let kit = test_flow(with_default_lane(lane), worker);

    kit.fire::<TestPlanner>(events([1, 2])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s test.worker/default task:1 submitted",
            "0s test.worker/default task:2 submitted",
            "0s test.worker/default task:1 started attempt 1",
            "2h test.worker/default task:1 done",
            "2h test.worker/default task:2 started attempt 1",
            "2h test.worker/default task:2 deferred, redeliver in 1m",
            "2h1m test.worker/default task:2 started attempt 1",
            "2h1m test.worker/default task:2 done",
        ]
    );
}
