mod support;

use indexing_engine::{EngineConfig, LaneConfig};
use support::{
    DocIndex, DocPlanner, HOUR, Library, MINUTE, SECOND, Step, big_doc, changed, doc, doc_key,
    tenant,
};

fn with_default_lane(lane: LaneConfig) -> EngineConfig {
    EngineConfig::default().with_lane("doc.index/default", lane)
}

#[tokio::test(start_paused = true)]
async fn an_error_retries_after_1s_10s_1m_and_5m_then_dead_letters_and_fails_its_job() {
    let kit = Library::default()
        .with_steps(
            1,
            [Step::Fail, Step::Fail, Step::Fail, Step::Fail, Step::Fail],
        )
        .kit();

    kit.fire::<DocPlanner>(changed([doc(1)])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s doc.index/default doc:1 submitted",
            "0s doc.index/default doc:1 started attempt 1",
            "0s doc.index/default doc:1 failed, retry in 1s",
            "1s doc.index/default doc:1 started attempt 2",
            "1s doc.index/default doc:1 failed, retry in 10s",
            "11s doc.index/default doc:1 started attempt 3",
            "11s doc.index/default doc:1 failed, retry in 1m",
            "1m11s doc.index/default doc:1 started attempt 4",
            "1m11s doc.index/default doc:1 failed, retry in 5m",
            "6m11s doc.index/default doc:1 started attempt 5",
            "6m11s doc.index/default doc:1 failed, dead-letter as failed: the store did not answer",
        ]
    );
    let job = kit.last_job::<DocIndex>(&tenant(), &doc_key(1)).unwrap();
    assert!(job.is_failed());
}

#[tokio::test(start_paused = true)]
async fn a_retry_that_succeeds_finishes_the_task() {
    let kit = Library::default().with_steps(1, [Step::Fail]).kit();

    kit.fire::<DocPlanner>(changed([doc(1)])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s doc.index/default doc:1 submitted",
            "0s doc.index/default doc:1 started attempt 1",
            "0s doc.index/default doc:1 failed, retry in 1s",
            "1s doc.index/default doc:1 started attempt 2",
            "1s doc.index/default doc:1 done",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_permanent_error_dead_letters_at_once_and_fails_its_job() {
    let kit = Library::default().with_steps(1, [Step::Permanent]).kit();

    kit.fire::<DocPlanner>(changed([doc(1)])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s doc.index/default doc:1 submitted",
            "0s doc.index/default doc:1 started attempt 1",
            "0s doc.index/default doc:1 permanent error, dead-letter as permanent: the doc is corrupt",
        ]
    );
    let job = kit.last_job::<DocIndex>(&tenant(), &doc_key(1)).unwrap();
    assert!(job.is_failed());
}

#[tokio::test(start_paused = true)]
async fn a_deferred_task_comes_back_after_its_delay_and_uses_no_attempt() {
    let kit = Library::default()
        .with_steps(1, [Step::Defer(30 * SECOND), Step::Defer(30 * SECOND)])
        .kit();

    kit.fire::<DocPlanner>(changed([doc(1)])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s doc.index/default doc:1 submitted",
            "0s doc.index/default doc:1 started attempt 1",
            "0s doc.index/default doc:1 deferred, redeliver in 30s",
            "30s doc.index/default doc:1 started attempt 1",
            "30s doc.index/default doc:1 deferred, redeliver in 30s",
            "1m doc.index/default doc:1 started attempt 1",
            "1m doc.index/default doc:1 done",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_throttle_pauses_only_its_own_lane_until_the_delay_ends_and_uses_no_attempt() {
    let kit = Library::default()
        .with_steps(1, [Step::Throttle(MINUTE)])
        .kit();

    kit.fire::<DocPlanner>(changed([doc(1), doc(2), big_doc(3)]))
        .await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s doc.index/default doc:1 submitted",
            "0s doc.index/default doc:2 submitted",
            "0s doc.index/big doc:3 submitted",
            "0s doc.index/default doc:1 started attempt 1",
            "0s doc.index/big doc:3 started attempt 1",
            "0s doc.index/default doc:1 throttled, pause the lane for 1m",
            "0s doc.index/big doc:3 done",
            "1m doc.index/default doc:2 started attempt 1",
            "1m doc.index/default doc:2 done",
            "1m doc.index/default doc:1 started attempt 1",
            "1m doc.index/default doc:1 done",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn an_incompatible_task_is_redelivered_and_uses_no_attempt() {
    let kit = Library::default().with_steps(1, [Step::Incompatible]).kit();

    kit.fire::<DocPlanner>(changed([doc(1)])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s doc.index/default doc:1 submitted",
            "0s doc.index/default doc:1 started attempt 1",
            "0s doc.index/default doc:1 incompatible, redeliver in 30s",
            "30s doc.index/default doc:1 started attempt 1",
            "30s doc.index/default doc:1 done",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_timed_out_task_moves_to_its_on_timeout_lane_and_uses_an_attempt() {
    let lane = LaneConfig::new(1, SECOND).on_timeout("doc.index/big");
    let kit = Library::default()
        .with_steps(1, [Step::Sleep(5 * SECOND)])
        .kit()
        .config(with_default_lane(lane));

    kit.fire::<DocPlanner>(changed([doc(1)])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s doc.index/default doc:1 submitted",
            "0s doc.index/default doc:1 started attempt 1",
            "1s doc.index/default doc:1 timed out, move to big",
            "1s doc.index/big doc:1 started attempt 2",
            "1s doc.index/big doc:1 done",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_timed_out_task_with_no_on_timeout_lane_retries_and_uses_an_attempt() {
    let kit = Library::default()
        .with_steps(1, [Step::Sleep(5 * SECOND)])
        .kit()
        .config(with_default_lane(LaneConfig::new(1, SECOND)));

    kit.fire::<DocPlanner>(changed([doc(1)])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s doc.index/default doc:1 submitted",
            "0s doc.index/default doc:1 started attempt 1",
            "1s doc.index/default doc:1 timed out, retry in 1s",
            "2s doc.index/default doc:1 started attempt 2",
            "2s doc.index/default doc:1 done",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_task_older_than_its_max_age_dead_letters_as_expired() {
    let lane = LaneConfig {
        max_age: HOUR,
        ..LaneConfig::new(1, MINUTE)
    };
    let kit = Library::default()
        .with_steps(1, (0..4).map(|_| Step::Defer(20 * MINUTE)))
        .kit()
        .config(with_default_lane(lane));

    kit.fire::<DocPlanner>(changed([doc(1)])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s doc.index/default doc:1 submitted",
            "0s doc.index/default doc:1 started attempt 1",
            "0s doc.index/default doc:1 deferred, redeliver in 20m",
            "20m doc.index/default doc:1 started attempt 1",
            "20m doc.index/default doc:1 deferred, redeliver in 20m",
            "40m doc.index/default doc:1 started attempt 1",
            "40m doc.index/default doc:1 deferred, redeliver in 20m",
            "1h doc.index/default doc:1 started attempt 1",
            "1h doc.index/default doc:1 deferred, dead-letter as expired: the task is older than its max age of 1h",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn the_age_of_a_task_counts_from_its_first_start_not_from_its_submit() {
    let lane = LaneConfig {
        max_age: HOUR,
        ..LaneConfig::new(1, 3 * HOUR)
    };
    let kit = Library::default()
        .with_steps(1, [Step::Sleep(2 * HOUR)])
        .with_steps(2, [Step::Defer(MINUTE)])
        .kit()
        .config(with_default_lane(lane));

    kit.fire::<DocPlanner>(changed([doc(1), doc(2)])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s doc.index/default doc:1 submitted",
            "0s doc.index/default doc:2 submitted",
            "0s doc.index/default doc:1 started attempt 1",
            "2h doc.index/default doc:1 done",
            "2h doc.index/default doc:2 started attempt 1",
            "2h doc.index/default doc:2 deferred, redeliver in 1m",
            "2h1m doc.index/default doc:2 started attempt 1",
            "2h1m doc.index/default doc:2 done",
        ]
    );
}
