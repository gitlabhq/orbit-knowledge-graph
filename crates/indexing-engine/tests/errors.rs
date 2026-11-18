mod support;

use std::time::Duration;

use indexing_engine::{EngineConfig, LaneConfig};
use support::{
    BIG, DocIndex, DocPlanner, Flows, Step, changed, doc, doc_key, find_doc_task, submit_and_run,
    tenant,
};
use tokio::time::Instant;

const MINUTE: Duration = Duration::from_secs(60);
const HOUR: Duration = Duration::from_secs(60 * 60);

fn default_lane(lane: LaneConfig) -> EngineConfig {
    EngineConfig::default().with_lane("doc.index/default", lane)
}

#[tokio::test(start_paused = true)]
async fn an_error_retries_after_1s_10s_60s_and_300s_then_dead_letters_and_fails_its_job() {
    let kit = Flows::default()
        .with_steps(1, (0..5).map(|_| Step::Fail))
        .kit();
    let started = Instant::now();

    submit_and_run::<DocPlanner>(&kit, changed(&[doc(1, 10)])).await;

    let task = find_doc_task(&kit, 1);
    assert_eq!(task.attempts, 5);
    assert_eq!(task.error_class, Some("failed"));
    assert_eq!(started.elapsed(), Duration::from_secs(1 + 10 + 60 + 300));
    let job = kit.last_job::<DocIndex>(&tenant(), &doc_key(1)).unwrap();
    assert!(job.is_failed());
}

#[tokio::test(start_paused = true)]
async fn a_retry_that_succeeds_finishes_the_task() {
    let kit = Flows::default().with_steps(1, [Step::Fail]).kit();

    submit_and_run::<DocPlanner>(&kit, changed(&[doc(1, 10)])).await;

    let task = find_doc_task(&kit, 1);
    assert_eq!(task.attempts, 2);
    assert_eq!(task.error_class, None);
}

#[tokio::test(start_paused = true)]
async fn a_permanent_error_dead_letters_at_once_and_fails_its_job() {
    let kit = Flows::default().with_steps(1, [Step::Permanent]).kit();

    submit_and_run::<DocPlanner>(&kit, changed(&[doc(1, 10)])).await;

    let task = find_doc_task(&kit, 1);
    assert_eq!(task.attempts, 1);
    assert_eq!(task.error_class, Some("permanent"));
    let job = kit.last_job::<DocIndex>(&tenant(), &doc_key(1)).unwrap();
    assert!(job.is_failed());
}

#[tokio::test(start_paused = true)]
async fn a_deferred_task_comes_back_after_its_delay_and_uses_no_attempt() {
    let kit = Flows::default()
        .with_steps(1, (0..8).map(|_| Step::Defer(Duration::from_secs(30))))
        .kit();
    let started = Instant::now();

    submit_and_run::<DocPlanner>(&kit, changed(&[doc(1, 10)])).await;

    let task = find_doc_task(&kit, 1);
    assert_eq!(task.attempts, 1);
    assert_eq!(task.error_class, None);
    assert_eq!(started.elapsed(), 8 * Duration::from_secs(30));
}

#[tokio::test(start_paused = true)]
async fn a_throttle_pauses_its_lane_until_the_delay_ends_and_uses_no_attempt() {
    let kit = Flows::default()
        .with_steps(1, [Step::Throttle(MINUTE)])
        .kit();

    submit_and_run::<DocPlanner>(&kit, changed(&[doc(1, 10), doc(2, 10)])).await;

    assert_eq!(find_doc_task(&kit, 1).attempts, 1);
    assert_eq!(find_doc_task(&kit, 2).started_at, MINUTE);
}

#[tokio::test(start_paused = true)]
async fn a_throttle_does_not_pause_the_other_lanes() {
    let kit = Flows::default()
        .with_steps(1, [Step::Throttle(MINUTE)])
        .kit();

    submit_and_run::<DocPlanner>(&kit, changed(&[doc(1, 10), doc(2, 5_000)])).await;

    let big = find_doc_task(&kit, 2);
    assert_eq!(big.lane, BIG);
    assert_eq!(big.started_at, Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn an_incompatible_task_is_redelivered_and_uses_no_attempt() {
    let kit = Flows::default().with_steps(1, [Step::Incompatible]).kit();

    submit_and_run::<DocPlanner>(&kit, changed(&[doc(1, 10)])).await;

    let task = find_doc_task(&kit, 1);
    assert_eq!(task.attempts, 1);
    assert_eq!(task.error_class, None);
}

#[tokio::test(start_paused = true)]
async fn a_timed_out_task_moves_to_its_on_timeout_lane_and_uses_an_attempt() {
    let lane = LaneConfig::new(1, Duration::from_secs(1)).on_timeout("doc.index/big");
    let kit = Flows::default()
        .with_steps(1, [Step::Sleep(Duration::from_secs(5))])
        .kit()
        .config(default_lane(lane));

    submit_and_run::<DocPlanner>(&kit, changed(&[doc(1, 10)])).await;

    let task = find_doc_task(&kit, 1);
    assert_eq!(task.lane, BIG);
    assert_eq!(task.attempts, 2);
}

#[tokio::test(start_paused = true)]
async fn a_timed_out_task_with_no_on_timeout_lane_retries_and_uses_an_attempt() {
    let lane = LaneConfig::new(1, Duration::from_secs(1));
    let kit = Flows::default()
        .with_steps(1, [Step::Sleep(Duration::from_secs(5))])
        .kit()
        .config(default_lane(lane));

    submit_and_run::<DocPlanner>(&kit, changed(&[doc(1, 10)])).await;

    let task = find_doc_task(&kit, 1);
    assert_eq!(task.attempts, 2);
    assert_eq!(task.error_class, None);
}

#[tokio::test(start_paused = true)]
async fn a_task_older_than_its_max_age_dead_letters_as_expired() {
    let lane = LaneConfig {
        max_age: HOUR,
        ..LaneConfig::new(1, MINUTE)
    };
    let kit = Flows::default()
        .with_steps(1, (0..5).map(|_| Step::Defer(20 * MINUTE)))
        .kit()
        .config(default_lane(lane));

    submit_and_run::<DocPlanner>(&kit, changed(&[doc(1, 10)])).await;

    assert_eq!(find_doc_task(&kit, 1).error_class, Some("expired"));
}

#[tokio::test(start_paused = true)]
async fn the_age_of_a_task_counts_from_its_first_start_not_from_its_submit() {
    let lane = LaneConfig {
        max_age: HOUR,
        ..LaneConfig::new(1, 3 * HOUR)
    };
    let kit = Flows::default()
        .with_steps(1, [Step::Sleep(2 * HOUR)])
        .with_steps(2, [Step::Defer(MINUTE)])
        .kit()
        .config(default_lane(lane));

    submit_and_run::<DocPlanner>(&kit, changed(&[doc(1, 10), doc(2, 10)])).await;

    let waited = find_doc_task(&kit, 2);
    assert_eq!(waited.started_at, 2 * HOUR);
    assert_eq!(waited.error_class, None);
}
