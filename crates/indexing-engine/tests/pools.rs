mod support;

use std::time::Duration;

use indexing_engine::{ClusterPoolConfig, EngineConfig, LaneConfig, PoolConfig};
use support::{DocPlanner, Flows, Step, changed, doc, find_doc_task, submit_and_run};
use tokio::time::Instant;

fn two_slots_with_pool(pool: PoolConfig) -> EngineConfig {
    EngineConfig::default()
        .with_lane(
            "doc.index/default",
            LaneConfig::new(2, Duration::from_secs(600)),
        )
        .with_pool("parse", pool)
}

fn hold(name: &'static str, seconds: u64) -> Step {
    Step::Pool {
        name,
        hold: Duration::from_secs(seconds),
    }
}

#[tokio::test(start_paused = true)]
async fn a_pod_pool_lets_only_its_size_of_tasks_hold_it_at_once() {
    let kit = Flows::default()
        .with_steps(1, [hold("parse", 10)])
        .with_steps(2, [hold("parse", 10)])
        .kit()
        .config(two_slots_with_pool(PoolConfig::Pod(1)));
    let started = Instant::now();

    submit_and_run::<DocPlanner>(&kit, changed(&[doc(1, 10), doc(2, 10)])).await;

    assert_eq!(find_doc_task(&kit, 2).started_at, Duration::ZERO);
    assert_eq!(started.elapsed(), Duration::from_secs(20));
}

#[tokio::test(start_paused = true)]
async fn a_task_that_waits_past_the_max_wait_of_a_cluster_pool_defers_and_uses_no_attempt() {
    let pool = PoolConfig::Cluster(ClusterPoolConfig {
        cluster: true,
        per_key: 1,
        unknown_key: 1,
        max_wait: Duration::from_secs(10),
    });
    let kit = Flows::default()
        .with_steps(1, [hold("parse", 60)])
        .with_steps(2, [hold("parse", 1)])
        .kit()
        .config(two_slots_with_pool(pool));

    submit_and_run::<DocPlanner>(&kit, changed(&[doc(1, 10), doc(2, 10)])).await;

    let waited = find_doc_task(&kit, 2);
    assert_eq!(waited.attempts, 1);
    assert_eq!(waited.error_class, None);
}

#[tokio::test(start_paused = true)]
async fn a_pool_that_no_module_registered_is_a_permanent_error() {
    let kit = Flows::default().with_steps(1, [hold("missing", 1)]).kit();

    submit_and_run::<DocPlanner>(&kit, changed(&[doc(1, 10)])).await;

    let task = find_doc_task(&kit, 1);
    assert_eq!(task.error_class, Some("permanent"));
    assert_eq!(
        task.error_message.as_deref(),
        Some("no module registered the pool missing")
    );
}

#[tokio::test(start_paused = true)]
async fn a_pool_with_no_size_in_the_config_of_the_pod_is_a_permanent_error() {
    let kit = Flows::default().with_steps(1, [hold("parse", 1)]).kit();

    submit_and_run::<DocPlanner>(&kit, changed(&[doc(1, 10)])).await;

    let task = find_doc_task(&kit, 1);
    assert_eq!(task.error_class, Some("permanent"));
    assert_eq!(
        task.error_message.as_deref(),
        Some("the config of this pod gives no size to the pool parse")
    );
}
