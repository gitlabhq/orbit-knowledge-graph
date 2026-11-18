mod support;

use indexing_engine::{ClusterPoolConfig, EngineConfig, LaneConfig, PoolConfig};
use support::{DocPlanner, Library, MINUTE, SECOND, changed, doc, hold};

fn two_slots_with_parse_pool(pool: PoolConfig) -> EngineConfig {
    EngineConfig::default()
        .with_lane("doc.index/default", LaneConfig::new(2, 10 * MINUTE))
        .with_pool("parse", pool)
}

#[tokio::test(start_paused = true)]
async fn a_pod_pool_lets_only_its_size_of_tasks_hold_it_at_once() {
    let kit = Library::default()
        .with_steps(1, [hold("parse", 10 * SECOND)])
        .with_steps(2, [hold("parse", 10 * SECOND)])
        .kit()
        .config(two_slots_with_parse_pool(PoolConfig::Pod(1)));

    kit.fire::<DocPlanner>(changed([doc(1), doc(2)])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s doc.index/default doc:1 submitted",
            "0s doc.index/default doc:2 submitted",
            "0s doc.index/default doc:1 started attempt 1",
            "0s doc.index/default doc:2 started attempt 1",
            "10s doc.index/default doc:1 done",
            "20s doc.index/default doc:2 done",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_task_that_waits_past_the_max_wait_of_a_cluster_pool_defers_and_uses_no_attempt() {
    let pool = PoolConfig::Cluster(ClusterPoolConfig {
        cluster: true,
        per_key: 1,
        unknown_key: 1,
        max_wait: 10 * SECOND,
    });
    let kit = Library::default()
        .with_steps(1, [hold("parse", MINUTE)])
        .with_steps(2, [hold("parse", SECOND)])
        .kit()
        .config(two_slots_with_parse_pool(pool));

    kit.fire::<DocPlanner>(changed([doc(1), doc(2)])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s doc.index/default doc:1 submitted",
            "0s doc.index/default doc:2 submitted",
            "0s doc.index/default doc:1 started attempt 1",
            "0s doc.index/default doc:2 started attempt 1",
            "10s doc.index/default doc:2 deferred, redeliver in 10s",
            "20s doc.index/default doc:2 started attempt 1",
            "20s doc.index/default doc:2 done",
            "1m doc.index/default doc:1 done",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_pool_that_no_module_registered_is_a_permanent_error() {
    let kit = Library::default()
        .with_steps(1, [hold("missing", SECOND)])
        .kit();

    kit.fire::<DocPlanner>(changed([doc(1)])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s doc.index/default doc:1 submitted",
            "0s doc.index/default doc:1 started attempt 1",
            "0s doc.index/default doc:1 permanent error, dead-letter as permanent: no module registered the pool missing",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_pool_with_no_size_in_the_config_of_the_pod_is_a_permanent_error() {
    let kit = Library::default()
        .with_steps(1, [hold("parse", SECOND)])
        .kit();

    kit.fire::<DocPlanner>(changed([doc(1)])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s doc.index/default doc:1 submitted",
            "0s doc.index/default doc:1 started attempt 1",
            "0s doc.index/default doc:1 permanent error, dead-letter as permanent: the config of this pod gives no size to the pool parse",
        ]
    );
}
