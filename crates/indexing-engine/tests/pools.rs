mod support;

use indexing_engine::testkit::Testkit;
use indexing_engine::{ClusterPoolConfig, EngineConfig, LaneConfig, PoolConfig};
use support::{MINUTE, Run, SECOND, TestPlanner, TestWorker, events};

fn two_slots_and_a_parse_pool(pool: PoolConfig) -> EngineConfig {
    EngineConfig::default()
        .with_lane("test.worker/default", LaneConfig::new(2, 10 * MINUTE))
        .with_pool("parse", pool)
}

fn test_flow_with_parse_pool(config: EngineConfig, worker: TestWorker) -> Testkit {
    Testkit::new().config(config).module(|engine| {
        engine
            .worker(worker)
            .planner(TestPlanner::default())
            .pool("parse");
    })
}

#[tokio::test(start_paused = true)]
async fn a_pod_pool_lets_only_its_size_of_tasks_hold_it_at_once() {
    let worker = TestWorker::default()
        .runs(1, [Run::HoldPool("parse", 10 * SECOND)])
        .runs(2, [Run::HoldPool("parse", 10 * SECOND)]);
    let config = two_slots_and_a_parse_pool(PoolConfig::Pod(1));
    let kit = test_flow_with_parse_pool(config, worker);

    kit.fire::<TestPlanner>(events([1, 2])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s test.worker/default task:1 submitted",
            "0s test.worker/default task:2 submitted",
            "0s test.worker/default task:1 started attempt 1",
            "0s test.worker/default task:2 started attempt 1",
            "10s test.worker/default task:1 done",
            "20s test.worker/default task:2 done",
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
    let worker = TestWorker::default()
        .runs(1, [Run::HoldPool("parse", MINUTE)])
        .runs(2, [Run::HoldPool("parse", SECOND)]);
    let kit = test_flow_with_parse_pool(two_slots_and_a_parse_pool(pool), worker);

    kit.fire::<TestPlanner>(events([1, 2])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s test.worker/default task:1 submitted",
            "0s test.worker/default task:2 submitted",
            "0s test.worker/default task:1 started attempt 1",
            "0s test.worker/default task:2 started attempt 1",
            "10s test.worker/default task:2 deferred, redeliver in 10s",
            "20s test.worker/default task:2 started attempt 1",
            "20s test.worker/default task:2 done",
            "1m test.worker/default task:1 done",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_pool_that_no_module_registered_is_a_permanent_error() {
    let worker = TestWorker::default().runs(1, [Run::HoldPool("missing", SECOND)]);
    let kit = test_flow_with_parse_pool(EngineConfig::default(), worker);

    kit.fire::<TestPlanner>(events([1])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s test.worker/default task:1 submitted",
            "0s test.worker/default task:1 started attempt 1",
            "0s test.worker/default task:1 permanent error, dead-letter as permanent: no module registered the pool missing",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_pool_with_no_size_in_the_config_of_the_pod_is_a_permanent_error() {
    let worker = TestWorker::default().runs(1, [Run::HoldPool("parse", SECOND)]);
    let kit = test_flow_with_parse_pool(EngineConfig::default(), worker);

    kit.fire::<TestPlanner>(events([1])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s test.worker/default task:1 submitted",
            "0s test.worker/default task:1 started attempt 1",
            "0s test.worker/default task:1 permanent error, dead-letter as permanent: the config of this pod gives no size to the pool parse",
        ]
    );
}
