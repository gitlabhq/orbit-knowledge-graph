mod support;

use indexing_engine_next::prelude::*;
use indexing_engine_next::testkit::Testkit;
use support::{
    DownstreamPlanner, DownstreamWorker, Run, SECOND, TestPlanner, TestWorker, events, task_key,
    tenant,
};

fn test_flow(worker: TestWorker) -> Testkit {
    Testkit::new().module(|engine| {
        engine.worker(worker).planner(TestPlanner::default());
    })
}

#[tokio::test(start_paused = true)]
async fn spawned_tasks_join_the_job_of_their_parent_and_add_up_their_rows() {
    let worker = TestWorker::default()
        .runs(1, [Run::Spawn(vec![2, 3])])
        .runs(2, [Run::DoneWithRows("pages", 2)])
        .runs(3, [Run::DoneWithRows("pages", 3)]);
    let kit = test_flow(worker);

    kit.fire::<TestPlanner>(events([1])).await;
    kit.run_until_idle().await;

    let job = kit.last_job::<TestWorker>(&tenant(), &task_key(1)).unwrap();
    assert!(job.is_done());
    assert_eq!(job.count_tasks(), 3);
    assert_eq!(job.count_rows("pages"), 5);
}

#[tokio::test(start_paused = true)]
async fn a_job_runs_until_its_last_task_ends() {
    let worker = TestWorker::default()
        .runs(1, [Run::Spawn(vec![2])])
        .runs(2, [Run::Sleep(10 * SECOND)]);
    let kit = test_flow(worker);
    kit.fire::<TestPlanner>(events([1])).await;

    let look_midway = async {
        tokio::time::sleep(5 * SECOND).await;
        kit.last_job::<TestWorker>(&tenant(), &task_key(1)).unwrap()
    };
    let ((), midway) = tokio::join!(kit.run_until_idle(), look_midway);

    assert!(midway.is_running());
    let job = kit.last_job::<TestWorker>(&tenant(), &task_key(1)).unwrap();
    assert!(job.is_done());
}

#[tokio::test(start_paused = true)]
async fn a_spawned_task_that_dead_letters_fails_the_job_of_its_parent() {
    let corrupt = TaskError::permanent(anyhow::anyhow!("the input is corrupt"));
    let worker = TestWorker::default()
        .runs(1, [Run::Spawn(vec![2])])
        .runs(2, [Run::Error(corrupt)]);
    let kit = test_flow(worker);

    kit.fire::<TestPlanner>(events([1])).await;
    kit.run_until_idle().await;

    let job = kit.last_job::<TestWorker>(&tenant(), &task_key(1)).unwrap();
    assert!(job.is_failed());
}

#[tokio::test(start_paused = true)]
async fn an_outcome_that_spawns_more_than_10_000_children_dead_letters_as_permanent() {
    let worker = TestWorker::default().runs(1, [Run::Spawn((2..=10_002).collect())]);
    let kit = test_flow(worker);

    kit.fire::<TestPlanner>(events([1])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s test.worker/default task:1 submitted",
            "0s test.worker/default task:1 started attempt 1",
            "0s test.worker/default task:1 permanent error, dead-letter as permanent: an outcome spawned 10001 children, and the limit is 10000",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_task_with_no_change_writes_no_task_row_and_wakes_no_planner() {
    let kit = Testkit::new().module(|engine| {
        engine
            .worker(TestWorker::default().runs(1, [Run::NoChange]))
            .planner(TestPlanner::default())
            .worker(DownstreamWorker)
            .planner(DownstreamPlanner::default());
    });

    kit.fire::<TestPlanner>(events([1])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s test.worker/default task:1 submitted",
            "0s test.worker/default task:1 started attempt 1",
            "0s test.worker/default task:1 no change",
        ]
    );
    assert!(kit.find_tasks::<TestWorker>().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_tenant_finds_only_the_jobs_of_its_own_keys() {
    let kit = test_flow(TestWorker::default());

    kit.fire::<TestPlanner>(events([1])).await;
    kit.run_until_idle().await;

    let other = Tenant::new("tenant-2");
    assert!(
        kit.last_job::<TestWorker>(&tenant(), &task_key(1))
            .is_some()
    );
    assert_eq!(kit.last_job::<TestWorker>(&other, &task_key(1)), None);
}
