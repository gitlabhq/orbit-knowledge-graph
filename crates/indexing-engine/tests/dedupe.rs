mod support;

use std::time::Duration;

use indexing_engine::{EngineConfig, LaneConfig};
use support::{
    DocPlanner, Flows, Step, changed, doc, doc_key, find_doc_tasks, manual, submit_and_run,
};

#[tokio::test(start_paused = true)]
async fn many_submits_for_one_key_before_it_starts_give_one_task() {
    let kit = Flows::default().kit();

    kit.plan_and_submit::<DocPlanner>(changed(&[doc(1, 10), doc(1, 10)]))
        .await
        .unwrap();
    submit_and_run::<DocPlanner>(&kit, changed(&[doc(1, 10)])).await;

    assert_eq!(find_doc_tasks(&kit, 1).len(), 1);
}

#[tokio::test(start_paused = true)]
async fn many_manual_requests_for_one_key_give_one_run() {
    let kit = Flows::default().with_docs(vec![doc(1, 10)]).kit();

    for _ in 0..3 {
        kit.plan_and_submit::<DocPlanner>(manual(vec![doc_key(1)]))
            .await
            .unwrap();
    }
    kit.run_until_idle().await;

    assert_eq!(find_doc_tasks(&kit, 1).len(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_submit_while_the_task_runs_queues_one_more_task() {
    let kit = Flows::default()
        .with_steps(1, [Step::Sleep(Duration::from_secs(5))])
        .kit();
    kit.plan_and_submit::<DocPlanner>(changed(&[doc(1, 10)]))
        .await
        .unwrap();

    let resubmit = async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        kit.plan_and_submit::<DocPlanner>(changed(&[doc(1, 10)]))
            .await
            .unwrap();
    };
    tokio::join!(kit.run_until_idle(), resubmit);

    assert_eq!(find_doc_tasks(&kit, 1).len(), 2);
}

#[tokio::test(start_paused = true)]
async fn two_runs_of_an_exclusive_key_never_overlap() {
    let config = EngineConfig::default().with_lane(
        "doc.index/default",
        LaneConfig::new(2, Duration::from_secs(60)),
    );
    let kit = Flows::default()
        .with_steps(1, [Step::Sleep(Duration::from_secs(5))])
        .kit()
        .config(config);
    kit.plan_and_submit::<DocPlanner>(changed(&[doc(1, 10)]))
        .await
        .unwrap();

    let resubmit = async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        kit.plan_and_submit::<DocPlanner>(changed(&[doc(1, 10)]))
            .await
            .unwrap();
    };
    tokio::join!(kit.run_until_idle(), resubmit);

    let starts: Vec<Duration> = find_doc_tasks(&kit, 1)
        .into_iter()
        .map(|task| task.started_at)
        .collect();
    assert_eq!(starts, [Duration::ZERO, Duration::from_secs(5)]);
}
