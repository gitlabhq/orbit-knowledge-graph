mod support;

use indexing_engine::{EngineConfig, LaneConfig};
use support::{DocPlanner, Library, MINUTE, SECOND, Step, changed, doc, doc_key, manual};

#[tokio::test(start_paused = true)]
async fn many_submits_for_one_key_before_it_starts_give_one_task() {
    let kit = Library::default().kit();

    kit.fire::<DocPlanner>(changed([doc(1), doc(1)])).await;
    kit.fire::<DocPlanner>(changed([doc(1)])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s doc.index/default doc:1 submitted",
            "0s doc.index/default doc:1 already pending",
            "0s doc.index/default doc:1 already pending",
            "0s doc.index/default doc:1 started attempt 1",
            "0s doc.index/default doc:1 done",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn many_manual_requests_for_one_key_give_one_run() {
    let kit = Library::default().with_docs([doc(1)]).kit();

    for _ in 0..3 {
        kit.fire::<DocPlanner>(manual(vec![doc_key(1)])).await;
    }
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s doc.index/default doc:1 submitted",
            "0s doc.index/default doc:1 already pending",
            "0s doc.index/default doc:1 already pending",
            "0s doc.index/default doc:1 started attempt 1",
            "0s doc.index/default doc:1 done",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_submit_while_the_task_runs_queues_one_more_task() {
    let kit = Library::default()
        .with_steps(1, [Step::Sleep(5 * SECOND)])
        .kit();
    kit.fire::<DocPlanner>(changed([doc(1)])).await;

    let push_again = async {
        tokio::time::sleep(SECOND).await;
        kit.fire::<DocPlanner>(changed([doc(1)])).await;
    };
    tokio::join!(kit.run_until_idle(), push_again);

    assert_eq!(
        kit.timeline(),
        [
            "0s doc.index/default doc:1 submitted",
            "0s doc.index/default doc:1 started attempt 1",
            "1s doc.index/default doc:1 submitted",
            "5s doc.index/default doc:1 done",
            "5s doc.index/default doc:1 started attempt 1",
            "5s doc.index/default doc:1 done",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn two_runs_of_an_exclusive_key_never_overlap_but_other_keys_run_beside_it() {
    let two_slots = LaneConfig::new(2, MINUTE);
    let kit = Library::default()
        .with_steps(1, [Step::Sleep(5 * SECOND)])
        .kit()
        .config(EngineConfig::default().with_lane("doc.index/default", two_slots));
    kit.fire::<DocPlanner>(changed([doc(1)])).await;

    let push_again = async {
        tokio::time::sleep(SECOND).await;
        kit.fire::<DocPlanner>(changed([doc(1), doc(2)])).await;
    };
    tokio::join!(kit.run_until_idle(), push_again);

    assert_eq!(
        kit.timeline(),
        [
            "0s doc.index/default doc:1 submitted",
            "0s doc.index/default doc:1 started attempt 1",
            "1s doc.index/default doc:1 submitted",
            "1s doc.index/default doc:2 submitted",
            "1s doc.index/default doc:2 started attempt 1",
            "1s doc.index/default doc:2 done",
            "5s doc.index/default doc:1 done",
            "5s doc.index/default doc:1 started attempt 1",
            "5s doc.index/default doc:1 done",
        ]
    );
}
