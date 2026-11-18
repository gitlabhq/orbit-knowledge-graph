mod support;

use indexing_engine::prelude::*;
use support::{
    DocIndex, DocPlanner, Library, SECOND, ShelfPlanner, ShelfSplit, Step, changed, doc, doc_key,
    register_shelves, register_summaries, shelf, shelved, tenant,
};

#[tokio::test(start_paused = true)]
async fn spawned_tasks_join_the_job_of_their_parent_and_add_up_their_rows() {
    let kit = Library::default()
        .with_steps(1, [Step::Done { rows: 2 }])
        .with_steps(2, [Step::Done { rows: 3 }])
        .kit()
        .module(register_shelves);
    let shelf = shelf(7, [doc(1), doc(2)]);

    kit.fire::<ShelfPlanner>(shelved(&shelf)).await;
    kit.run_until_idle().await;

    let job = kit
        .last_job::<ShelfSplit>(&tenant(), &ShelfSplit::key(&shelf))
        .unwrap();
    assert!(job.is_done());
    assert_eq!(job.count_tasks(), 3);
    assert_eq!(job.count_rows("pages"), 5);
}

#[tokio::test(start_paused = true)]
async fn a_job_runs_until_its_last_task_ends() {
    let kit = Library::default()
        .with_steps(2, [Step::Sleep(10 * SECOND)])
        .kit()
        .module(register_shelves);
    let shelf = shelf(7, [doc(1), doc(2)]);
    let key = ShelfSplit::key(&shelf);
    kit.fire::<ShelfPlanner>(shelved(&shelf)).await;

    let look_midway = async {
        tokio::time::sleep(5 * SECOND).await;
        kit.last_job::<ShelfSplit>(&tenant(), &key).unwrap()
    };
    let ((), midway) = tokio::join!(kit.run_until_idle(), look_midway);

    assert!(midway.is_running());
    let job = kit.last_job::<ShelfSplit>(&tenant(), &key).unwrap();
    assert!(job.is_done());
}

#[tokio::test(start_paused = true)]
async fn a_spawned_task_that_dead_letters_fails_the_job_of_its_parent() {
    let kit = Library::default()
        .with_steps(2, [Step::Permanent])
        .kit()
        .module(register_shelves);
    let shelf = shelf(7, [doc(1), doc(2)]);

    kit.fire::<ShelfPlanner>(shelved(&shelf)).await;
    kit.run_until_idle().await;

    let job = kit
        .last_job::<ShelfSplit>(&tenant(), &ShelfSplit::key(&shelf))
        .unwrap();
    assert!(job.is_failed());
}

#[tokio::test(start_paused = true)]
async fn an_outcome_that_spawns_more_than_10_000_children_dead_letters_as_permanent() {
    let kit = Library::default().kit().module(register_shelves);
    let docs = (1..=10_001).map(doc);

    kit.fire::<ShelfPlanner>(shelved(&shelf(7, docs))).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s shelf.split/big shelf:7 submitted",
            "0s shelf.split/big shelf:7 started attempt 1",
            "0s shelf.split/big shelf:7 permanent error, dead-letter as permanent: an outcome spawned 10001 children, and the limit is 10000",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_task_with_no_change_writes_no_task_row_and_wakes_no_planner() {
    let kit = Library::default()
        .with_steps(1, [Step::NoChange])
        .kit()
        .module(register_summaries);

    kit.fire::<DocPlanner>(changed([doc(1)])).await;
    kit.run_until_idle().await;

    assert_eq!(
        kit.timeline(),
        [
            "0s doc.index/default doc:1 submitted",
            "0s doc.index/default doc:1 started attempt 1",
            "0s doc.index/default doc:1 no change",
        ]
    );
    assert!(kit.find_tasks::<DocIndex>().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_tenant_finds_only_the_jobs_of_its_own_keys() {
    let kit = Library::default().kit();

    kit.fire::<DocPlanner>(changed([doc(1)])).await;
    kit.run_until_idle().await;

    let other = Tenant::new("tenant-2");
    assert!(kit.last_job::<DocIndex>(&tenant(), &doc_key(1)).is_some());
    assert_eq!(kit.last_job::<DocIndex>(&other, &doc_key(1)), None);
}
