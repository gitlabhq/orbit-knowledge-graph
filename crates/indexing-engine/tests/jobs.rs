mod support;

use std::time::Duration;

use indexing_engine::prelude::*;
use support::{
    DocIndex, DocPlanner, Flows, ShelfPlanner, ShelfSplit, Step, SummaryBuild, changed, doc,
    doc_key, shelf, shelved, submit_and_run, tenant,
};

#[tokio::test(start_paused = true)]
async fn spawned_tasks_join_the_job_of_their_parent_and_add_up_their_rows() {
    let kit = Flows::default()
        .with_steps(1, [Step::Done { rows: 2 }])
        .with_steps(2, [Step::Done { rows: 3 }])
        .kit();
    let shelf = shelf(7, vec![doc(1, 10), doc(2, 10)]);

    submit_and_run::<ShelfPlanner>(&kit, shelved(&shelf)).await;

    let job = kit
        .last_job::<ShelfSplit>(&tenant(), &ShelfSplit::key(&shelf))
        .unwrap();
    assert!(job.is_done());
    assert_eq!(job.count_tasks(), 3);
    assert_eq!(job.count_rows("pages"), 5);
}

#[tokio::test(start_paused = true)]
async fn a_job_runs_until_its_last_task_ends() {
    let kit = Flows::default()
        .with_steps(2, [Step::Sleep(Duration::from_secs(10))])
        .kit();
    let shelf = shelf(7, vec![doc(1, 10), doc(2, 10)]);
    let key = ShelfSplit::key(&shelf);
    kit.plan_and_submit::<ShelfPlanner>(shelved(&shelf))
        .await
        .unwrap();

    let look_midway = async {
        tokio::time::sleep(Duration::from_secs(5)).await;
        kit.last_job::<ShelfSplit>(&tenant(), &key).unwrap()
    };
    let ((), midway) = tokio::join!(kit.run_until_idle(), look_midway);

    assert!(midway.is_running());
    assert!(
        kit.last_job::<ShelfSplit>(&tenant(), &key)
            .unwrap()
            .is_done()
    );
}

#[tokio::test(start_paused = true)]
async fn a_spawned_task_that_dead_letters_fails_the_job_of_its_parent() {
    let kit = Flows::default().with_steps(2, [Step::Permanent]).kit();
    let shelf = shelf(7, vec![doc(1, 10), doc(2, 10)]);

    submit_and_run::<ShelfPlanner>(&kit, shelved(&shelf)).await;

    let job = kit
        .last_job::<ShelfSplit>(&tenant(), &ShelfSplit::key(&shelf))
        .unwrap();
    assert!(job.is_failed());
}

#[tokio::test(start_paused = true)]
async fn an_outcome_that_spawns_more_than_10_000_children_dead_letters_as_permanent() {
    let kit = Flows::default().kit();
    let docs = (1..=10_001).map(|id| doc(id, 10)).collect();

    submit_and_run::<ShelfPlanner>(&kit, shelved(&shelf(7, docs))).await;

    let split = &kit.find_tasks::<ShelfSplit>()[0];
    assert_eq!(split.error_class, Some("permanent"));
    assert!(kit.find_tasks::<DocIndex>().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_task_with_no_change_writes_no_task_row_and_wakes_no_planner() {
    let kit = Flows::default().with_steps(1, [Step::NoChange]).kit();

    submit_and_run::<DocPlanner>(&kit, changed(&[doc(1, 10)])).await;

    assert!(kit.find_tasks::<DocIndex>().is_empty());
    assert!(kit.find_tasks::<SummaryBuild>().is_empty());
    let job = kit.last_job::<DocIndex>(&tenant(), &doc_key(1)).unwrap();
    assert!(job.is_done());
}

#[tokio::test(start_paused = true)]
async fn a_tenant_finds_only_the_jobs_of_its_own_keys() {
    let kit = Flows::default().kit();

    submit_and_run::<DocPlanner>(&kit, changed(&[doc(1, 10)])).await;

    let other = Tenant::new("tenant-2");
    assert!(kit.last_job::<DocIndex>(&tenant(), &doc_key(1)).is_some());
    assert_eq!(kit.last_job::<DocIndex>(&other, &doc_key(1)), None);
}
