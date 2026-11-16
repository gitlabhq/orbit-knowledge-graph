mod support;

use std::time::Duration;

use indexing_engine::testkit::Testkit;
use indexing_engine::{EngineConfig, EventBatch, Fired, Key, Lane, LaneConfig, PoolConfig, Worker};
use support::{
    BIG, DocIndex, DocPlanner, Library, Shelf, ShelfPlanner, ShelfSplit, StatusBoard, Step,
    SummaryBuild, changed, doc, kit, register, script, tenant,
};

fn doc_key(id: u64) -> Key {
    DocIndex::key(&doc(id, 0))
}

#[tokio::test(start_paused = true)]
async fn spawned_tasks_join_the_job_of_their_parent() {
    let kit = kit(script([]), vec![], StatusBoard::default());
    let shelf = Shelf {
        tenant: tenant(),
        id: 7,
        docs: vec![doc(1, 10), doc(2, 10), doc(3, 10)],
    };
    let event =
        Fired::Events(EventBatch::encode("shelves.changed", std::slice::from_ref(&shelf)).unwrap());

    kit.trigger::<ShelfPlanner>(event).await.unwrap();
    kit.settle().await;

    let job = kit
        .last_job::<ShelfSplit>(&tenant(), &ShelfSplit::key(&shelf))
        .unwrap();
    assert!(job.is_done());
    assert_eq!(job.count_tasks(), 4);
    assert_eq!(job.count_rows("pages"), 3);
}

#[tokio::test(start_paused = true)]
async fn a_done_task_wakes_the_planner_of_another_worker_in_the_same_lane() {
    let kit = kit(script([]), vec![], StatusBoard::default());

    kit.trigger::<DocPlanner>(changed(&[doc(1, 5_000)]))
        .await
        .unwrap();
    kit.settle().await;

    let summaries = kit.find_tasks::<SummaryBuild>();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].lane, BIG);
}

#[tokio::test(start_paused = true)]
async fn a_task_with_no_change_writes_no_task_row_and_wakes_no_planner() {
    let kit = kit(script([Step::NoChange]), vec![], StatusBoard::default());

    kit.trigger::<DocPlanner>(changed(&[doc(1, 10)]))
        .await
        .unwrap();
    kit.settle().await;

    assert!(kit.find_tasks::<DocIndex>().is_empty());
    assert!(kit.find_tasks::<SummaryBuild>().is_empty());
    assert!(
        kit.last_job::<DocIndex>(&tenant(), &doc_key(1))
            .unwrap()
            .is_done()
    );
}

#[tokio::test(start_paused = true)]
async fn many_submits_for_one_key_give_one_task() {
    let kit = kit(script([]), vec![], StatusBoard::default());

    kit.trigger::<DocPlanner>(changed(&[doc(1, 10), doc(1, 10), doc(1, 10)]))
        .await
        .unwrap();
    kit.settle().await;

    assert_eq!(kit.find_tasks::<DocIndex>().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_failed_task_retries_with_backoff_until_it_succeeds() {
    let kit = kit(
        script([Step::Fail, Step::Fail]),
        vec![],
        StatusBoard::default(),
    );
    let started = tokio::time::Instant::now();

    kit.trigger::<DocPlanner>(changed(&[doc(1, 10)]))
        .await
        .unwrap();
    kit.settle().await;

    assert_eq!(kit.find_tasks::<DocIndex>()[0].attempts, 3);
    assert_eq!(started.elapsed(), Duration::from_secs(11));
}

#[tokio::test(start_paused = true)]
async fn a_task_dead_letters_after_its_last_attempt_and_fails_its_job() {
    let kit = kit(
        script((0..5).map(|_| Step::Fail)),
        vec![],
        StatusBoard::default(),
    );

    kit.trigger::<DocPlanner>(changed(&[doc(1, 10)]))
        .await
        .unwrap();
    kit.settle().await;

    let task = &kit.find_tasks::<DocIndex>()[0];
    assert_eq!(task.attempts, 5);
    assert_eq!(task.error_class, Some("failed"));
    assert!(
        kit.last_job::<DocIndex>(&tenant(), &doc_key(1))
            .unwrap()
            .is_failed()
    );
}

#[tokio::test(start_paused = true)]
async fn a_permanent_error_dead_letters_at_once() {
    let kit = kit(script([Step::Permanent]), vec![], StatusBoard::default());

    kit.trigger::<DocPlanner>(changed(&[doc(1, 10)]))
        .await
        .unwrap();
    kit.settle().await;

    let task = &kit.find_tasks::<DocIndex>()[0];
    assert_eq!(task.attempts, 1);
    assert_eq!(task.error_class, Some("permanent"));
}

#[tokio::test(start_paused = true)]
async fn a_deferred_task_uses_no_attempt() {
    let defers = (0..8).map(|_| Step::Defer(Duration::from_secs(30)));
    let kit = kit(script(defers), vec![], StatusBoard::default());

    kit.trigger::<DocPlanner>(changed(&[doc(1, 10)]))
        .await
        .unwrap();
    kit.settle().await;

    let task = &kit.find_tasks::<DocIndex>()[0];
    assert_eq!(task.attempts, 1);
    assert_eq!(task.error_class, None);
}

#[tokio::test(start_paused = true)]
async fn a_task_older_than_its_max_age_expires() {
    let defers = (0..30).map(|_| Step::Defer(Duration::from_secs(60 * 60)));
    let kit = kit(script(defers), vec![], StatusBoard::default());

    kit.trigger::<DocPlanner>(changed(&[doc(1, 10)]))
        .await
        .unwrap();
    kit.settle().await;

    assert_eq!(kit.find_tasks::<DocIndex>()[0].error_class, Some("expired"));
}

#[tokio::test(start_paused = true)]
async fn a_throttled_task_pauses_its_lane() {
    let kit = kit(
        script([Step::Throttle(Duration::from_secs(60))]),
        vec![],
        StatusBoard::default(),
    );
    let started = tokio::time::Instant::now();

    kit.trigger::<DocPlanner>(changed(&[doc(1, 10), doc(2, 10)]))
        .await
        .unwrap();
    kit.settle().await;

    assert_eq!(kit.find_tasks::<DocIndex>().len(), 2);
    assert!(
        kit.find_tasks::<DocIndex>()
            .iter()
            .all(|task| task.attempts == 1)
    );
    assert_eq!(started.elapsed(), Duration::from_secs(60));
}

#[tokio::test(start_paused = true)]
async fn a_timed_out_task_moves_to_its_on_timeout_lane() {
    let lane = LaneConfig::new(1, Duration::from_secs(1)).on_timeout("doc.index/big");
    let config = EngineConfig::default().with_lane("doc.index/default", lane);
    let kit = kit(
        script([Step::Sleep(Duration::from_secs(5))]),
        vec![],
        StatusBoard::default(),
    )
    .config(config);

    kit.trigger::<DocPlanner>(changed(&[doc(1, 10)]))
        .await
        .unwrap();
    kit.settle().await;

    let task = &kit.find_tasks::<DocIndex>()[0];
    assert_eq!(task.lane, BIG);
    assert_eq!(task.attempts, 2);
}

#[tokio::test(start_paused = true)]
async fn a_pool_that_no_module_registered_is_a_permanent_error() {
    let kit = kit(
        script([Step::Pool("missing")]),
        vec![],
        StatusBoard::default(),
    );

    kit.trigger::<DocPlanner>(changed(&[doc(1, 10)]))
        .await
        .unwrap();
    kit.settle().await;

    assert_eq!(
        kit.find_tasks::<DocIndex>()[0].error_class,
        Some("permanent")
    );
}

#[tokio::test(start_paused = true)]
async fn a_sized_pool_gives_a_permit() {
    let config = EngineConfig::default().with_pool("parse", PoolConfig::Pod(2));
    let kit = Testkit::new()
        .provide(script([Step::Pool("parse")]))
        .provide(Library::default())
        .provide(StatusBoard::default())
        .config(config)
        .module(register);

    kit.trigger::<DocPlanner>(changed(&[doc(1, 10)]))
        .await
        .unwrap();
    kit.settle().await;

    assert_eq!(kit.find_tasks::<DocIndex>()[0].error_class, None);
}

#[tokio::test(start_paused = true)]
async fn a_campaign_plans_every_page_in_the_backfill_lane() {
    let docs = vec![doc(1, 10), doc(2, 10), doc(3, 5_000)];
    let kit = kit(script([]), docs, StatusBoard::default());

    kit.campaign::<DocPlanner>("rebuild").await.unwrap();
    kit.settle().await;

    let tasks = kit.find_tasks::<DocIndex>();
    assert_eq!(tasks.len(), 3);
    assert!(tasks.iter().all(|task| task.lane == Lane::BACKFILL));
}
