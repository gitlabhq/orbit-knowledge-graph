mod support;

use indexing_engine::prelude::*;
use support::{Doc, DocIndex, DocPlanner, Flows, doc, sweep};

#[tokio::test]
async fn a_sweep_plans_one_page_and_keeps_the_cursor_of_the_next_page() {
    let kit = Flows::default()
        .with_docs(vec![doc(1, 10), doc(2, 10), doc(3, 10)])
        .kit();

    let planned = kit.plan::<DocPlanner>(sweep(None)).await.unwrap();

    let ids: Vec<u64> = planned
        .inputs::<DocIndex>()
        .unwrap()
        .iter()
        .map(|doc: &Doc| doc.id)
        .collect();
    assert_eq!(ids, [1, 2]);
    assert_eq!(planned.next_page(), Some("2"));
}

#[tokio::test(start_paused = true)]
async fn a_campaign_plans_every_page_of_its_planner_in_the_backfill_lane() {
    let kit = Flows::default()
        .with_docs(vec![doc(1, 10), doc(2, 10), doc(3, 5_000)])
        .kit();

    kit.campaign::<DocPlanner>("rebuild").await.unwrap();
    kit.run_until_idle().await;

    let tasks = kit.find_tasks::<DocIndex>();
    assert_eq!(tasks.len(), 3);
    assert!(tasks.iter().all(|task| task.lane == Lane::BACKFILL));
}
