mod support;

use support::{Doc, DocIndex, DocPlanner, Library, big_doc, doc, sweep};

#[tokio::test]
async fn a_sweep_plans_one_page_and_keeps_the_cursor_of_the_next_page() {
    let kit = Library::default().with_docs([doc(1), doc(2), doc(3)]).kit();

    let planned = kit.plan::<DocPlanner>(sweep(None)).await;

    let ids: Vec<u64> = planned
        .inputs::<DocIndex>()
        .iter()
        .map(|doc: &Doc| doc.id)
        .collect();
    assert_eq!(ids, [1, 2]);
    assert_eq!(planned.next_page(), Some("2"));
}

#[tokio::test(start_paused = true)]
async fn a_campaign_plans_every_page_of_its_planner_in_the_backfill_lane() {
    let kit = Library::default()
        .with_docs([doc(1), doc(2), big_doc(3)])
        .kit();

    kit.campaign::<DocPlanner>("rebuild").await;

    assert_eq!(
        kit.timeline(),
        [
            "0s doc.index/backfill doc:1 submitted",
            "0s doc.index/backfill doc:2 submitted",
            "0s doc.index/backfill doc:3 submitted",
        ]
    );
}
