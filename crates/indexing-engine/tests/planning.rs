mod support;

use indexing_engine::{Fired, Lane};
use support::{BIG, Doc, DocIndex, DocPlanner, StatusBoard, changed, doc, kit, script};

#[tokio::test]
async fn routes_a_big_doc_to_the_big_lane() {
    let kit = kit(script([]), vec![], StatusBoard::default());

    let planned = kit
        .plan::<DocPlanner>(changed(&[doc(1, 5_000)]))
        .await
        .unwrap();

    assert_eq!(planned.lane_of::<DocIndex>(), Some(BIG));
}

#[tokio::test]
async fn plans_a_small_doc_in_the_default_lane() {
    let kit = kit(script([]), vec![], StatusBoard::default());

    let planned = kit
        .plan::<DocPlanner>(changed(&[doc(1, 10)]))
        .await
        .unwrap();

    assert_eq!(planned.lane_of::<DocIndex>(), Some(Lane::DEFAULT));
}

#[tokio::test]
async fn a_sweep_ignores_the_requested_lane_and_saves_the_next_page() {
    let docs = vec![doc(1, 5_000), doc(2, 10), doc(3, 10)];
    let kit = kit(script([]), docs, StatusBoard::default());
    let sweep = Fired::Sweep {
        cursor: None,
        tag: "rebuild".to_owned(),
    };

    let planned = kit.plan::<DocPlanner>(sweep).await.unwrap();

    assert_eq!(planned.lane_of::<DocIndex>(), Some(Lane::BACKFILL));
    assert_eq!(planned.next_page(), Some("2"));
    let ids: Vec<u64> = planned
        .inputs::<DocIndex>()
        .unwrap()
        .iter()
        .map(|doc: &Doc| doc.id)
        .collect();
    assert_eq!(ids, [1, 2]);
}

#[tokio::test]
async fn a_planner_that_no_module_registered_fails_to_plan() {
    let kit = indexing_engine::testkit::Testkit::new();

    let planned = kit.plan::<DocPlanner>(Fired::Cron).await;

    assert!(planned.is_err());
}
