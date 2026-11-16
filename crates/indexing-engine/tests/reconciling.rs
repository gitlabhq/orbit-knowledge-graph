mod support;

use support::{DocStatus, StatusBoard, doc, kit, script};

#[tokio::test(start_paused = true)]
async fn a_reconciler_replans_a_key_with_no_job_then_sees_it_done() {
    let board = StatusBoard::default();
    let kit = kit(script([]), vec![doc(1, 10)], board.clone());

    kit.reconcile::<DocStatus>().await.unwrap();
    assert!(board.indexed().is_empty());
    kit.run_until_idle().await;
    kit.reconcile::<DocStatus>().await.unwrap();

    assert_eq!(board.indexed(), [1]);
}
