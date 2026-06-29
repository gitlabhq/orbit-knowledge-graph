//! The namespace dispatcher carries a per-entity `targets` list
//! (`crates/indexer/src/orchestrator/scheduled/namespace/`). The entity handler
//! must honor it: a request scoped to one target indexes that entity and leaves
//! the other namespaced entities untouched, while their checkpoints stay unset
//! so a later request still picks them up.

use arrow::array::Int64Array;
use gkg_utils::arrow::ArrowUtils;
use indexer::testkit::TestEnvelopeFactory;
use indexer::types::Envelope;
use integration_testkit::t;

use crate::indexer::common::{
    TestContext, create_namespace, create_project, default_test_watermark, handler_context,
    namespace_handler,
};

fn namespace_envelope_with_targets(
    traversal_path: &str,
    namespace_id: i64,
    targets: &[&str],
) -> Envelope {
    TestEnvelopeFactory::simple(
        &serde_json::json!({
            "namespace": namespace_id,
            "traversal_path": traversal_path,
            "watermark": default_test_watermark().to_rfc3339(),
            "dispatch_id": uuid::Uuid::new_v4(),
            "targets": targets,
        })
        .to_string(),
    )
}

async fn node_ids(ctx: &TestContext, table: &str) -> Vec<i64> {
    let result = ctx
        .query(&format!("SELECT id FROM {} FINAL ORDER BY id", t(table)))
        .await;
    result
        .iter()
        .flat_map(|batch| {
            let ids = ArrowUtils::get_column_by_name::<Int64Array>(batch, "id").expect("id column");
            (0..ids.len()).map(|i| ids.value(i)).collect::<Vec<_>>()
        })
        .collect()
}

pub async fn targeted_request_indexes_only_selected_entity(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;

    namespace_handler(ctx)
        .await
        .handle(
            handler_context(),
            namespace_envelope_with_targets("1/100/", 100, &["Group"]),
        )
        .await
        .expect("handler should succeed");

    assert_eq!(node_ids(ctx, "gl_group").await, vec![100]);
    assert!(
        node_ids(ctx, "gl_project").await.is_empty(),
        "Project was not in targets, so its handler must skip and write nothing"
    );

    namespace_handler(ctx)
        .await
        .handle(
            handler_context(),
            namespace_envelope_with_targets("1/100/", 100, &["Project"]),
        )
        .await
        .expect("handler should succeed");

    assert_eq!(
        node_ids(ctx, "gl_project").await,
        vec![1000],
        "the skipped Project never advanced its checkpoint, so a later Project-targeted request still indexes it"
    );
}
