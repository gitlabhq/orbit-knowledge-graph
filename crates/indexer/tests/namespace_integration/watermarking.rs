//! Integration tests for watermark-based incremental processing in the namespace handler.

use arrow::array::UInt64Array;
use indexer::testkit::TestEnvelopeFactory;
use serial_test::serial;

use crate::common::{
    TestContext, create_namespace_payload, default_test_watermark, get_namespace_handler,
    get_string_column,
};

#[tokio::test]
#[serial]
async fn namespace_handler_uses_watermark_for_incremental_processing() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO namespace_indexing_watermark (namespace, entity, watermark)
            VALUES (100, 'Group', '2024-01-19 00:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_namespaces (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
            VALUES
            (100, 'org1', 'org1', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-18 12:00:00'),
            (101, 'new-team', 'new-team', 10, 100, NULL, '2024-01-19', '2024-01-20', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_namespace_details (namespace_id, description)
            VALUES (100, 'Old org'), (101, 'New team')",
        )
        .await;

    context
        .execute(
            "INSERT INTO namespace_traversal_paths (id, traversal_path)
            VALUES (100, '1/100/'), (101, '1/100/101/')",
        )
        .await;

    let namespace_handler = get_namespace_handler(&context).await;
    let watermark = default_test_watermark();

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context.query("SELECT count() as cnt FROM gl_group").await;
    let count_array = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("expected UInt64Array");

    assert_eq!(
        count_array.value(0),
        1,
        "should only process new-team, not org1"
    );

    let names = context.query("SELECT name FROM gl_group").await;
    let name_array = get_string_column(&names[0], "name");

    assert_eq!(name_array.value(0), "new-team");
}
